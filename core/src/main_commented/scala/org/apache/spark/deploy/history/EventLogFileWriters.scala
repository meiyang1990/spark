// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.commons.io.output.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

/**
 * Spark事件日志文件写入器抽象基类，负责将应用事件日志写入存储系统。
 * 支持压缩、覆盖、缓冲区大小、事件过滤等可配置参数，为单文件和滚动多文件写入提供统一抽象接口。
 * 
 * 可配置参数：
 *   spark.eventLog.compress - 是否压缩事件日志
 *   spark.eventLog.compression.codec - 压缩使用的编解码器
 *   spark.eventLog.overwrite - 是否覆盖已存在的日志文件
 *   spark.eventLog.buffer.kb - 输出流缓冲区大小
 *   spark.eventLog.excludedPatterns - 指定需要排除的事件名称，逗号分隔
 * 
 * 注意：initLogFile返回的CountingOutputStream统计的是压缩前的字节数。
 */
abstract class EventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends Logging {

  // 是否需要压缩日志
  protected val shouldCompress = sparkConf.get(EVENT_LOG_COMPRESS) &&
      !sparkConf.get(EVENT_LOG_COMPRESSION_CODEC).equalsIgnoreCase("none")
  // 需要排除的事件匹配模式
  protected val excludedPatterns = sparkConf.get(EVENT_LOG_EXCLUDED_PATTERNS)
      .map(name => s"""{"Event":"$name"""")
  // 是否允许覆盖已存在文件
  protected val shouldOverwrite = sparkConf.get(EVENT_LOG_OVERWRITE)
  // 输出缓冲区大小，转换为字节
  protected val outputBufferSize = sparkConf.get(EVENT_LOG_OUTPUT_BUFFER_SIZE).toInt * 1024
  // 获取日志存储的Hadoop文件系统实例
  protected val fileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  // 创建压缩编解码器实例
  protected val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf, sparkConf.get(EVENT_LOG_COMPRESSION_CODEC)))
    } else {
      None
    }

  // 压缩编解码器短名称，用于文件名编码
  private[history] val compressionCodecName = compressionCodec.map { c =>
    CompressionCodec.getShortName(c.getClass.getName)
  }

  // Hadoop FSDataOutputStream引用，仅当非本地文件系统时有效
  protected var hadoopDataStream: Option[FSDataOutputStream] = None
  // 打印流写入器引用
  protected var writer: Option[PrintWriter] = None

  /** 检查日志根目录是否为合法目录 */
  protected def requireLogBaseDirAsDirectory(): Unit = {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }
  }

  /**
   * 初始化日志文件，创建输出流并调用回调设置写入器
   * @param path 日志文件路径
   * @param fnSetupWriter 回调函数，基于输出流创建PrintWriter
   */
  protected def initLogFile(path: Path)(fnSetupWriter: OutputStream => PrintWriter): Unit = {
    if (shouldOverwrite && fileSystem.delete(path, true)) {
      logWarning(log"Event log ${MDC(LogKeys.PATH, path)} already exists. Overwriting...")
    }

    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"
    val uri = path.toUri

    // Hadoop LocalFileSystem存在同步问题(HADOOP-7844)，本地文件直接使用JDK的FileOutputStream
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        // 非本地文件使用Hadoop API创建输出流
        hadoopDataStream = Some(
          SparkHadoopUtil.createFile(fileSystem, path, sparkConf.get(EVENT_LOG_ALLOW_EC)))
        hadoopDataStream.get
      }

    try {
      // 包装压缩流
      val cstream = compressionCodec.map(_.compressedContinuousOutputStream(dstream))
        .getOrElse(dstream)
      // 包装缓冲流
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)
      // 设置日志文件权限
      fileSystem.setPermission(path, EventLogFileWriter.LOG_FILE_PERMISSIONS)
      logInfo(log"Logging events to ${MDC(PATH, path)}")
      // 调用回调创建写入器
      writer = Some(fnSetupWriter(bstream))
    } catch {
      case e: Exception =>
        // 创建失败，关闭流并抛出异常
        dstream.close()
        throw e
    }
  }

  /**
   * 写入一行日志，过滤排除事件，支持强制刷新
   * @param line 日志行内容
   * @param flushLogger 是否强制刷新缓冲区
   */
  protected def writeLine(line: String, flushLogger: Boolean = false): Unit = {
    // 如果事件匹配排除规则，则跳过写入
    if (excludedPatterns.exists(line.startsWith(_))) return
    // scalastyle:off println
    writer.foreach(_.println(line))
    // scalastyle:on println
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(_.hflush())
    }
  }

  /** 安全关闭写入器和输出流，检查IO错误并记录日志 */
  protected def closeWriter(): Unit = {
    // 1. 先刷新缓冲区，检查错误
    writer.foreach(_.flush())
    if (writer.exists(_.checkError())) {
      logError("Spark detects errors while flushing event logs.")
    }
    hadoopDataStream.foreach(_.hflush())

    // 2. 尝试关闭并检查错误
    writer.foreach(_.close())
    if (writer.exists(_.checkError())) {
      logError("Spark detects errors while closing event logs.")
      // 3. 确保底层流至少被关闭（尽力而为）
      hadoopDataStream.foreach(_.close())
    }
  }

  /**
   * 重命名文件，处理已存在目标文件，更新修改时间适配对象存储
   * @param src 源文件路径
   * @param dest 目标文件路径
   * @param overwrite 是否覆盖已存在目标
   */
  protected def renameFile(src: Path, dest: Path, overwrite: Boolean): Unit = {
    if (fileSystem.exists(dest)) {
      if (overwrite) {
        logWarning(log"Event log ${MDC(EVENT_LOG_DESTINATION, dest)} already exists. " +
          log"Overwriting...")
        if (!fileSystem.delete(dest, true)) {
          logWarning(log"Error deleting ${MDC(EVENT_LOG_DESTINATION, dest)}")
        }
      } else {
        throw new IOException(s"Target log file already exists ($dest)")
      }
    }
    // 执行重命名
    fileSystem.rename(src, dest)
    // 更新修改时间，适配那些rename不更新修改时间但支持setTimes的对象存储，大多数文件系统不影响
    try {
      fileSystem.setTimes(dest, System.currentTimeMillis(), -1)
    } catch {
      case e: Exception => logDebug(s"failed to set time of $dest", e)
    }
  }

  /** 初始化事件日志写入器，开始日志记录 */
  def start(): Unit

  /** 将JSON格式的事件写入日志文件 */
  def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit

  /** 停止日志写入，表示应用运行完成 */
  def stop(): Unit

  /** 返回日志文件路径，仅用于测试 */
  def logPath: String
}

/**
 * EventLogFileWriter伴生对象，提供工厂方法和常量定义
 */
object EventLogFileWriter {
  // 写入中文件后缀，标识应用还在运行
  val IN_PROGRESS = ".inprogress"
  // 已压缩文件后缀
  val COMPACTED = ".compact"

  // 日志文件权限 660
  val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("660", 8).toShort)
  // 日志目录权限 770
  val LOG_FOLDER_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  /**
   * EventLogFileWriter工厂方法，根据配置选择单文件或滚动写入实现
   * @param appId 应用ID
   * @param appAttemptId 应用尝试ID
   * @param logBaseDir 日志根目录
   * @param sparkConf Spark配置
   * @param hadoopConf Hadoop配置
   * @return 具体写入器实例
   */
  def apply(
      appId: String,
      appAttemptId: Option[String],
      logBaseDir: URI,
      sparkConf: SparkConf,
      hadoopConf: Configuration): EventLogFileWriter = {
    if (sparkConf.get(EVENT_LOG_ENABLE_ROLLING)) {
      new RollingEventLogFilesWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    } else {
      new SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)
    }
  }

  /** 根据应用ID和尝试ID生成日志基础名称 */
  def nameForAppAndAttempt(appId: String, appAttemptId: Option[String]): String = {
    Utils.nameForAppAndAttempt(appId, appAttemptId)
  }

  /** 从日志文件名解析压缩编解码器名称 */
  def codecName(log: Path): Option[String] = {
    // 压缩编解码器编码为扩展名，例如app_123.lzf
    // 因为应用ID中不包含点，所以可以安全分割
    val logName = log.getName.stripSuffix(COMPACTED).stripSuffix(IN_PROGRESS)
    logName.split("\\.").tail.lastOption
  }

  /** 判断日志文件是否已压缩 */
  def isCompacted(log: Path): Boolean = log.getName.endsWith(COMPACTED)
}

/**
 * 单文件事件日志写入器实现，将所有事件写入单个日志文件
 */
class SingleEventLogFileWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  override val logPath: String = SingleEventLogFileWriter.getLogPath(logBaseDir, appId,
    appAttemptId, compressionCodecName)

  // 写入中状态的文件路径
  protected def inProgressPath = logPath + EventLogFileWriter.IN_PROGRESS

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    initLogFile(new Path(inProgressPath)) { os =>
      new PrintWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))
    }
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    writeLine(eventJson, flushLogger)
  }

  /**
   * 停止日志记录，将日志文件从.inprogress后缀重命名为正式名称，表示应用已完成
   */
  override def stop(): Unit = {
    logInfo(log"Stopping event writer for ${MDC(PATH, logPath)}")
    closeWriter()
    renameFile(new Path(inProgressPath), new Path(logPath), shouldOverwrite)
  }
}

/**
 * SingleEventLogFileWriter伴生对象，提供路径计算工具方法
 */
object SingleEventLogFileWriter {
  /**
   * 计算给定应用的日志文件路径，将压缩编码编码到文件名中
   * @param logBaseDir 日志根目录
   * @param appId 应用ID
   * @param appAttemptId 应用尝试ID
   * @param compressionCodecName 压缩编解码器名称，None表示不压缩
   * @return 完整日志文件路径
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val codec = compressionCodecName.map("." + _).getOrElse("")
    new Path(logBaseDir).toString.stripSuffix("/") + "/" +
      EventLogFileWriter.nameForAppAndAttempt(appId, appAttemptId) + codec
  }
}

/**
 * 滚动事件日志写入器实现，按文件大小滚动切分多个日志文件。
 * 每个应用创建一个独立目录，存放多个事件日志分片文件和应用状态文件，每个文件大小不超过配置阈值。
 * 
 * 目录和文件命名规则：
 * - 应用目录名: eventlog_v2_appId(_[appAttemptId])
 * - 事件日志分片前缀: events_[index]_[appId](_[appAttemptId])(.[codec])
 *   - index 单调递增序列
 * - 应用状态文件名: appstatus_[appId](_[appAttemptId])(.inprogress)
 * 
 * 滚动逻辑基于压缩前的字节数判断，不跟踪压缩后的实际文件大小。
 * 应用状态文件使用空文件标识，最小化存储开销。
 */
class RollingEventLogFilesWriter(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  import RollingEventLogFilesWriter._

  // 单个事件日志文件最大字节数
  private val eventFileMaxLength = sparkConf.get(EVENT_LOG_ROLLING_MAX_FILE_SIZE)

  // 当前应用的日志目录路径
  private val logDirForAppPath = getAppEventLogDirPath(logBaseDir, appId, appAttemptId)

  // 统计压缩前字节数的输出流引用
  private var countingOutputStream: Option[CountingOutputStream] = None

  // 当前分片索引和路径，start方法中会初始化
  private var index: Long = 0L
  private var currentEventLogFilePath: Path = _

  override def start(): Unit = {
    requireLogBaseDirAsDirectory()

    // 如果允许覆盖，先删除已存在目录
    if (fileSystem.exists(logDirForAppPath) && shouldOverwrite) {
      fileSystem.delete(logDirForAppPath, true)
    }

    // 如果目录存在且不允许覆盖，抛出异常
    if (fileSystem.exists(logDirForAppPath)) {
      throw new IOException(s"Target log directory already exists ($logDirForAppPath)")
    }

    // 创建应用日志目录，指定权限避免umask问题
    FileSystem.mkdirs(fileSystem, logDirForAppPath, EventLogFileWriter.LOG_FOLDER_PERMISSIONS)
    // 创建写入中状态的应用状态文件
    createAppStatusFile(inProgress = true)
    // 滚动创建第一个分片
    rollEventLogFile()
  }

  override def writeEvent(eventJson: String, flushLogger: Boolean = false): Unit = {
    writer.foreach { w =>
      val currentLen = countingOutputStream.get.getByteCount
      // 写入后超过最大大小，先滚动创建新分片
      if (currentLen + eventJson.length > eventFileMaxLength) {
        rollEventLogFile()
      }
    }

    writeLine(eventJson, flushLogger)
  }

  /** 滚动创建新的事件日志分片，仅对测试暴露 */
  private[history] def rollEventLogFile(): Unit = {
    // 关闭旧分片
    closeWriter()

    // 索引递增，计算新分片路径
    index += 1
    currentEventLogFilePath = getEventLogFilePath(logDirForAppPath, appId, appAttemptId, index,
      compressionCodecName)

    // 初始化新分片
    initLogFile(currentEventLogFilePath) { os =>
      countingOutputStream = Some(new CountingOutputStream(os))
      new PrintWriter(
        new
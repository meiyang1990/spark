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

package org.apache.spark.util.logging

import java.io._
import java.util.EnumSet
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream
import org.apache.logging.log4j._
import org.apache.logging.log4j.core.Logger
import org.apache.logging.log4j.core.appender.{FileAppender => Log4jFileAppender}
import org.apache.logging.log4j.core.layout.PatternLayout

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Driver日志持久化管理器，负责将Driver端日志写入本地文件并同步到HDFS，实现Driver日志的持久化保存
 * 用于支持Driver日志查看和故障排查，在YARN/K8s集群模式下保存Driver运行日志
 * @param conf Spark配置对象，用于获取日志路径、同步配置等参数
 */
private[spark] class DriverLogger(conf: SparkConf) extends Logging {

  // 分块上传大小，1MB
  private val UPLOAD_CHUNK_SIZE = 1024 * 1024
  // HDFS同步间隔，单位秒
  private val UPLOAD_INTERVAL_IN_SECS = 5
  // 默认日志输出格式
  private val DEFAULT_LAYOUT = "%d{yy/MM/dd HH:mm:ss.SSS} %t %p %c{1}: %m%n%ex"
  // 日志文件HDFS权限配置 770
  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // 本地Driver日志文件绝对路径，根据配置生成
  private val localLogFile: String = conf.get(DRIVER_LOG_LOCAL_DIR).map {
    Utils.getFile(_, DriverLogger.DRIVER_LOG_FILE).getAbsolutePath()
  }.getOrElse(Utils.getFile(
    Utils.getLocalDir(conf),
    DriverLogger.DRIVER_LOG_DIR,
    DriverLogger.DRIVER_LOG_FILE).getAbsolutePath())
  // HDFS异步写入器，为空表示未开启同步
  private var writer: Option[DfsAsyncWriter] = None

  // 初始化时添加Log4j本地日志appender
  addLogAppender()

  /**
   * 向Log4j根日志添加本地文件appender，捕获Driver输出的所有日志到本地文件
   */
  private def addLogAppender(): Unit = {
    val logger = LogManager.getRootLogger().asInstanceOf[Logger]
    // 使用用户自定义日志格式或默认格式构建日志布局
    val layout = if (conf.contains(DRIVER_LOG_LAYOUT)) {
      PatternLayout.newBuilder().withPattern(conf.get(DRIVER_LOG_LAYOUT).get).build()
    } else {
      PatternLayout.newBuilder().withPattern(DEFAULT_LAYOUT).build()
    }
    val config = logger.getContext.getConfiguration()
    def log4jFileAppender() = {
      // SPARK-37853: We can't use the chained API invocation mode because
      // `AbstractFilterable.Builder.asBuilder()` method will return `Any` in Scala.
      val builder: Log4jFileAppender.Builder[_] = Log4jFileAppender.newBuilder()
      builder.withAppend(false)
      builder.setBufferedIo(false)
      builder.setConfiguration(config)
      builder.withFileName(localLogFile)
      builder.setIgnoreExceptions(false)
      builder.setLayout(layout)
      builder.setName(DriverLogger.APPENDER_NAME)
      builder.build()
    }
    val fa = log4jFileAppender()
    logger.addAppender(fa)
    fa.start()
    logInfo(log"Added a local log appender at: ${MDC(FILE_NAME, localLogFile)}")
  }

  /**
   * 启动异步同步任务，将本地Driver日志定期同步到HDFS
   * @param hadoopConf Hadoop配置，用于连接HDFS
   */
  def startSync(hadoopConf: Configuration): Unit = {
    try {
      // Setup a writer which moves the local file to hdfs continuously
      val appId = Utils.sanitizeDirName(conf.getAppId)
      writer = Some(new DfsAsyncWriter(appId, hadoopConf))
    } catch {
      case e: Exception =>
        logError(s"Could not persist driver logs to dfs", e)
    }
  }

  /**
   * 停止Driver日志记录，清理资源并完成HDFS同步
   */
  def stop(): Unit = {
    try {
      val logger = LogManager.getRootLogger().asInstanceOf[Logger]
      val fa = logger.getAppenders.get(DriverLogger.APPENDER_NAME)
      logger.removeAppender(fa)
      Utils.tryLogNonFatalError(fa.stop())
      writer.foreach(_.closeWriter())
    } catch {
      case e: Exception =>
        logError(s"Error in persisting driver logs", e)
    } finally {
      Utils.tryLogNonFatalError {
        JavaUtils.deleteRecursively(Utils.getFile(localLogFile).getParentFile())
      }
    }
  }

  /**
   * 异步将本地Driver日志增量同步到HDFS的写入器，通过定时任务定期同步新产生的日志
   * @param appId Spark应用ID，用于生成HDFS日志文件名
   * @param hadoopConf Hadoop配置，用于连接HDFS
   */
  // Visible for testing
  private[spark] class DfsAsyncWriter(appId: String, hadoopConf: Configuration) extends Runnable
      with Logging {

    private var streamClosed = false
    private var inStream: InputStream = null
    private var outputStream: FSDataOutputStream = null
    private val tmpBuffer = new Array[Byte](UPLOAD_CHUNK_SIZE)
    private var threadpool: ScheduledExecutorService = _
    // 初始化时完成HDFS连接和流创建
    init()

    /**
     * 初始化HDFS文件系统、输入输出流和定时同步任务
     */
    private def init(): Unit = {
      val rootDir = conf.get(DRIVER_LOG_DFS_DIR).get
      val fileSystem: FileSystem = new Path(rootDir).getFileSystem(hadoopConf)
      // 检查HDFS根目录是否存在，不存在则抛出异常
      if (!fileSystem.exists(new Path(rootDir))) {
        throw new RuntimeException(s"${rootDir} does not exist." +
          s" Please create this dir in order to persist driver logs")
      }
      // 生成带应用ID的HDFS日志文件路径
      val dfsLogFile: Path = fileSystem.makeQualified(new Path(rootDir, appId
        + DriverLogger.DRIVER_LOG_FILE_SUFFIX))
      try {
        // 打开本地日志输入流和HDFS输出流
        inStream = new BufferedInputStream(new FileInputStream(localLogFile))
        outputStream = SparkHadoopUtil.createFile(fileSystem, dfsLogFile,
          conf.get(DRIVER_LOG_ALLOW_EC))
        // 设置HDFS文件权限
        fileSystem.setPermission(dfsLogFile, LOG_FILE_PERMISSIONS)
      } catch {
        case e: Exception =>
          JavaUtils.closeQuietly(inStream)
          JavaUtils.closeQuietly(outputStream)
          throw e
      }
      // 创建单线程定时调度器，启动定期同步任务
      threadpool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("dfsSyncThread")
      threadpool.scheduleWithFixedDelay(this, UPLOAD_INTERVAL_IN_SECS, UPLOAD_INTERVAL_IN_SECS,
        TimeUnit.SECONDS)
      logInfo(log"Started driver log file sync to: ${MDC(PATH, dfsLogFile)}")
    }

    /**
     * 定时执行的同步任务，增量读取本地日志新增内容并写入HDFS
     */
    def run(): Unit = {
      if (streamClosed) {
        return
      }
      try {
        // 获取当前本地日志文件剩余可读字节数
        var remaining = inStream.available()
        val hadData = remaining > 0
        // 分块读取本地日志并写入HDFS
        while (remaining > 0) {
          val read = inStream.read(tmpBuffer, 0, math.min(remaining, UPLOAD_CHUNK_SIZE))
          outputStream.write(tmpBuffer, 0, read)
          remaining -= read
        }
        // 如果有新数据写入，执行刷新持久化到磁盘
        if (hadData) {
          outputStream match {
            // HDFS输出流执行hsync确保数据持久化
            case hdfsStream: HdfsDataOutputStream =>
              hdfsStream.hsync(EnumSet.allOf(classOf[HdfsDataOutputStream.SyncFlag]))
            // 其他文件系统执行hflush
            case other =>
              other.hflush()
          }
        }
      } catch {
        case e: Exception => logError("Failed writing driver logs to dfs", e)
      }
    }

    /**
     * 关闭流，先同步剩余所有日志再关闭输入输出流
     */
    private def close(): Unit = {
      if (streamClosed) {
        return
      }
      try {
        // Write all remaining bytes
        run()
      } finally {
        try {
          streamClosed = true
          inStream.close()
          outputStream.close()
        } catch {
          case e: Exception =>
            logError("Error in closing driver log input/output stream", e)
        }
      }
    }

    /**
     * 安全关闭写入器和定时线程池，等待剩余任务完成
     */
    def closeWriter(): Unit = {
      try {
        threadpool.execute(() => DfsAsyncWriter.this.close())
        threadpool.shutdown()
        threadpool.awaitTermination(1, TimeUnit.MINUTES)
      } catch {
        case e: Exception =>
          logError("Error in shutting down threadpool", e)
      }
    }
  }

}

/**
 * DriverLogger工厂对象，根据配置决定是否创建DriverLogger实例
 */
private[spark] object DriverLogger extends Logging {
  val DRIVER_LOG_DIR = "__driver_logs__"
  val DRIVER_LOG_FILE = "driver.log"
  val DRIVER_LOG_FILE_SUFFIX = "_" + DRIVER_LOG_FILE
  val APPENDER_NAME = "_DriverLogAppender"

  /**
   * 根据Spark配置创建DriverLogger实例，仅在满足条件时创建
   * @param conf Spark配置对象
   * @return 配置符合条件返回Some(DriverLogger)，否则返回None
   */
  def apply(conf: SparkConf): Option[DriverLogger] = {
    val localDriverLogEnabled = conf.get(DRIVER_LOG_LOCAL_DIR).nonEmpty
    if (conf.get(DRIVER_LOG_PERSISTTODFS) && Utils.isClientMode(conf)
      || localDriverLogEnabled) {
      if (conf.contains(DRIVER_LOG_DFS_DIR)) {
        try {
          Some(new DriverLogger(conf))
        } catch {
          case e: Exception =>
            logError("Could not add driver logger", e)
            None
        }
      } else if (localDriverLogEnabled) {
        // Driver Logger is started only for Spark Driver Log UI Tab
        new DriverLogger(conf)
        // Return None because we don't need DFS-related logic in SparkContext and DfsAsyncWriter
        None
      } else {
        logWarning(log"Driver logs are not persisted because" +
          log" ${MDC(CONFIG, DRIVER_LOG_DFS_DIR.key)} is not configured")
        None
      }
    } else {
      None
    }
  }
}
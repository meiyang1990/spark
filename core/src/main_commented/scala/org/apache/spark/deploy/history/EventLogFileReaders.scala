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

import java.io.{BufferedInputStream, InputStream}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hdfs.DFSInputStream

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventLogFileWriter.codecName
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * 文件级注释：事件日志文件读取工具抽象基类，为历史服务器提供应用事件日志的元信息读取能力
 * 支持单个日志文件和滚动分割日志文件两种场景，提供统一的读取接口
 */
/** The base class of reader which will read the information of event log file(s). */
abstract class EventLogFileReader(
    protected val fileSystem: FileSystem,
    val rootPath: Path) {

  /**
   * 获取HDFS文件当前实际长度（处理正在写入的不完整文件）
   * @param path 目标文件路径
   * @return DFSInputStream时返回实际长度，否则返回None
   */
  protected def fileSizeForDFS(path: Path): Option[Long] = {
    Utils.tryWithResource(fileSystem.open(path)) { in =>
      in.getWrappedStream match {
        case dfsIn: DFSInputStream => Some(dfsIn.getFileLength)
        case _ => None
      }
    }
  }

  /**
   * 将指定HDFS文件添加为ZIP压缩包中的一个条目
   * @param zipStream 输出ZIP流
   * @param path 源文件HDFS路径
   * @param entryName ZIP条目标名称
   */
  protected def addFileAsZipEntry(
      zipStream: ZipOutputStream,
      path: Path,
      entryName: String): Unit = {
    Utils.tryWithResource(fileSystem.open(path, 1 * 1024 * 1024)) { inputStream =>
      zipStream.putNextEntry(new ZipEntry(entryName))
      inputStream.transferTo(zipStream)
      zipStream.closeEntry()
    }
  }

  /** Returns the last index of event log files. None for single event log file. */
  def lastIndex: Option[Long]

  /**
   * Returns the size of file for the last index of event log files. Returns its size for
   * single event log file.
   */
  def fileSizeForLastIndex: Long

  /** Returns whether the application is completed. */
  def completed: Boolean

  /**
   * Returns the size of file for the last index (itself for single event log file) of event log
   * files, only when underlying input stream is DFSInputStream. Otherwise returns None.
   */
  def fileSizeForLastIndexForDFS: Option[Long]

  /**
   * Returns the modification time for the last index (itself for single event log file)
   * of event log files.
   */
  def modificationTime: Long

  /**
   * This method compresses the files passed in, and writes the compressed data out into the
   * ZipOutputStream passed in. Each file is written as a new ZipEntry with its name being
   * the name of the file being compressed.
   */
  def zipEventLogFiles(zipStream: ZipOutputStream): Unit

  /** Returns all available event log files. */
  def listEventLogFiles: Seq[FileStatus]

  /** Returns the short compression name if being used. None if it's uncompressed. */
  def compressionCodec: Option[String]

  /** Returns the size of all event log files. */
  def totalSize: Long
}

/**
 * 事件日志读取器工厂对象，负责根据日志存储结构创建对应类型的读取器
 * 提供事件日志输入流打开、压缩编码缓存等公共能力
 */
object EventLogFileReader extends Logging {
  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new ConcurrentHashMap[String, CompressionCodec]()

  /**
   * 根据已知的最后索引创建事件日志读取器
   * @param fs Hadoop文件系统实例
   * @param path 日志根路径
   * @param lastIndex 最后一个日志文件索引，None表示单文件
   * @return 对应类型的事件日志读取器
   */
  def apply(
      fs: FileSystem,
      path: Path,
      lastIndex: Option[Long]): EventLogFileReader = {
    lastIndex match {
      case Some(_) => new RollingEventLogFilesFileReader(fs, path)
      case None => new SingleFileEventLogFileReader(fs, path)
    }
  }

  /**
   * 根据路径自动探测日志类型并创建对应读取器
   * @param fs Hadoop文件系统实例
   * @param path 日志根路径
   * @return 探测成功返回读取器，否则返回None
   */
  def apply(fs: FileSystem, path: Path): Option[EventLogFileReader] = {
    apply(fs, fs.getFileStatus(path))
  }

  /**
   * 根据文件状态自动探测日志类型并创建对应读取器
   * @param fs Hadoop文件系统实例
   * @param status 文件/目录状态
   * @return 探测成功返回读取器，否则返回None
   */
  def apply(fs: FileSystem, status: FileStatus): Option[EventLogFileReader] = {
    if (isSingleEventLog(status)) {
      Some(new SingleFileEventLogFileReader(fs, status.getPath, Option(status)))
    } else if (isRollingEventLogs(status)) {
      val files = fs.listStatus(status.getPath)
      if (files.exists(RollingEventLogFilesWriter.isEventLogFile) &&
          files.exists(RollingEventLogFilesWriter.isAppStatusFile)) {
        Some(new RollingEventLogFilesFileReader(fs, status.getPath))
      } else {
        logDebug(s"Rolling event log directory have no event log file at ${status.getPath}")
        None
      }
    } else {
      None
    }
  }

  /**
   * 打开事件日志文件，返回解压后的输入流
   * @param log 日志文件路径
   * @param fs Hadoop文件系统实例
   * @return 每行一个JSON记录的输入流
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    val in = new BufferedInputStream(fs.open(log))
    try {
      // 从文件名解析压缩编码名称
      val codec = codecName(log).map { c =>
        // 从缓存获取或创建新的压缩编码器
        codecMap.computeIfAbsent(c, CompressionCodec.createCodec(new SparkConf, _))
      }
      // 返回对应压缩格式的输入流，无压缩则返回原始流
      codec.map(_.compressedContinuousInputStream(in)).getOrElse(in)
    } catch {
      case e: Throwable =>
        in.close()
        throw e
    }
  }

  /** 判断是否为单个事件日志文件 */
  private def isSingleEventLog(status: FileStatus): Boolean = {
    !status.isDirectory &&
      // FsHistoryProvider used to generate a hidden file which can't be read.  Accidentally
      // reading a garbage file is safe, but we would log an error which can be scary to
      // the end-user.
      !status.getPath.getName.startsWith(".")
  }

  /** 判断是否为滚动分割事件日志目录 */
  private def isRollingEventLogs(status: FileStatus): Boolean = {
    RollingEventLogFilesWriter.isEventLogDir(status)
  }
}

/**
 * 单个事件日志文件读取器，用于读取未分割的单个应用日志文件
 * 延迟加载文件状态，不提供实时动态更新，适用于单文件日志场景
 */
/**
 * The reader which will read the information of single event log file.
 *
 * This reader gets the status of event log file only once when required;
 * It may not give "live" status of file that could be changing concurrently, and
 * FileNotFoundException could occur if the log file is renamed before getting the
 * status of log file.
 */
private[history] class SingleFileEventLogFileReader(
    fs: FileSystem,
    path: Path,
    maybeStatus: Option[FileStatus]) extends EventLogFileReader(fs, path) {
  // 延迟加载文件状态
  private lazy val status = maybeStatus.getOrElse(fileSystem.getFileStatus(rootPath))

  def this(fs: FileSystem, path: Path) = this(fs, path, None)

  override def lastIndex: Option[Long] = None

  override def fileSizeForLastIndex: Long = status.getLen

  /** 判断应用是否已经完成写入 */
  override def completed: Boolean = !rootPath.getName.stripSuffix(EventLogFileWriter.COMPACTED)
    .endsWith(EventLogFileWriter.IN_PROGRESS)

  override def fileSizeForLastIndexForDFS: Option[Long] = {
    if (completed) {
      Some(fileSizeForLastIndex)
    } else {
      fileSizeForDFS(rootPath)
    }
  }

  override def modificationTime: Long = status.getModificationTime

  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {
    addFileAsZipEntry(zipStream, rootPath, rootPath.getName)
  }

  override def listEventLogFiles: Seq[FileStatus] = Seq(status)

  override def compressionCodec: Option[String] = EventLogFileWriter.codecName(rootPath)

  override def totalSize: Long = fileSizeForLastIndex
}

/**
 * 滚动分割事件日志读取器，用于读取按大小分割的多文件滚动事件日志
 * 一次读取目录列表后缓存，不动态更新，需要刷新列表需重新创建实例
 */
/**
 * The reader which will read the information of rolled multiple event log files.
 *
 * This reader lists the files only once; if caller would like to play with updated list,
 * it needs to create another reader instance.
 */
private[history] class RollingEventLogFilesFileReader(
    fs: FileSystem,
    path: Path) extends EventLogFileReader(fs, path) {
  import RollingEventLogFilesWriter._

  // 延迟加载目录下所有文件
  private lazy val files: Seq[FileStatus] = {
    val ret = fs.listStatus(rootPath).toImmutableArraySeq
    require(ret.exists(isEventLogFile), "Log directory must contain at least one event log file!")
    require(ret.exists(isAppStatusFile), "Log directory must contain an appstatus file!")
    ret
  }

  // 获取应用状态文件
  private lazy val appStatusFile = files.find(isAppStatusFile).get

  // 延迟加载并排序事件日志文件列表
  private lazy val eventLogFiles: Seq[FileStatus] = {
    // 按索引排序日志文件
    val eventLogFiles = files.filter(isEventLogFile).sortBy { status =>
      val filePath = status.getPath
      var idx = getEventLogFileIndex(filePath.getName).toDouble
      // trick to place compacted file later than normal file if index is same.
      if (EventLogFileWriter.isCompacted(filePath)) {
        idx += 0.1
      }
      idx
    }
    // 丢弃最后一个压缩文件之前的旧文件，只保留增量部分
    val filesToRead = dropBeforeLastCompactFile(eventLogFiles)
    // 检查文件索引是否连续，不连续则报错
    val indices = filesToRead.map { file => getEventLogFileIndex(file.getPath.getName) }
    require((indices.head to indices.last) == indices, "Found missing event log file, expected" +
      s" indices: ${indices.head to indices.last}, actual: ${indices}")
    filesToRead
  }

  override def lastIndex: Option[Long] = Some(
    getEventLogFileIndex(lastEventLogFile.getPath.getName))

  override def fileSizeForLastIndex: Long = lastEventLogFile.getLen

  /** 根据应用状态文件后缀判断应用是否完成写入 */
  override def completed: Boolean = {
    !appStatusFile.getPath.getName.endsWith(EventLogFileWriter.IN_PROGRESS)
  }

  override def fileSizeForLastIndexForDFS: Option[Long] = {
    if (completed) {
      Some(fileSizeForLastIndex)
    } else {
      fileSizeForDFS(lastEventLogFile.getPath)
    }
  }

  override def modificationTime: Long = lastEventLogFile.getModificationTime

  override def zipEventLogFiles(zipStream: ZipOutputStream): Unit = {
    val dirEntryName = rootPath.getName + "/"
    zipStream.putNextEntry(new ZipEntry(dirEntryName))
    files.foreach { file =>
      addFileAsZipEntry(zipStream, file.getPath, dirEntryName + file.getPath.getName)
    }
  }

  override def listEventLogFiles: Seq[FileStatus] = eventLogFiles

  override def compressionCodec: Option[String] = {
    EventLogFileWriter.codecName(eventLogFiles.head.getPath)
  }

  override def totalSize: Long = eventLogFiles.map(_.getLen).sum

  /** 获取最后一个事件日志文件 */
  private def lastEventLogFile: FileStatus = eventLogFiles.last

  /**
   * 丢弃最后一个压缩文件之前的所有文件，只保留该压缩文件及之后的增量文件
   * @param eventLogFiles 排序后的事件日志列表
   * @return 裁剪后需要读取的文件列表
   */
  private def dropBeforeLastCompactFile(eventLogFiles: Seq[FileStatus]): Seq[FileStatus] = {
    val lastCompactedFileIdx = eventLogFiles.lastIndexWhere { fs =>
      EventLogFileWriter.isCompacted(fs.getPath)
    }
    eventLogFiles.drop(lastCompactedFileIdx)
  }
}
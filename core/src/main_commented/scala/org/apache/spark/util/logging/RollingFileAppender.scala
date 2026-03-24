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
import java.util.zip.GZIPOutputStream

import org.apache.spark.SparkConf
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * 滚动日志文件追加器，持续从输入流读取数据写入日志文件，按指定策略滚动日志，
 * 支持日志压缩和保留文件数量限制，用于Spark executor日志管理。
 *
 * @param inputStream             要读取数据的输入流
 * @param activeFile              当前活跃日志文件（正在写入的文件）
 * @param rollingPolicy           日志滚动策略
 * @param conf                    Spark配置，用于读取滚动相关配置项
 * @param bufferSize              可选缓冲区大小，主要用于测试
 * @param closeStreams            选项标志，结束时是否关闭输入流
 */
private[spark] class RollingFileAppender(
    inputStream: InputStream,
    activeFile: File,
    val rollingPolicy: RollingPolicy,
    conf: SparkConf,
    bufferSize: Int = RollingFileAppender.DEFAULT_BUFFER_SIZE,
    closeStreams: Boolean = false
  ) extends FileAppender(inputStream, activeFile, bufferSize, closeStreams) {

  // 从配置读取最大保留日志文件数量
  private val maxRetainedFiles = conf.get(config.EXECUTOR_LOGS_ROLLING_MAX_RETAINED_FILES)
  // 从配置读取是否开启滚动日志压缩
  private val enableCompression = conf.get(config.EXECUTOR_LOGS_ROLLING_ENABLE_COMPRESSION)

  /** 停止日志追加器 */
  override def stop(): Unit = {
    super.stop()
  }

  /** 按需触发滚动后，将字节写入文件 */
  override protected def appendToFile(bytes: Array[Byte], len: Int): Unit = {
    if (rollingPolicy.shouldRollover(len)) {
      rollover()
      rollingPolicy.rolledOver()
    }
    super.appendToFile(bytes, len)
    rollingPolicy.bytesWritten(len)
  }

  /** 执行日志滚动：关闭当前输出流，移动文件，重新打开新文件，清理旧日志 */
  private def rollover(): Unit = {
    try {
      closeFile()
      moveFile()
      openFile()
      if (maxRetainedFiles > 0) {
        deleteOldFiles()
      }
    } catch {
      case e: Exception =>
        logError(log"Error rolling over ${MDC(PATH, activeFile)}", e)
    }
  }

  // 滚动日志文件，若开启压缩则压缩后保存
  private def rotateFile(activeFile: File, rolloverFile: File): Unit = {
    if (enableCompression) {
      val gzFile = new File(rolloverFile.getAbsolutePath + RollingFileAppender.GZIP_LOG_SUFFIX)
      var gzOutputStream: GZIPOutputStream = null
      var inputStream: InputStream = null
      try {
        inputStream = new FileInputStream(activeFile)
        gzOutputStream = new GZIPOutputStream(new FileOutputStream(gzFile))
        inputStream.transferTo(gzOutputStream)
        inputStream.close()
        gzOutputStream.close()
        activeFile.delete()
      } finally {
        Utils.closeQuietly(inputStream)
        Utils.closeQuietly(gzOutputStream)
      }
    } else {
      Utils.moveFile(activeFile, rolloverFile)
    }
  }

  // 检查滚动后的日志文件是否已存在（包含未压缩和已压缩两种情况）
  private def rolloverFileExist(file: File): Boolean = {
    file.exists || new File(file.getAbsolutePath + RollingFileAppender.GZIP_LOG_SUFFIX).exists
  }

  /** 将当前活跃日志文件移动到滚动日志文件 */
  private def moveFile(): Unit = {
    val rolloverSuffix = rollingPolicy.generateRolledOverFileSuffix()
    val rolloverFile = new File(
      activeFile.getParentFile, activeFile.getName + rolloverSuffix).getAbsoluteFile
    logDebug(s"Attempting to rollover file $activeFile to file $rolloverFile")
    if (activeFile.exists) {
      if (!rolloverFileExist(rolloverFile)) {
        rotateFile(activeFile, rolloverFile)
        logInfo(log"Rolled over ${MDC(FILE_NAME, activeFile)} to ${MDC(FILE_NAME2, rolloverFile)}")
      } else {
        // 若滚动文件名冲突，生成唯一文件名
        // 仅在冲突时使用，避免冲突可通过正确的命名模式实现
        var i = 0
        var altRolloverFile: File = null
        do {
          altRolloverFile = new File(activeFile.getParent,
            s"${activeFile.getName}$rolloverSuffix--$i").getAbsoluteFile
          i += 1
        } while (i < 10000 && rolloverFileExist(altRolloverFile))

        logWarning(log"Rollover file ${MDC(FILE_NAME, rolloverFile)} already exists, " +
          log"rolled over ${MDC(FILE_NAME2, activeFile)} " +
          log"to file ${MDC(FILE_NAME3, altRolloverFile)}")
        rotateFile(activeFile, altRolloverFile)
      }
    } else {
      logWarning(log"File ${MDC(FILE_NAME, activeFile)} does not exist")
    }
  }

  /** 删除超过保留数量限制的旧日志文件 */
  private[util] def deleteOldFiles(): Unit = {
    try {
      // 过滤出所有当前日志前缀的滚动日志文件（排除当前活跃文件）
      val rolledoverFiles = activeFile.getParentFile.listFiles(new FileFilter {
        def accept(f: File): Boolean = {
          f.getName.startsWith(activeFile.getName) && f != activeFile
        }
      }).sorted
      // 计算需要删除的文件（超过最大保留数量的最早文件）
      val filesToBeDeleted = rolledoverFiles.take(
        math.max(0, rolledoverFiles.length - maxRetainedFiles))
      filesToBeDeleted.foreach { file =>
        logInfo(log"Deleting file executor log file" +
          log" ${MDC(FILE_ABSOLUTE_PATH, file.getAbsolutePath)}")
        file.delete()
      }
    } catch {
      case e: Exception =>
        val path = activeFile.getParentFile.getAbsolutePath
        logError(log"Error cleaning logs in directory ${MDC(PATH, path)}", e)
    }
  }
}

/**
 * RollingFileAppender 的伴生对象，定义常量和工具方法。
 */
private[spark] object RollingFileAppender {
  val DEFAULT_BUFFER_SIZE = 8192

  val GZIP_LOG_SUFFIX = ".gz"

  /**
   * 获取排序后的滚动日志文件列表，将滚动日志按名称排序后追加当前活跃日志文件，
   * 用于按时间顺序聚合完整的日志内容。
   * @param directory 日志所在目录
   * @param activeFileName 当前活跃日志文件名
   * @return 排序后的文件序列，从最早到最新（最后是当前活跃文件）
   */
  def getSortedRolledOverFiles(directory: String, activeFileName: String): Seq[File] = {
    val rolledOverFiles = new File(directory).getAbsoluteFile.listFiles.filter { file =>
      val fileName = file.getName
      fileName.startsWith(activeFileName) && fileName != activeFileName
    }.sorted
    val activeFile = {
      val file = new File(directory, activeFileName).getAbsoluteFile
      if (file.exists) Some(file) else None
    }
    (rolledOverFiles.sortBy(_.getName.stripSuffix(GZIP_LOG_SUFFIX)) ++ activeFile)
      .toImmutableArraySeq
  }
}
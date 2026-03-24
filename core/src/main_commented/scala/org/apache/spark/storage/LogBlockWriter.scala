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

package org.apache.spark.storage

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream

import org.apache.commons.io.output.CountingOutputStream

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.storage.LogBlockType.LogBlockType
import org.apache.spark.util.Utils

/**
 * 文件级注释：日志块写入器，负责将日志条目写入本地磁盘临时文件，最终将完整日志块保存到BlockManager
 * 本类属于Spark核心存储模块，用于磁盘块日志的持久化写入流程
 *
 * A class for writing logs directly to a file on disk and save as a block in BlockManager if
 * there are any logs written.
 * `save` or `close` must be called to ensure resources are released properly. `save` will add
 * the log block to BlockManager, while `close` will just release the resources without saving
 * the log block.
 *
 * Notes:
 * - This class does not support concurrent writes.
 * - The writer will be automatically closed when failed to write logs or failed to save the
 *   log block.
 * - Write operations after closing will throw exceptions.
 */
private[spark] class LogBlockWriter(
    blockManager: BlockManager,
    logBlockType: LogBlockType,
    sparkConf: SparkConf,
    bufferSize: Int = 32 * 1024) extends Logging {

  private[storage] var tmpFile: File = null

  private var cos: CountingOutputStream = null
  private var objOut: SerializationStream = null
  private var hasBeenClosed = false
  private var recordsWritten = false
  private var totalBytesWritten = 0

  initialize()

  /**
   * 初始化日志块写入器，创建本地临时文件并初始化输出流和序列化流
   */
  private def initialize(): Unit = {
    try {
      // 获取Spark本地目录，创建临时文件存储日志
      val dir = new File(Utils.getLocalDir(sparkConf))
      tmpFile = File.createTempFile(s"spark_log_$logBlockType", "", dir)
      val fos = new FileOutputStream(tmpFile, false)
      val bos = new BufferedOutputStream(fos, bufferSize)
      // 使用计数输出流统计写入字节数
      cos = new CountingOutputStream(bos)
      val emptyBlockId = LogBlockId.empty(logBlockType)
      // 获取块管理器提供的序列化输出流，用于写入日志条目
      objOut = blockManager
        .serializerManager
        .blockSerializationStream(emptyBlockId, cos)(LogLine.getClassTag(logBlockType))
    } catch {
      case e: Exception =>
        logError(log"Failed to initialize LogBlockWriter.", e)
        // 初始化失败自动关闭资源
        close()
        throw e
    }
  }

  /**
   * 获取当前已写入的字节数，若输出流已关闭则返回最终总写入字节数
   * @return 已写入字节数
   */
  def bytesWritten(): Int = {
    Option(cos)
      .map(_.getCount)
      .getOrElse(totalBytesWritten)
  }

  /**
   * Write a log entry to the log block. Exception will be thrown if the writer has been closed
   * or if there is an error during writing. Caller needs to deal with the exception. Suggest to
   * close the writer when exception is thrown as block data could be corrupted which would lead
   * to issues when reading the log block later.
   *
   * @param logEntry The log entry to write.
   */
  def writeLog(logEntry: LogLine): Unit = {
    if (hasBeenClosed) {
      throw SparkException.internalError(
        "Writer already closed. Cannot write more data.",
        category = "STORAGE"
      )
    }

    try {
      // 序列化并写入日志条目
      objOut.writeObject(logEntry)
      // 标记已有记录写入
      recordsWritten = true
    } catch {
      case e: Exception =>
        logError(log"Failed to write log entry.", e)
        throw e
    }
  }

  /**
   * 保存日志块到 BlockManager
   * @param blockId 要保存的日志块ID
   */
  def save(blockId: LogBlockId): Unit = {
    if (hasBeenClosed) {
      throw SparkException.internalError(
        "Writer already closed. Cannot save.",
        category = "STORAGE"
      )
    }

    try {
      // 校验日志块类型与当前写入器类型一致
      if (blockId.logBlockType != logBlockType) {
        throw SparkException.internalError(
          s"LogBlockWriter is for $logBlockType, but got blockId $blockId")
      }

      // 刷新并关闭序列化输出流，完成文件写入
      objOut.flush()
      objOut.close()
      objOut = null

      if(recordsWritten) {
        // 获取总写入字节数
        totalBytesWritten = cos.getCount
        // 保存日志块到 BlockManager 并删除临时文件
        val success = saveToBlockManager(blockId, totalBytesWritten)
        if (!success) {
          throw SparkException.internalError(s"Failed to save log block $blockId to BlockManager")
        }
      }
    } finally {
      // 无论保存成功失败，最终关闭释放资源
      close()
    }
  }

  /**
   * 关闭写入器，释放所有资源，删除未保存的临时文件
   */
  def close(): Unit = {
    if (hasBeenClosed) {
      return
    }

    try {
      // 关闭序列化输出流
      if (objOut != null) {
        objOut.close()
      }
      // 删除临时文件，清理磁盘空间
      if (tmpFile != null && tmpFile.exists()) {
        tmpFile.delete()
      }
    } catch {
      case e: Exception =>
        logWarning(log"Failed to close resources of LogBlockWriter", e)
    } finally {
      // 清空引用，标记已关闭
      objOut = null
      cos = null
      hasBeenClosed = true
    }
  }

  // For test only.
  private[storage] def flush(): Unit = {
    if (objOut != null) {
      objOut.flush()
    }
  }

  /**
   * 将临时文件中的日志块注册保存到BlockManager
   * @param blockId 目标日志块ID
   * @param blockSize 日志块总大小（字节）
   * @return 保存是否成功
   */
  private[storage] def saveToBlockManager(blockId: LogBlockId, blockSize: Long): Boolean = {
    blockManager.
      TempFileBasedBlockStoreUpdater(
        blockId,
        StorageLevel.DISK_ONLY,
        LogLine.getClassTag(logBlockType),
        tmpFile,
        blockSize)
      .save()
  }
}
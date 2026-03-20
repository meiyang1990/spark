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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle

import java.io.{Closeable, OutputStream}
import java.util.zip.Checksum

import org.apache.spark.SparkException
import org.apache.spark.io.MutableCheckedOutputStream
import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.storage.{BlockId, TimeTrackingOutputStream}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.PairsWriter

/**
 * 键值对写入器，灵感来自 {@link DiskBlockObjectWriter}。
 * 将字节推送到任意分区写入器，而非通过 BlockManager 写入本地磁盘。
 *
 * @param partitionWriter 分区写入器
 * @param serializerManager 序列化管理器
 * @param serializerInstance 序列化实例
 * @param blockId 块 ID
 * @param writeMetrics 写入指标报告器
 * @param checksum 校验和（可选）
 */
private[spark] class ShufflePartitionPairsWriter(
    partitionWriter: ShufflePartitionWriter,
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    blockId: BlockId,
    writeMetrics: ShuffleWriteMetricsReporter,
    checksum: Checksum)
  extends PairsWriter with Closeable {

  private var isClosed = false
  private var partitionStream: OutputStream = _
  private var timeTrackingStream: OutputStream = _
  private var wrappedStream: OutputStream = _
  private var objOut: SerializationStream = _
  private var numRecordsWritten = 0
  private var curNumBytesWritten = 0L
  // 仅当 checksum != null 时初始化，表示启用了 Shuffle 校验和
  private var checksumOutputStream: MutableCheckedOutputStream = _

  override def write(key: Any, value: Any): Unit = {
    if (isClosed) {
      throw SparkException.internalError("Partition pairs writer is already closed.", "SHUFFLE")
    }
    if (objOut == null) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()
  }

  /** 打开输出流进行写入 */
  private def open(): Unit = {
    try {
      partitionStream = partitionWriter.openStream
      timeTrackingStream = new TimeTrackingOutputStream(writeMetrics, partitionStream)
      if (checksum != null) {
        checksumOutputStream = new MutableCheckedOutputStream(timeTrackingStream)
        checksumOutputStream.setChecksum(checksum)
      }
      wrappedStream = serializerManager.wrapStream(blockId,
        if (checksumOutputStream != null) checksumOutputStream else timeTrackingStream)
      objOut = serializerInstance.serializeStream(wrappedStream)
    } catch {
      case e: Exception =>
        Utils.tryLogNonFatalError {
          close()
        }
        throw e
    }
  }

  override def close(): Unit = {
    if (!isClosed) {
      Utils.tryWithSafeFinally {
        Utils.tryWithSafeFinally {
          objOut = closeIfNonNull(objOut)
          // 设置为 null 防止底层流被关闭两次
          // 以防某些流的 close() 实现不是幂等的
          wrappedStream = null
          timeTrackingStream = null
          partitionStream = null
        } {
          // 通常关闭 objOut 会同时关闭内部流，但以防初始化等出错，
          // 确保也清理其他流
          Utils.tryWithSafeFinally {
            wrappedStream = closeIfNonNull(wrappedStream)
            // 同上 - 如果 wrappedStream 关闭，假设它关闭了底层 partitionStream，
            // 不要在 finally 中再次关闭
            timeTrackingStream = null
            partitionStream = null
          } {
            Utils.tryWithSafeFinally {
              timeTrackingStream = closeIfNonNull(timeTrackingStream)
              partitionStream = null
            } {
              partitionStream = closeIfNonNull(partitionStream)
            }
          }
        }
        updateBytesWritten()
      } {
        isClosed = true
      }
    }
  }

  private def closeIfNonNull[T <: Closeable](closeable: T): T = {
    if (closeable != null) {
      closeable.close()
    }
    null.asInstanceOf[T]
  }

  /**
   * 通知写入器已通过 OutputStream#write 写入了一条记录的数据。
   */
  private def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /** 更新已写入字节数的指标 */
  private def updateBytesWritten(): Unit = {
    val numBytesWritten = partitionWriter.getNumBytesWritten
    val bytesWrittenDiff = numBytesWritten - curNumBytesWritten
    writeMetrics.incBytesWritten(bytesWrittenDiff)
    curNumBytesWritten = numBytesWritten
  }
}

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

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.network.shuffle.RemoteBlockPushResolver
import org.apache.spark.storage.LogBlockType.LogBlockType

/**
 * :: DeveloperApi ::
 * Block 的唯一标识符，通常与单个文件关联。
 * 每个 Block 可以通过文件名唯一识别，但不同类型的 Block 有不同的键集来生成其唯一名称。
 * 
 * 如果你的 BlockId 需要可序列化，请确保将其添加到 BlockId.apply() 方法中。
 */
@DeveloperApi
sealed abstract class BlockId {
  /** Block 的全局唯一标识符，可用于序列化/反序列化 */
  def name: String

  // 便捷方法
  def asRDDId: Option[RDDBlockId] = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  def isRDD: Boolean = isInstanceOf[RDDBlockId]
  // 判断是否为 Shuffle 相关的 Block（包括多种 Shuffle Block 类型）
  def isShuffle: Boolean = {
    (isInstanceOf[ShuffleBlockId] || isInstanceOf[ShuffleBlockBatchId] ||
     isInstanceOf[ShuffleDataBlockId] || isInstanceOf[ShuffleIndexBlockId])
  }
  def isShuffleChunk: Boolean = isInstanceOf[ShuffleBlockChunkId]
  def isBroadcast: Boolean = isInstanceOf[BroadcastBlockId]

  override def toString: String = name
}

// RDD Block 的标识符：rdd_{rddId}_{splitIndex}
@DeveloperApi
case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  override def name: String = "rdd_" + rddId + "_" + splitIndex
}

// Shuffle Block 的 ID 格式（包括 data 和 index）需要与
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getBlockData() 保持同步
@DeveloperApi
case class ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

// 同一 mapId 在范围 [startReduceId, endReduceId) 内的连续 shuffle block 的批次 ID
@DeveloperApi
case class ShuffleBlockBatchId(
    shuffleId: Int,
    mapId: Long,
    startReduceId: Int,
    endReduceId: Int) extends BlockId {
  override def name: String = {
    "shuffle_" + shuffleId + "_" + mapId + "_" + startReduceId + "_" + endReduceId
  }
}

// Shuffle 合并后的分块标识符（Push-based Shuffle）
@Since("3.2.0")
@DeveloperApi
case class ShuffleBlockChunkId(
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int,
    chunkId: Int) extends BlockId {
  override def name: String =
    "shuffleChunk_" + shuffleId  + "_" + shuffleMergeId + "_" + reduceId + "_" + chunkId
}

// Shuffle 数据块标识符（.data 文件）
@DeveloperApi
case class ShuffleDataBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
}

// Shuffle 索引块标识符（.index 文件）
@DeveloperApi
case class ShuffleIndexBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
}

// Shuffle 校验和块标识符（.checksum 文件）
@Since("3.2.0")
@DeveloperApi
case class ShuffleChecksumBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".checksum"
}

// Shuffle Push 块标识符（Push-based Shuffle 推送的块）
@Since("3.2.0")
@DeveloperApi
case class ShufflePushBlockId(
    shuffleId: Int,
    shuffleMergeId: Int,
    mapIndex: Int,
    reduceId: Int) extends BlockId {
  override def name: String = "shufflePush_" + shuffleId + "_" +
    shuffleMergeId + "_" + mapIndex + "_" + reduceId + ""
}

// Shuffle 合并块标识符（多个 map 输出合并后的块）
@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedBlockId(
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = "shuffleMerged_" + shuffleId + "_" +
    shuffleMergeId + "_" + reduceId
}

// Shuffle 合并数据块标识符（合并后的 .data 文件，包含 appId）
@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedDataBlockId(
    appId: String,
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = RemoteBlockPushResolver.MERGED_SHUFFLE_FILE_NAME_PREFIX + "_" +
    appId + "_" + shuffleId + "_" + shuffleMergeId + "_" + reduceId + ".data"
}

// Shuffle 合并索引块标识符（合并后的 .index 文件）
@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedIndexBlockId(
    appId: String,
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = RemoteBlockPushResolver.MERGED_SHUFFLE_FILE_NAME_PREFIX + "_" +
    appId + "_" + shuffleId + "_" + shuffleMergeId + "_" + reduceId + ".index"
}

// Shuffle 合并元数据块标识符（合并后的 .meta 文件）
@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedMetaBlockId(
    appId: String,
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = RemoteBlockPushResolver.MERGED_SHUFFLE_FILE_NAME_PREFIX + "_" +
    appId + "_" + shuffleId + "_" + shuffleMergeId + "_" + reduceId + ".meta"
}

// Broadcast 变量块标识符
@DeveloperApi
case class BroadcastBlockId(broadcastId: Long, field: String = "") extends BlockId {
  override def name: String = "broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
}

// Task 结果块标识符（用于大型任务结果）
@DeveloperApi
case class TaskResultBlockId(taskId: Long) extends BlockId {
  override def name: String = "taskresult_" + taskId
}

// 流块标识符（Spark Streaming）
@DeveloperApi
case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  override def name: String = "input-" + streamId + "-" + uniqueId
}

// Python 流块标识符（PySpark Streaming）
@DeveloperApi
case class PythonStreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  override def name: String = "python-stream-" + streamId + "-" + uniqueId
}

// 日志块类型枚举
object LogBlockType extends Enumeration {
  type LogBlockType = Value
  val TEST = Value
  val PYTHON_WORKER = Value
}

/**
 * 日志数据块标识符。
 *
 * @param lastLogTime 该块中最后一条日志的时间戳，用于过滤和日志管理
 * @param executorId 产生此日志块的 executor ID
 */
abstract sealed class LogBlockId extends BlockId {
  def lastLogTime: Long
  def executorId: String
  def logBlockType: LogBlockType
}

object LogBlockId {
  // 根据日志类型创建空的 LogBlockId
  def empty(logBlockType: LogBlockType): LogBlockId = {
    logBlockType match {
      case LogBlockType.TEST => TestLogBlockId(0L, "")
      case LogBlockType.PYTHON_WORKER => PythonWorkerLogBlockId(0L, "", "", "")
      case _ => throw new SparkException(s"Unsupported log block type: $logBlockType")
    }
  }
}

// 仅用于测试目的的日志块
case class TestLogBlockId(lastLogTime: Long, executorId: String)
  extends LogBlockId {
  override def name: String =
    "test_log_" + lastLogTime + "_" + executorId

  override def logBlockType: LogBlockType = LogBlockType.TEST
}

/**
 * Python Worker 日志块标识符。
 *
 * @param lastLogTime 该块中最后一条日志的时间戳，用于过滤和日志管理
 * @param executorId 产生此日志块的 executor ID
 * @param sessionId 会话 ID，用于隔离日志
 * @param workerId Worker ID，用于区分 Python Worker 进程
 */
@DeveloperApi
case class PythonWorkerLogBlockId(
    lastLogTime: Long,
    executorId: String,
    sessionId: String,
    workerId: String)
  extends LogBlockId {
  override def name: String = {
    s"python_worker_log_${lastLogTime}_${executorId}_${sessionId}_$workerId"
  }

  override def logBlockType: LogBlockType = LogBlockType.PYTHON_WORKER
}

/** 与作为 block 管理的临时本地数据关联的 ID，不可序列化 */
private[spark] case class TempLocalBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_local_" + id
}

/** 与作为 block 管理的临时 shuffle 数据关联的 ID，不可序列化 */
private[spark] case class TempShuffleBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_shuffle_" + id
}

// 仅用于测试目的
private[spark] case class TestBlockId(id: String) extends BlockId {
  override def name: String = "test_" + id
}

// 无法识别的 BlockId 异常
@DeveloperApi
class UnrecognizedBlockId(name: String)
    extends SparkException(s"Failed to parse $name into a block ID")

// 缓存块标识符（用于 SQL 缓存）
@DeveloperApi
case class CacheId(sessionUUID: String, hash: String) extends BlockId {
  override def name: String = s"cache_${sessionUUID}_$hash"
}

@DeveloperApi
object BlockId {
  // 用于解析各种 BlockId 名称的正则表达式
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_BATCH = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val SHUFFLE_PUSH = "shufflePush_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_MERGED = "shuffleMerged_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_MERGED_DATA =
    "shuffleMerged_([_A-Za-z0-9]*)_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_MERGED_INDEX =
    "shuffleMerged_([_A-Za-z0-9]*)_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val SHUFFLE_MERGED_META =
    "shuffleMerged_([_A-Za-z0-9]*)_([0-9]+)_([0-9]+)_([0-9]+).meta".r
  val SHUFFLE_CHUNK = "shuffleChunk_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r
  val BROADCAST = "broadcast_([0-9]+)([_A-Za-z0-9]*)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val PYTHON_STREAM = "python-stream-([0-9]+)-([0-9]+)".r
  val TEMP_LOCAL = "temp_local_([-A-Fa-f0-9]+)".r
  val TEMP_SHUFFLE = "temp_shuffle_([-A-Fa-f0-9]+)".r
  val TEST = "test_(.*)".r
  val TEST_LOG_BLOCK = "test_log_([0-9]+)_(.*)".r
  val PYTHON_WORKER_LOG_BLOCK = "python_worker_log_([0-9]+)_([^_]*)_([^_]*)_([^_]*)".r

  // 根据 BlockId 名称字符串反序列化为对应的 BlockId 对象
  def apply(name: String): BlockId = name match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
    case SHUFFLE_BATCH(shuffleId, mapId, startReduceId, endReduceId) =>
      ShuffleBlockBatchId(shuffleId.toInt, mapId.toLong, startReduceId.toInt, endReduceId.toInt)
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      ShuffleDataBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
    case SHUFFLE_PUSH(shuffleId, shuffleMergeId, mapIndex, reduceId) =>
      ShufflePushBlockId(shuffleId.toInt, shuffleMergeId.toInt, mapIndex.toInt,
        reduceId.toInt)
    case SHUFFLE_MERGED(shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedBlockId(shuffleId.toInt, shuffleMergeId.toInt, reduceId.toInt)
    case SHUFFLE_MERGED_DATA(appId, shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedDataBlockId(appId, shuffleId.toInt, shuffleMergeId.toInt, reduceId.toInt)
    case SHUFFLE_MERGED_INDEX(appId, shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedIndexBlockId(appId, shuffleId.toInt, shuffleMergeId.toInt,
        reduceId.toInt)
    case SHUFFLE_MERGED_META(appId, shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedMetaBlockId(appId, shuffleId.toInt, shuffleMergeId.toInt,
        reduceId.toInt)
    case SHUFFLE_CHUNK(shuffleId, shuffleMergeId, reduceId, chunkId) =>
      ShuffleBlockChunkId(shuffleId.toInt, shuffleMergeId.toInt, reduceId.toInt,
        chunkId.toInt)
    case BROADCAST(broadcastId, field) =>
      BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case PYTHON_STREAM(streamId, uniqueId) =>
      PythonStreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEMP_LOCAL(uuid) =>
      TempLocalBlockId(UUID.fromString(uuid))
    case TEMP_SHUFFLE(uuid) =>
      TempShuffleBlockId(UUID.fromString(uuid))
    case TEST_LOG_BLOCK(lastLogTime, executorId) =>
      TestLogBlockId(lastLogTime.toLong, executorId)
    case PYTHON_WORKER_LOG_BLOCK(lastLogTime, executorId, sessionId, workerId) =>
      PythonWorkerLogBlockId(lastLogTime.toLong, executorId, sessionId, workerId)
    case TEST(value) =>
      TestBlockId(value)
    case _ => throw SparkCoreErrors.unrecognizedBlockIdError(name)
  }
}

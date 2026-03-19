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

package org.apache.spark

import java.io.{ByteArrayInputStream, InputStream, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection
import scala.collection.mutable.{HashMap, ListBuffer, Map, Set}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.io.output.{ByteArrayOutputStream => ApacheByteArrayOutputStream}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{MapStatus, MergeStatus, ShuffleOutputStatus}
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId, ShuffleMergedBlockId}
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * 用于 [[MapOutputTrackerMaster]] 对单个 ShuffleMapStage 进行状态簿记的辅助类。
 * 维护 map index 到 MapStatus 的映射关系，并缓存序列化后的 map 状态信息以加速查询。
 * 所有公开方法均为线程安全。
 */
private class ShuffleStatus(
    numPartitions: Int,
    numReducers: Int = -1) extends Logging {

  // 读写锁，用于保护下面所有可变状态的并发访问
  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock(), lock.writeLock())
  }

  // 在读锁保护下执行操作
  private def withReadLock[B](fn: => B): B = {
    readLock.lock()
    try {
      fn
    } finally {
      readLock.unlock()
    }
  }

  // 在写锁保护下执行操作
  private def withWriteLock[B](fn: => B): B = {
    writeLock.lock()
    try {
      fn
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * 每个分区的 MapStatus 数组，索引为 map 分区 id。
   * 值为对应分区的 MapStatus，若分区尚未完成则为 null。
   * 虽然理论上任务可能多次运行（推测执行、Stage 重试等），但实践中同一输出
   * 存在多个位置的概率极低，因此只存储单个位置。
   */
  // Exposed for testing
  val mapStatuses = new Array[MapStatus](numPartitions)

  // 保留已删除的 MapStatus，用于故障恢复
  val mapStatusesDeleted = new Array[MapStatus](numPartitions)

  // 记录跨重试校验和不一致的 Map 任务索引集合
  private[spark] val checksumMismatchIndices: Set[Int] = Set()

  /**
   * Push-Based Shuffle 启用时，每个 shuffle 分区的 MergeStatus 数组。
   * 索引为 shuffle 分区 id（reduce id），提供以 reducer 视角查看
   * shuffle 分区块合并结果的状态。
   */
  val mergeStatuses = if (numReducers > 0) {
    new Array[MergeStatus](numReducers)
  } else {
    Array.empty[MergeStatus]
  }

  // 序列化后的 map 状态缓存，在 serializedMapStatus 调用时懒加载填充
  private[this] var cachedSerializedMapStatus: Array[Byte] = _

  /**
   * 持有序列化 map 输出状态的广播变量。当序列化结果过大无法通过单次 RPC 发送时，
   * 将结果放入广播变量发送；此引用防止广播变量被 GC 回收，并支持后续显式销毁。
   */
  private[spark] var cachedSerializedBroadcast: Broadcast[Array[Array[Byte]]] = _

  // 与 cachedSerializedMapStatus/cachedSerializedBroadcast 类似，但用于 MergeStatus
  private[this] var cachedSerializedMergeStatus: Array[Byte] = _

  private[this] var cachedSerializedBroadcastMergeStatus: Broadcast[Array[Array[Byte]]] = _

  // 已产出输出的分区计数器，等价于 mapStatuses.count(_ ne null)，用于避免遍历数组
  private[this] var _numAvailableMapOutputs: Int = 0

  // 已收到的 MergeStatus 结果计数器
  private[this] var _numAvailableMergeResults: Int = 0

  private[this] var shufflePushMergerLocations: Seq[BlockManagerId] = Seq.empty

  // mapId 到 mapIndex 的映射，用于减少 updateMapOutput 中的搜索开销
  private[spark] val mapIdToMapIndex = new HashMap[Long, Int]()

  /**
   * 注册一个 map 输出。若该 mapIndex 已有注册位置，将被新位置覆盖。
   * 若新 MapStatus 的校验和与之前注册的不同，返回 true，否则返回 false。
   */
  def addMapOutput(mapIndex: Int, status: MapStatus): Boolean = withWriteLock {
    var isChecksumMismatch: Boolean = false
    val currentMapStatus = mapStatuses(mapIndex)
    // 若该分区之前无输出，增加可用计数并清除序列化缓存
    if (currentMapStatus == null) {
      _numAvailableMapOutputs += 1
      invalidateSerializedMapOutputStatusCache()
    } else {
      mapIdToMapIndex.remove(currentMapStatus.mapId)
    }
    logDebug(s"Checksum of map output for task ${status.mapId} is ${status.checksumValue}")

    // 检查新旧 MapStatus 校验和是否一致，不一致则记录到 checksumMismatchIndices
    val preStatus =
      if (mapStatuses(mapIndex) != null) mapStatuses(mapIndex) else mapStatusesDeleted(mapIndex)
    if (preStatus != null && preStatus.checksumValue != status.checksumValue) {
      logInfo(s"Checksum of map output changes from ${preStatus.checksumValue} to " +
        s"${status.checksumValue} for task ${status.mapId}.")
      checksumMismatchIndices.add(mapIndex)
      isChecksumMismatch = true
    }
    mapStatuses(mapIndex) = status
    mapIdToMapIndex(status.mapId) = mapIndex
    isChecksumMismatch
  }

  // 根据 mapId 获取对应的 MapStatus
  def getMapStatus(mapId: Long): Option[MapStatus] = withReadLock {
    mapIdToMapIndex.get(mapId).map(mapStatuses(_)) match {
      case Some(null) => None
      case m => m
    }
  }

  // 更新 map 输出的位置信息（例如在迁移期间），支持从已删除的状态中恢复
  def updateMapOutput(mapId: Long, bmAddress: BlockManagerId): Unit = withWriteLock {
    try {
      val mapIndex = mapIdToMapIndex.get(mapId)
      val mapStatusOpt = mapIndex.map(mapStatuses(_)).flatMap(Option(_))
      mapStatusOpt match {
        // 正常情况：该 mapId 仍在 mapStatuses 中，直接更新位置
        case Some(mapStatus) =>
          logInfo(log"Updating map output for ${MDC(MAP_ID, mapId)}" +
            log" to ${MDC(BLOCK_MANAGER_ID, bmAddress)}")
          mapStatus.updateLocation(bmAddress)
          invalidateSerializedMapOutputStatusCache()
        case None =>
          // 在已删除的状态中查找，找到则恢复到 mapStatuses
          val index = mapStatusesDeleted.indexWhere(x => x != null && x.mapId == mapId)
          if (index >= 0 && mapStatuses(index) == null) {
            val mapStatus = mapStatusesDeleted(index)
            mapStatus.updateLocation(bmAddress)
            mapStatuses(index) = mapStatus
            _numAvailableMapOutputs += 1
            invalidateSerializedMapOutputStatusCache()
            mapStatusesDeleted(index) = null
            logInfo(log"Recover ${MDC(MAP_ID, mapStatus.mapId)}" +
              log" ${MDC(BLOCK_MANAGER_ID, mapStatus.location)}")
          } else {
            logWarning(log"Asked to update map output ${MDC(MAP_ID, mapId)} " +
              log"for untracked map status.")
          }
      }
    } catch {
      case e: java.lang.NullPointerException =>
        logWarning(log"Unable to update map output for ${MDC(MAP_ID, mapId)}, " +
          log"status removed in-flight")
    }
  }

  // 移除指定 BlockManager 提供的 map 输出（仅当注册的输出确实来自该 BlockManager 时才移除）
  def removeMapOutput(mapIndex: Int, bmAddress: BlockManagerId): Unit = withWriteLock {
    logDebug(s"Removing existing map output ${mapIndex} ${bmAddress}")
    val currentMapStatus = mapStatuses(mapIndex)
    if (currentMapStatus != null && currentMapStatus.location == bmAddress) {
      _numAvailableMapOutputs -= 1
      mapIdToMapIndex.remove(currentMapStatus.mapId)
      mapStatusesDeleted(mapIndex) = currentMapStatus
      mapStatuses(mapIndex) = null
      invalidateSerializedMapOutputStatusCache()
    }
  }

  // 注册一个 merge 结果（Push-Based Shuffle）
  def addMergeResult(reduceId: Int, status: MergeStatus): Unit = withWriteLock {
    if (mergeStatuses(reduceId) != status) {
      _numAvailableMergeResults += 1
      invalidateSerializedMergeOutputStatusCache()
    }
    mergeStatuses(reduceId) = status
  }

  // 注册 shuffle push merger 的位置信息（仅在首次为空时设置）
  def registerShuffleMergerLocations(shuffleMergers: Seq[BlockManagerId]): Unit = withWriteLock {
    if (shufflePushMergerLocations.isEmpty) {
      shufflePushMergerLocations = shuffleMergers
    }
  }

  // 清除 shuffle push merger 位置信息
  def removeShuffleMergerLocations(): Unit = withWriteLock {
    shufflePushMergerLocations = Nil
  }

  // TODO support updateMergeResult for similar use cases as updateMapOutput

  // 移除指定 BlockManager 提供的 merge 结果
  def removeMergeResult(reduceId: Int, bmAddress: BlockManagerId): Unit = withWriteLock {
    if (mergeStatuses(reduceId) != null && mergeStatuses(reduceId).location == bmAddress) {
      _numAvailableMergeResults -= 1
      mergeStatuses(reduceId) = null
      invalidateSerializedMergeOutputStatusCache()
    }
  }

  // 移除指定 host 上的所有 shuffle 输出（含外部 shuffle 服务的输出）
  def removeOutputsOnHost(host: String): Unit = withWriteLock {
    logDebug(s"Removing outputs for host ${host}")
    removeOutputsByFilter(x => x.host == host)
    removeMergeResultsByFilter(x => x.host == host)
  }

  // 移除指定 Executor 上的所有 map 输出
  def removeOutputsOnExecutor(execId: String): Unit = withWriteLock {
    logDebug(s"Removing outputs for execId ${execId}")
    removeOutputsByFilter(x => x.executorId == execId)
  }

  // 按过滤条件移除所有匹配的 shuffle 输出
  def removeOutputsByFilter(f: BlockManagerId => Boolean): Unit = withWriteLock {
    for (mapIndex <- mapStatuses.indices) {
      val currentMapStatus = mapStatuses(mapIndex)
      if (currentMapStatus != null && f(currentMapStatus.location)) {
        _numAvailableMapOutputs -= 1
        mapIdToMapIndex.remove(currentMapStatus.mapId)
        mapStatusesDeleted(mapIndex) = currentMapStatus
        mapStatuses(mapIndex) = null
        invalidateSerializedMapOutputStatusCache()
      }
    }
  }

  // 按过滤条件移除所有匹配的 merge 结果
  def removeMergeResultsByFilter(f: BlockManagerId => Boolean): Unit = withWriteLock {
    for (reduceId <- mergeStatuses.indices) {
      if (mergeStatuses(reduceId) != null && f(mergeStatuses(reduceId).location)) {
        _numAvailableMergeResults -= 1
        mergeStatuses(reduceId) = null
        invalidateSerializedMergeOutputStatusCache()
      }
    }
  }

  // 已有 shuffle map 输出的分区数量
  def numAvailableMapOutputs: Int = withReadLock {
    _numAvailableMapOutputs
  }

  // Push-Based Shuffle 场景下已完成 merge 的分区数量
  def numAvailableMergeResults: Int = withReadLock {
    _numAvailableMergeResults
  }

  // 返回尚未完成计算的分区 id 序列
  def findMissingPartitions(): Seq[Int] = withReadLock {
    val missing = (0 until numPartitions).filter(id => mapStatuses(id) == null)
    assert(missing.size == numPartitions - _numAvailableMapOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableMapOutputs}")
    missing
  }

  /**
   * 将 mapStatuses 数组序列化为高效的压缩格式。
   * 支持多线程并发调用，采用缓存机制：若缓存为空且多个线程同时请求，
   * 仅一个线程执行序列化，其余线程阻塞等待缓存填充。
   */
  def serializedMapStatus(
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): Array[Byte] = {
    var result: Array[Byte] = null
    // 先尝试读缓存
    withReadLock {
      if (cachedSerializedMapStatus != null) {
        result = cachedSerializedMapStatus
      }
    }

    // 缓存未命中时获取写锁并执行序列化
    if (result == null) withWriteLock {
      if (cachedSerializedMapStatus == null) {
        val serResult = MapOutputTracker.serializeOutputStatuses[MapStatus](
          mapStatuses, broadcastManager, isLocal, minBroadcastSize, conf)
        cachedSerializedMapStatus = serResult._1
        cachedSerializedBroadcast = serResult._2
      }
      // 必须在 if 外赋值：另一个线程可能在 withReadLock 和 withWriteLock 之间完成了初始化
      result = cachedSerializedMapStatus
    }
    result
  }

  /**
   * 将 mapStatuses 和 mergeStatuses 同时序列化为高效压缩格式。
   * 同样采用缓存机制加速重复请求。
   */
  def serializedMapAndMergeStatus(
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): (Array[Byte], Array[Byte]) = {
    val mapStatusesBytes: Array[Byte] =
      serializedMapStatus(broadcastManager, isLocal, minBroadcastSize, conf)
    var mergeStatusesBytes: Array[Byte] = null

    withReadLock {
      if (cachedSerializedMergeStatus != null) {
        mergeStatusesBytes = cachedSerializedMergeStatus
      }
    }

    if (mergeStatusesBytes == null) withWriteLock {
      if (cachedSerializedMergeStatus == null) {
        val serResult = MapOutputTracker.serializeOutputStatuses[MergeStatus](
          mergeStatuses, broadcastManager, isLocal, minBroadcastSize, conf)
        cachedSerializedMergeStatus = serResult._1
        cachedSerializedBroadcastMergeStatus = serResult._2
      }

      mergeStatusesBytes = cachedSerializedMergeStatus
    }
    (mapStatusesBytes, mergeStatusesBytes)
  }

  // Used in testing.
  def hasCachedSerializedBroadcast: Boolean = withReadLock {
    cachedSerializedBroadcast != null
  }

  // 在读锁保护下对 mapStatuses 数组执行只读操作
  def withMapStatuses[T](f: Array[MapStatus] => T): T = withReadLock {
    f(mapStatuses)
  }

  // 在读锁保护下对 mergeStatuses 数组执行只读操作
  def withMergeStatuses[T](f: Array[MergeStatus] => T): T = withReadLock {
    f(mergeStatuses)
  }

  // 获取 shuffle push merger 的位置列表
  def getShufflePushMergerLocations: Seq[BlockManagerId] = withReadLock {
    shufflePushMergerLocations
  }

  // 清除已缓存的序列化 map 输出状态（含广播变量的销毁）
  def invalidateSerializedMapOutputStatusCache(): Unit = withWriteLock {
    if (cachedSerializedBroadcast != null) {
      // 防止广播变量清理时异常导致 DAGScheduler 崩溃（SPARK-21444）
      Utils.tryLogNonFatalError {
        // 使用非阻塞方式销毁，避免因向已死亡的 Executor 发送清理 RPC 而挂起
        cachedSerializedBroadcast.destroy()
      }
      cachedSerializedBroadcast = null
    }
    cachedSerializedMapStatus = null
  }

  // 清除已缓存的序列化 merge 输出状态
  def invalidateSerializedMergeOutputStatusCache(): Unit = withWriteLock {
    if (cachedSerializedBroadcastMergeStatus != null) {
      Utils.tryLogNonFatalError {
        cachedSerializedBroadcastMergeStatus.destroy()
      }
      cachedSerializedBroadcastMergeStatus = null
    }
    cachedSerializedMergeStatus = null
  }
}

// MapOutputTracker 的 RPC 消息协议
private[spark] sealed trait MapOutputTrackerMessage
// 请求获取指定 shuffle 的 map 输出状态
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
// 请求获取指定 shuffle 的 map 和 merge 输出状态
private[spark] case class GetMapAndMergeResultStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
// 请求获取指定 shuffle 的 push merger 位置
private[spark] case class GetShufflePushMergerLocations(shuffleId: Int)
  extends MapOutputTrackerMessage
// 停止 MapOutputTracker 的信号消息
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

// MapOutputTrackerMaster 内部消息队列使用的消息类型
private[spark] sealed trait MapOutputTrackerMasterMessage
private[spark] case class GetMapOutputMessage(shuffleId: Int,
  context: RpcCallContext) extends MapOutputTrackerMasterMessage
private[spark] case class GetMapAndMergeOutputMessage(shuffleId: Int,
  context: RpcCallContext) extends MapOutputTrackerMasterMessage
private[spark] case class GetShufflePushMergersMessage(shuffleId: Int,
  context: RpcCallContext) extends MapOutputTrackerMasterMessage
// 封装 Executor 按 BlockManagerId 分组的 shuffle 块大小信息及是否启用批量拉取
private[spark] case class MapSizesByExecutorId(
  iter: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])], enableBatchFetch: Boolean)

/** MapOutputTrackerMaster 的 RPC 端点，接收来自 Executor 的状态查询请求 */
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  private def logInfoMsg(msg: MessageWithContext, shuffleId: Int, context: RpcCallContext): Unit = {
    val hostPort = context.senderAddress.hostPort
    logInfo(log"Asked to send " +
      msg +
      log" locations for shuffle ${MDC(SHUFFLE_ID, shuffleId)} to ${MDC(HOST_PORT, hostPort)}")
  }

  // 接收并回复来自 Executor 的请求，将消息转发到 tracker 的消息队列
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      logInfoMsg(log"map output", shuffleId, context)
      tracker.post(GetMapOutputMessage(shuffleId, context))

    case GetMapAndMergeResultStatuses(shuffleId: Int) =>
      logInfoMsg(log"map/merge result", shuffleId, context)
      tracker.post(GetMapAndMergeOutputMessage(shuffleId, context))

    case GetShufflePushMergerLocations(shuffleId: Int) =>
      logInfoMsg(log"shuffle push merger", shuffleId, context)
      tracker.post(GetShufflePushMergersMessage(shuffleId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
 * 追踪 Stage 的 map 输出位置的抽象基类。
 * Driver 端和 Executor 端各有不同的子类实现（MapOutputTrackerMaster 和 MapOutputTrackerWorker）。
 */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
  // 指向 Driver 端 MapOutputTrackerMasterEndpoint 的 RPC 引用
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * epoch 计数器：Driver 端每次丢失 map 输出时递增。
   * 该值随 task 发送给 Executor，Executor 比对新旧 epoch，
   * 若新 epoch 更大则清除本地缓存并重新拉取状态。
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  // 向 trackerEndpoint 发送同步请求并等待结果，超时则抛出 SparkException
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  // 向 trackerEndpoint 发送单向消息，期望返回 true 确认
  protected def sendTracker(message: Any): Unit = {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  // For testing
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
      : Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    getMapSizesByExecutorId(shuffleId, 0, Int.MaxValue, reduceId, reduceId + 1)
  }

  // For testing
  def getPushBasedShuffleMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
      : MapSizesByExecutorId = {
    getPushBasedShuffleMapSizesByExecutorId(shuffleId, 0, Int.MaxValue, reduceId, reduceId + 1)
  }

  /**
   * 由 Executor 调用，获取非 Push-Based Shuffle 场景下指定 shuffle 块的服务端地址和大小信息。
   * startPartition 包含在范围内，endPartition 不包含。
   * startMapIndex 包含在范围内，endMapIndex 不包含；endMapIndex=Int.MaxValue 表示全部 map 输出。
   * 返回 (BlockManagerId, Seq[(块ID, 块大小, map索引)]) 的迭代器，零大小块已过滤。
   */
  def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])]

  /**
   * 由 Executor 调用，获取 Push-Based Shuffle 场景下指定 shuffle 块的服务端地址和大小信息。
   * 返回 MapSizesByExecutorId，包含块信息迭代器和是否启用批量拉取的标志。
   */
  def getPushBasedShuffleMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): MapSizesByExecutorId

  /**
   * Executor 在整个合并 shuffle reduce 分区拉取失败时调用。
   * 获取被合并到指定 merged shuffle block 中的各原始 shuffle 块信息，用于回退到拉取未合并块。
   */
  def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])]

  /**
   * Executor 在合并 shuffle 分区块（chunk）拉取失败时调用。
   * chunkBitMap 追踪属于当前 merged chunk 的 mapId，用于回退到拉取对应的原始块。
   */
  def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int,
      chunkBitmap: RoaringBitmap): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])]

  /**
   * Executor 在 Push-Based Shuffle 无可用 merger 时调用（通常出现在初始 Stage，注册 Executor 不足）。
   * 尽力而为地返回可用的 shuffle merger 位置，若无则返回空序列表示任务应跳过 shuffle push。
   */
  def getShufflePushMergerLocations(shuffleId: Int): Seq[BlockManagerId]

  // 删除指定 shuffle stage 的 map 输出状态信息
  def unregisterShuffle(shuffleId: Int): Unit

  def stop(): Unit = {}
}

/**
 * Driver 端的 MapOutputTracker 实现，负责追踪 Stage 的 map 输出位置。
 * DAGScheduler 使用它来注册/注销 map 输出状态，并查询统计信息以执行数据本地性感知的 reduce 任务调度。
 * ShuffleMapStage 通过它追踪可用/缺失的输出以确定需要运行的任务。
 */
private[spark] class MapOutputTrackerMaster(
    conf: SparkConf,
    private[spark] val broadcastManager: BroadcastManager,
    private[spark] val isLocal: Boolean)
  extends MapOutputTracker(conf) {

  // 序列化后的 map 输出状态超过此阈值时使用广播变量发送给 Executor
  private val minSizeForBroadcast = conf.get(SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST).toInt

  // 是否为 reduce 任务计算数据本地性偏好
  private val shuffleLocalityEnabled = conf.get(SHUFFLE_REDUCE_LOCALITY_ENABLE)

  // 是否启用 shuffle 块迁移（退役节点场景）
  private val shuffleMigrationEnabled = conf.get(DECOMMISSION_ENABLED) &&
    conf.get(STORAGE_DECOMMISSION_ENABLED) && conf.get(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)

  // 超过此 map/reduce 任务数时不再计算基于输出大小的首选位置（避免大作业的开销）
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // 某个位置的 map 输出占总输出的比例达到此阈值时，才被认为是 reduce 任务的首选位置
  private val REDUCER_PREF_LOCS_FRACTION = 0.2

  // Driver 端存储所有 shuffle 状态的哈希表（按 shuffleId 索引）
  val shuffleStatuses = new ConcurrentHashMap[Int, ShuffleStatus]().asScala

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // 处理来自 Executor 的 MapOutputTracker 请求的消息队列
  private val mapOutputTrackerMasterMessages =
    new LinkedBlockingQueue[MapOutputTrackerMasterMessage]

  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(conf, isDriver = true)

  // 用于处理 map 输出状态请求的独立线程池，避免阻塞正常的 RPC 分发线程
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.get(SHUFFLE_MAPOUTPUT_DISPATCHER_NUM_THREADS)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  private val availableProcessors = Runtime.getRuntime.availableProcessors()

  // Make sure that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val logEntry = log"${MDC(CONFIG, SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST.key)} " +
      log"(${MDC(MIN_SIZE, minSizeForBroadcast)} bytes) " +
      log"must be <= spark.rpc.message.maxSize (${MDC(MAX_SIZE, maxRpcMessageSize)} " +
      log"bytes) to prevent sending an rpc message that is too large."
    logError(logEntry)
    throw new IllegalArgumentException(logEntry.message)
  }

  // 将消息投递到内部消息队列，由 MessageLoop 线程异步处理
  def post(message: MapOutputTrackerMasterMessage): Unit = {
    mapOutputTrackerMasterMessages.offer(message)
  }

  /** 内部消息循环，从队列中取出请求消息并序列化 shuffle 状态回复给 Executor */
  private class MessageLoop extends Runnable {
    // 处理状态查询消息：根据 needMergeOutput 决定回复 map 状态还是 map+merge 状态
    private def handleStatusMessage(
        shuffleId: Int,
        context: RpcCallContext,
        needMergeOutput: Boolean): Unit = {
      val hostPort = context.senderAddress.hostPort
      val shuffleStatus = shuffleStatuses.get(shuffleId).head
      logDebug(s"Handling request to send ${if (needMergeOutput) "map/merge" else "map"}" +
        s" output locations for shuffle $shuffleId to $hostPort")
      if (needMergeOutput) {
        context.reply(
          shuffleStatus.
            serializedMapAndMergeStatus(broadcastManager, isLocal, minSizeForBroadcast, conf))
      } else {
        context.reply(
          shuffleStatus.serializedMapStatus(broadcastManager, isLocal, minSizeForBroadcast, conf))
      }
    }

    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputTrackerMasterMessages.take()
            // 收到毒丸消息则将其放回（让其他 MessageLoop 线程也能看到）并退出
            if (data == PoisonPill) {
              mapOutputTrackerMasterMessages.offer(PoisonPill)
              return
            }

            data match {
              case GetMapOutputMessage(shuffleId, context) =>
                handleStatusMessage(shuffleId, context, false)
              case GetMapAndMergeOutputMessage(shuffleId, context) =>
                handleStatusMessage(shuffleId, context, true)
              case GetShufflePushMergersMessage(shuffleId, context) =>
                logDebug(s"Handling request to send shuffle push merger locations for shuffle" +
                  s" $shuffleId to ${context.senderAddress.hostPort}")
                context.reply(shuffleStatuses.get(shuffleId).map(_.getShufflePushMergerLocations)
                  .getOrElse(Seq.empty[BlockManagerId]))
            }
          } catch {
            case NonFatal(e) => logError(log"${MDC(ERROR, e.getMessage)}", e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  // 毒丸消息：通知 MessageLoop 线程退出消息循环
  private val PoisonPill = GetMapOutputMessage(-99, null)

  // Used only in unit tests.
  private[spark] def getNumCachedSerializedBroadcast: Int = {
    shuffleStatuses.valuesIterator.count(_.hasCachedSerializedBroadcast)
  }

  // 注册新的 shuffle，根据是否启用 Push-Based Shuffle 创建不同的 ShuffleStatus
  def registerShuffle(shuffleId: Int, numMaps: Int, numReduces: Int): Unit = {
    if (pushBasedShuffleEnabled) {
      if (shuffleStatuses.put(shuffleId, new ShuffleStatus(numMaps, numReduces)).isDefined) {
        throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
      }
    } else {
      if (shuffleStatuses.put(shuffleId, new ShuffleStatus(numMaps)).isDefined) {
        throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
      }
    }
  }

  // 更新指定 shuffle 中某个 map 输出的位置（例如退役迁移场景）
  def updateMapOutput(shuffleId: Int, mapId: Long, bmAddress: BlockManagerId): Unit = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.updateMapOutput(mapId, bmAddress)
      case None if shuffleMigrationEnabled =>
        logWarning(log"Asked to update map output for unknown shuffle " +
          log"${MDC(SHUFFLE_ID, shuffleId)}")
      case None =>
        logError(log"Asked to update map output for unknown shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
    }
  }

  // 获取指定 shuffle 的状态，不存在则抛出异常
  private def getShuffleStatusOrError(shuffleId: Int, caller: String): ShuffleStatus = {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) => shuffleStatus
      case None => throw new ShuffleStatusNotFoundException(shuffleId, caller)
    }
  }

  // 注册单个 map 输出
  def registerMapOutput(shuffleId: Int, mapIndex: Int, status: MapStatus): Boolean = {
    getShuffleStatusOrError(shuffleId, "registerMapOutput").addMapOutput(mapIndex, status)
  }

  // 注销指定 shuffle 的 map 输出并递增 epoch
  def unregisterMapOutput(shuffleId: Int, mapIndex: Int, bmAddress: BlockManagerId): Unit = {
    getShuffleStatusOrError(shuffleId, "unregisterMapOutput").removeMapOutput(mapIndex, bmAddress)
    incrementEpoch()
  }

  // 注销指定 shuffle 的所有 map 和 merge 输出，并清除 merger 位置信息
  def unregisterAllMapAndMergeOutput(shuffleId: Int): Unit = {
    val shuffleStatus = getShuffleStatusOrError(shuffleId, "unregisterAllMapAndMergeOutput")
    shuffleStatus.removeOutputsByFilter(x => true)
    shuffleStatus.removeMergeResultsByFilter(x => true)
    shuffleStatus.removeShuffleMergerLocations()
    incrementEpoch()
  }

  // 注册单个 merge 结果
  def registerMergeResult(shuffleId: Int, reduceId: Int, status: MergeStatus): Unit = {
    getShuffleStatusOrError(shuffleId, "registerMergeResult").addMergeResult(reduceId, status)
  }

  // 批量注册 merge 结果
  def registerMergeResults(shuffleId: Int, statuses: Seq[(Int, MergeStatus)]): Unit = {
    statuses.foreach {
      case (reduceId, status) => registerMergeResult(shuffleId, reduceId, status)
    }
  }

  // 注册 shuffle push merger 的位置信息
  def registerShufflePushMergerLocations(
      shuffleId: Int,
      shuffleMergers: Seq[BlockManagerId]): Unit = {
    getShuffleStatusOrError(shuffleId, "registerShufflePushMergerLocations")
      .registerShuffleMergerLocations(shuffleMergers)
  }

  /**
   * 注销指定 reduceId 的 merge 结果。若指定了 mapIndex，仅在该 mapIndex
   * 属于合并结果时才注销。
   */
  def unregisterMergeResult(
      shuffleId: Int,
      reduceId: Int,
      bmAddress: BlockManagerId,
      mapIndex: Option[Int] = None): Unit = {
    val shuffleStatus = getShuffleStatusOrError(shuffleId, "unregisterMergeResult")
    val mergeStatus = shuffleStatus.mergeStatuses(reduceId)
    if (mergeStatus != null &&
      (mapIndex.isEmpty || mergeStatus.tracker.contains(mapIndex.get))) {
      shuffleStatus.removeMergeResult(reduceId, bmAddress)
      incrementEpoch()
    }
  }

  // 注销指定 shuffle 的所有 merge 结果
  def unregisterAllMergeResult(shuffleId: Int): Unit = {
    getShuffleStatusOrError(shuffleId, "unregisterAllMergeResult")
      .removeMergeResultsByFilter(x => true)
    incrementEpoch()
  }

  // 注销 shuffle，清除其全部序列化缓存
  def unregisterShuffle(shuffleId: Int): Unit = {
    shuffleStatuses.remove(shuffleId).foreach { shuffleStatus =>
      shuffleStatus.invalidateSerializedMapOutputStatusCache()
      shuffleStatus.invalidateSerializedMergeOutputStatusCache()
    }
  }

  // 移除指定 host 上的所有 shuffle 输出，并递增 epoch
  def removeOutputsOnHost(host: String): Unit = {
    shuffleStatuses.valuesIterator.foreach { _.removeOutputsOnHost(host) }
    incrementEpoch()
  }

  // 移除指定 Executor 上的所有 shuffle 输出，并递增 epoch
  def removeOutputsOnExecutor(execId: String): Unit = {
    shuffleStatuses.valuesIterator.foreach { _.removeOutputsOnExecutor(execId) }
    incrementEpoch()
  }

  // 检查指定 shuffle 是否正在被追踪
  def containsShuffle(shuffleId: Int): Boolean = shuffleStatuses.contains(shuffleId)

  // 获取指定 shuffle 的可用 map 输出数量
  def getNumAvailableOutputs(shuffleId: Int): Int = {
    shuffleStatuses.get(shuffleId).map(_.numAvailableMapOutputs).getOrElse(0)
  }

  /** VisibleForTest. Invoked in test only. */
  private[spark] def getNumAvailableMergeResults(shuffleId: Int): Int = {
    shuffleStatuses.get(shuffleId).map(_.numAvailableMergeResults).getOrElse(0)
  }

  // 返回尚未完成的分区 id 序列（需要计算的分区），shuffle 不存在时返回 None
  def findMissingPartitions(shuffleId: Int): Option[Seq[Int]] = {
    shuffleStatuses.get(shuffleId).map(_.findMissingPartitions())
  }

  // Range 的分组版本，避免 IterableLike 的 grouped 遍历所有元素
  def rangeGrouped(range: Range, size: Int): Seq[Range] = {
    val start = range.start
    val step = range.step
    val end = range.end
    for (i <- start.until(end, size * step)) yield {
      i.until(i + size * step, step)
    }
  }

  // 将 n 个元素尽可能均匀地分为 m 个桶，余数部分均摊到前几个桶
  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Seq[Int]] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    if (elementsPerBucket == 0) {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1)
    } else {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1) ++
        rangeGrouped(splitPoint.until(numElements), elementsPerBucket)
    }
  }

  /**
   * 统计指定 shuffle 所有输出的每个 reduce 分区的数据量大小。
   * 当 map 数量 × reduce 数量超过并行聚合阈值时，使用多线程并行统计。
   */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    getShuffleStatusOrError(dep.shuffleId, "getStatistics").withMapStatuses { statuses =>
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      val parallelAggThreshold = conf.get(
        SHUFFLE_MAP_OUTPUT_PARALLEL_AGGREGATION_THRESHOLD)
      val parallelism = math.min(
        availableProcessors,
        statuses.length.toLong * totalSizes.length / parallelAggThreshold + 1).toInt
      // 单线程统计
      if (parallelism <= 1) {
        statuses.filter(_ != null).foreach { s =>
          for (i <- totalSizes.indices) {
            totalSizes(i) += s.getSizeForBlock(i)
          }
        }
      } else {
        // 多线程并行统计：将 reduce 分区均分到各线程
        val threadPool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "map-output-aggregate")
        try {
          implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
          val mapStatusSubmitTasks = equallyDivide(totalSizes.length, parallelism).map {
            reduceIds => Future {
              statuses.filter(_ != null).foreach { s =>
                reduceIds.foreach(i => totalSizes(i) += s.getSizeForBlock(i))
              }
            }
          }
          ThreadUtils.awaitResult(Future.sequence(mapStatusSubmitTasks), Duration.Inf)
        } finally {
          threadPool.shutdown()
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
  }

  /**
   * 返回给定 shuffle 中指定 reduce 分区的首选运行位置（数据量最大的节点）。
   * 若 Push-Based Shuffle 已启用且合并比例超过阈值，优先返回合并块所在节点。
   * 否则根据各 map 输出在 reducer 分区的数据分布计算首选位置。
   */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
      : Seq[String] = {
    val shuffleStatus = shuffleStatuses.get(dep.shuffleId).orNull
    if (shuffleStatus != null) {
      // 检查 push-based 合并分区是否可用且合并比例达标
      val preferredLoc = if (pushBasedShuffleEnabled) {
        shuffleStatus.withMergeStatuses { statuses =>
          val status = statuses(partitionId)
          val numMaps = dep.rdd.partitions.length
          if (status != null && status.getNumMissingMapOutputs(numMaps).toDouble / numMaps
            <= (1 - REDUCER_PREF_LOCS_FRACTION)) {
            Seq(status.location.host)
          } else {
            Nil
          }
        }
      } else {
        Nil
      }
      if (preferredLoc.nonEmpty) {
        preferredLoc
      } else {
        // 在任务数未超过阈值时，按 map 输出大小计算数据本地性首选位置
        if (shuffleLocalityEnabled && dep.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD &&
          dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
          val blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
            dep.partitioner.numPartitions, REDUCER_PREF_LOCS_FRACTION)
          if (blockManagerIds.nonEmpty) {
            blockManagerIds.get.map(_.host).distinct.toImmutableArraySeq
          } else {
            Nil
          }
        } else {
          Nil
        }
      }
    } else {
      Nil
    }
  }

  /**
   * 返回指定 reducer 分区中数据量占比超过阈值 fractionThreshold 的位置列表。
   * 用于判断哪些节点持有足够多的 map 输出数据，适合作为 reduce 任务的首选位置。
   */
  def getLocationsWithLargestOutputs(
      shuffleId: Int,
      reducerId: Int,
      numReducers: Int,
      fractionThreshold: Double)
    : Option[Array[BlockManagerId]] = {

    val shuffleStatus = shuffleStatuses.get(shuffleId).orNull
    if (shuffleStatus != null) {
      shuffleStatus.withMapStatuses { statuses =>
        if (statuses.nonEmpty) {
          // 按 BlockManagerId 累加各 map 在该 reducer 分区的输出大小
          val locs = new HashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          var mapIdx = 0
          while (mapIdx < statuses.length) {
            val status = statuses(mapIdx)
            // status 可能为 null（registerShuffle 后 registerMapOutputs 之前）
            if (status != null) {
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) {
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                totalOutputSize += blockSize
              }
            }
            mapIdx = mapIdx + 1
          }
          // 过滤出数据量占比达到阈值的位置
          val topLocs = locs.filter { case (loc, size) =>
            size.toDouble / totalOutputSize >= fractionThreshold
          }
          // 若存在满足条件的位置则返回
          if (topLocs.nonEmpty) {
            return Some(topLocs.keys.toArray)
          }
        }
      }
    }
    None
  }

  /**
   * 返回指定范围内 Mapper 的运行位置（host + executorId），用于数据本地性调度。
   */
  def getMapLocation(
      dep: ShuffleDependency[_, _, _],
      startMapIndex: Int,
      endMapIndex: Int): Seq[String] =
  {
    if (!shuffleLocalityEnabled) return Nil

    val shuffleStatus = shuffleStatuses.get(dep.shuffleId).orNull
    if (shuffleStatus != null) {
      shuffleStatus.withMapStatuses { statuses =>
        if (startMapIndex < endMapIndex &&
          (startMapIndex >= 0 && endMapIndex <= statuses.length)) {
          val statusesPicked = statuses.slice(startMapIndex, endMapIndex).filter(_ != null)
          statusesPicked.map(_.location.host).distinct.toImmutableArraySeq
        } else {
          Nil
        }
      }
    } else {
      Nil
    }
  }

  // 根据 (shuffleId, mapId) 获取 map 输出的 BlockManagerId 位置
  def getMapOutputLocation(shuffleId: Int, mapId: Long): Option[BlockManagerId] = {
    shuffleStatuses.get(shuffleId).flatMap { shuffleStatus =>
      shuffleStatus.getMapStatus(mapId).map(_.location)
    }
  }

  // 递增 epoch 版本号（每次 map 输出丢失时调用，通知 Executor 清除缓存）
  def incrementEpoch(): Unit = {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  // 获取当前 epoch 值
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  // 仅在 local 模式调用
  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    val mapSizesByExecutorId = getPushBasedShuffleMapSizesByExecutorId(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    assert(mapSizesByExecutorId.enableBatchFetch == true)
    mapSizesByExecutorId.iter
  }

  // 仅在 local 模式调用
  override def getPushBasedShuffleMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): MapSizesByExecutorId = {
    logDebug(s"Fetching outputs for shuffle $shuffleId")
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.withMapStatuses { statuses =>
          val actualEndMapIndex = if (endMapIndex == Int.MaxValue) statuses.length else endMapIndex
          logDebug(s"Convert map statuses for shuffle $shuffleId, " +
            s"mappers $startMapIndex-$actualEndMapIndex, partitions $startPartition-$endPartition")
          MapOutputTracker.convertMapStatuses(
            shuffleId, startPartition, endPartition, statuses, startMapIndex, actualEndMapIndex)
        }
      case None =>
        MapSizesByExecutorId(Iterator.empty, true)
    }
  }

  // 仅在 local 模式调用；local 模式不启用 push-based shuffle，返回空列表
  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    Seq.empty.iterator
  }

  // 仅在 local 模式调用；local 模式不启用 push-based shuffle，返回空列表
  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int,
      chunkTracker: RoaringBitmap): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    Seq.empty.iterator
  }

  // 仅在 local 模式调用
  override def getShufflePushMergerLocations(shuffleId: Int): Seq[BlockManagerId] = {
    shuffleStatuses.get(shuffleId).map(_.getShufflePushMergerLocations).getOrElse(Seq.empty)
  }

  // 停止 MapOutputTrackerMaster：发送毒丸、关闭线程池、通知 RPC 端点停止
  override def stop(): Unit = {
    mapOutputTrackerMasterMessages.offer(PoisonPill)
    threadpool.shutdown()
    try {
      sendTracker(StopMapOutputTracker)
    } catch {
      case e: SparkException =>
        logError("Could not tell tracker we are stopping.", e)
    }
    trackerEndpoint = null
    shuffleStatuses.clear()
  }
}

// shuffle 状态未找到异常
case class ShuffleStatusNotFoundException(shuffleId: Int, methodName: String)
  extends SparkException(s"$methodName called for nonexistent shuffle ID $shuffleId.")

/**
 * Executor 端的 MapOutputTracker 实现，负责从 Driver 的 MapOutputTrackerMaster 拉取 map 输出信息。
 * 注意：local 模式不使用此类，而是直接访问 MapOutputTrackerMaster。
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {

  // Executor 端缓存的 map 输出状态（按 shuffleId 索引）
  val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]().asScala

  // Executor 端缓存的 merge 输出状态（按 shuffleId 索引）
  val mergeStatuses: Map[Int, Array[MergeStatus]] =
    new ConcurrentHashMap[Int, Array[MergeStatus]]().asScala

  // 延迟初始化以确保首次任务运行时才实例化，避免启动时用户库未下载导致失败（SPARK-36705）
  private lazy val fetchMergeResult = Utils.isPushBasedShuffleEnabled(conf, isDriver = false)

  // 追踪最新 shuffle 执行的 push merger 位置信息
  val shufflePushMergerLocations = new ConcurrentHashMap[Int, Seq[BlockManagerId]]().asScala

  // 按 shuffleId 加锁，确保同一时刻只有一个线程拉取同一 shuffle 的块信息
  private val fetchingLock = new KeyLock[Int]

  override def getMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    val mapSizesByExecutorId = getMapSizesByExecutorIdImpl(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, useMergeResult = false)
    assert(mapSizesByExecutorId.enableBatchFetch == true)
    mapSizesByExecutorId.iter
  }

  override def getPushBasedShuffleMapSizesByExecutorId(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int): MapSizesByExecutorId = {
    getMapSizesByExecutorIdImpl(
      shuffleId, startMapIndex, endMapIndex, startPartition, endPartition, useMergeResult = true)
  }

  // 核心实现：获取 map/merge 输出状态并转换为按 Executor 分组的块信息
  private def getMapSizesByExecutorIdImpl(
      shuffleId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      useMergeResult: Boolean): MapSizesByExecutorId = {
    logDebug(s"Fetching outputs for shuffle $shuffleId")
    val (mapOutputStatuses, mergedOutputStatuses) = getStatuses(shuffleId, conf,
      // Stage 重试时 shuffleDependency.isShuffleMergeFinalizedMarked 为 false，
      // 此时不需要拉取 mergeStatus（SPARK-37023）
      if (useMergeResult) fetchMergeResult else false)
    try {
      val actualEndMapIndex =
        if (endMapIndex == Int.MaxValue) mapOutputStatuses.length else endMapIndex
      logDebug(s"Convert map statuses for shuffle $shuffleId, " +
        s"mappers $startMapIndex-$actualEndMapIndex, partitions $startPartition-$endPartition")
      MapOutputTracker.convertMapStatuses(
        shuffleId, startPartition, endPartition, mapOutputStatuses, startMapIndex,
          actualEndMapIndex, Option(mergedOutputStatuses))
    } catch {
      // 拉取失败说明缓存过期，清除后由上层重试
      case e: MetadataFetchFailedException =>
        mapStatuses.clear()
        mergeStatuses.clear()
        throw e
    }
  }

  // 整个合并分区拉取失败时，回退获取被合并到该分区中的各原始块信息
  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    logDebug(s"Fetching backup outputs for shuffle $shuffleId, partition $partitionId")
    // 重新拉取状态，因为可能已被同一 Executor 上的其他任务清除
    val (mapOutputStatuses, mergeResultStatuses) = getStatuses(shuffleId, conf, fetchMergeResult)
    try {
      val mergeStatus = mergeResultStatuses(partitionId)
      // 原始 MergeStatus 已不可用时无法识别未合并块列表，抛出 MetadataFetchFailedException
      MapOutputTracker.validateStatus(mergeStatus, shuffleId, partitionId)
      // 使用分区级别的 bitmap 获取属于该合并分区的原始 map 块
      MapOutputTracker.getMapStatusesForMergeStatus(shuffleId, partitionId,
        mapOutputStatuses, mergeStatus.tracker)
    } catch {
      case e: MetadataFetchFailedException =>
        mapStatuses.clear()
        mergeStatuses.clear()
        throw e
    }
  }

  // 合并分区的某个 chunk 拉取失败时，回退获取属于该 chunk 的各原始块信息
  override def getMapSizesForMergeResult(
      shuffleId: Int,
      partitionId: Int,
      chunkTracker: RoaringBitmap
    ): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    logDebug(s"Fetching backup outputs for shuffle $shuffleId, partition $partitionId")
    // Fetch the map statuses and merge statuses again since they might have already been
    // cleared by another task running in the same executor.
    val (mapOutputStatuses, _) = getStatuses(shuffleId, conf, fetchMergeResult)
    try {
      MapOutputTracker.getMapStatusesForMergeStatus(shuffleId, partitionId, mapOutputStatuses,
        chunkTracker)
    } catch {
      // We experienced a fetch failure so our mapStatuses cache is outdated; clear it:
      case e: MetadataFetchFailedException =>
        mapStatuses.clear()
        mergeStatuses.clear()
        throw e
    }
  }

  // 获取 shuffle push merger 位置：优先使用缓存，缓存未命中时从 Driver 拉取
  override def getShufflePushMergerLocations(shuffleId: Int): Seq[BlockManagerId] = {
    shufflePushMergerLocations.getOrElse(shuffleId, getMergerLocations(shuffleId))
  }

  // 从 Driver 拉取 merger 位置并缓存
  private def getMergerLocations(shuffleId: Int): Seq[BlockManagerId] = {
    fetchingLock.withLock(shuffleId) {
      var fetchedMergers = shufflePushMergerLocations.get(shuffleId).orNull
      if (null == fetchedMergers) {
        fetchedMergers =
          askTracker[Seq[BlockManagerId]](GetShufflePushMergerLocations(shuffleId))
        if (fetchedMergers.nonEmpty) {
          shufflePushMergerLocations(shuffleId) = fetchedMergers
        } else {
          fetchedMergers = Seq.empty[BlockManagerId]
        }
      }
      fetchedMergers
    }
  }

  /**
   * 获取或从 Driver 拉取指定 shuffle 的 MapStatus 和 MergeStatus 数组。
   * 使用 fetchingLock 确保同一 shuffle 只有一个线程执行拉取，其余线程等待缓存填充。
   */
  private def getStatuses(
      shuffleId: Int,
      conf: SparkConf,
      canFetchMergeResult: Boolean): (Array[MapStatus], Array[MergeStatus]) = {
    if (canFetchMergeResult) {
      val mapOutputStatuses = mapStatuses.get(shuffleId).orNull
      val mergeOutputStatuses = mergeStatuses.get(shuffleId).orNull

      if (mapOutputStatuses == null || mergeOutputStatuses == null) {
        logInfo(log"Don't have map/merge outputs for" +
          log" shuffle ${MDC(SHUFFLE_ID, shuffleId)}, fetching them")
        val startTimeNs = System.nanoTime()
        fetchingLock.withLock(shuffleId) {
          var fetchedMapStatuses = mapStatuses.get(shuffleId).orNull
          var fetchedMergeStatuses = mergeStatuses.get(shuffleId).orNull
          if (fetchedMapStatuses == null || fetchedMergeStatuses == null) {
            logInfo(log"Doing the fetch; tracker endpoint = " +
              log"${MDC(RPC_ENDPOINT_REF, trackerEndpoint)}")
            val fetchedBytes =
              askTracker[(Array[Byte], Array[Byte])](GetMapAndMergeResultStatuses(shuffleId))
            try {
              fetchedMapStatuses =
                MapOutputTracker.deserializeOutputStatuses[MapStatus](fetchedBytes._1, conf)
              fetchedMergeStatuses =
                MapOutputTracker.deserializeOutputStatuses[MergeStatus](fetchedBytes._2, conf)
            } catch {
              case e: SparkException =>
                throw new MetadataFetchFailedException(shuffleId, -1,
                  s"Unable to deserialize broadcasted map/merge statuses" +
                    s" for shuffle $shuffleId: " + e.getCause)
            }
            logInfo("Got the map/merge output locations")
            mapStatuses.put(shuffleId, fetchedMapStatuses)
            mergeStatuses.put(shuffleId, fetchedMergeStatuses)
          }
          logDebug(s"Fetching map/merge output statuses for shuffle $shuffleId took " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          (fetchedMapStatuses, fetchedMergeStatuses)
        }
      } else {
        (mapOutputStatuses, mergeOutputStatuses)
      }
    } else {
      val statuses = mapStatuses.get(shuffleId).orNull
      if (statuses == null) {
        logInfo(log"Don't have map outputs for shuffle ${MDC(SHUFFLE_ID, shuffleId)}," +
          log" fetching them")
        val startTimeNs = System.nanoTime()
        fetchingLock.withLock(shuffleId) {
          var fetchedStatuses = mapStatuses.get(shuffleId).orNull
          if (fetchedStatuses == null) {
            logInfo(log"Doing the fetch; tracker endpoint =" +
              log" ${MDC(RPC_ENDPOINT_REF, trackerEndpoint)}")
            val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
            try {
              fetchedStatuses =
                MapOutputTracker.deserializeOutputStatuses[MapStatus](fetchedBytes, conf)
            } catch {
              case e: SparkException =>
                throw new MetadataFetchFailedException(shuffleId, -1,
                  s"Unable to deserialize broadcasted map statuses for shuffle $shuffleId: " +
                    e.getCause)
            }
            logInfo("Got the map output locations")
            mapStatuses.put(shuffleId, fetchedStatuses)
          }
          logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          (fetchedStatuses, null)
        }
      } else {
        (statuses, null)
      }
    }
  }

  // 注销 shuffle 数据，清除本地缓存
  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
    mergeStatuses.remove(shuffleId)
    shufflePushMergerLocations.remove(shuffleId)
  }

  /**
   * 由 Executor 调用以更新 epoch 版本号。若新 epoch 大于当前值，
   * 清除所有本地缓存（map 状态、merge 状态、merger 位置），迫使后续任务重新从 Driver 拉取。
   */
  def updateEpoch(newEpoch: Long): Unit = {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo(log"Updating epoch to ${MDC(EPOCH, newEpoch)} and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
        mergeStatuses.clear()
        shufflePushMergerLocations.clear()
      }
    }
  }
}

// MapOutputTracker 伴生对象：提供序列化/反序列化和状态转换的工具方法
private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  // 序列化标记：DIRECT 表示直接传输，BROADCAST 表示通过广播变量传输
  private val DIRECT = 0
  private val BROADCAST = 1

  // Push-Based Shuffle 中合并块的特殊 map ID 标识
  val SHUFFLE_PUSH_MAP_ID = -1

  /**
   * 将 map/merge 输出状态数组序列化为高效的压缩字节格式（使用 Zstd 压缩）。
   * 若序列化结果超过 minBroadcastSize 阈值，将通过广播变量发送以避免超出 RPC 消息大小限制。
   */
  def serializeOutputStatuses[T <: ShuffleOutputStatus](
      statuses: Array[T],
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): (Array[Byte], Broadcast[Array[Array[Byte]]]) = {
    // ByteArrayOutputStream 有 2GB 限制，使用 ChunkedByteBufferOutputStream 代替
    val out = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
    out.write(DIRECT)
    val codec = CompressionCodec.createCodec(conf, conf.get(MAP_STATUS_COMPRESSION_CODEC))
    val objOut = new ObjectOutputStream(codec.compressedOutputStream(out))
    Utils.tryWithSafeFinally {
      // statuses 可能被并行修改，需要同步
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val chunkedByteBuf = out.toChunkedByteBuffer
    val arrSize = out.size
    // 超过阈值时改用广播变量发送
    if (arrSize >= minBroadcastSize) {
      // arr(0) 是 DIRECT 标记，反序列化时需要跳过
      val arr = chunkedByteBuf.getChunks().map(_.array())
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // 使用 Apache ByteArrayOutputStream 避免内存重新分配和拷贝
      val out = new ApacheByteArrayOutputStream()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(codec.compressedOutputStream(out))
      Utils.tryWithSafeFinally {
        oos.writeObject(bcast)
      } {
        oos.close()
      }
      val outArr = out.toByteArray
      logInfo(log"Broadcast outputstatuses size = " +
        log"${MDC(BROADCAST_OUTPUT_STATUS_SIZE, outArr.length)}," +
        log" actual size = ${MDC(BROADCAST_OUTPUT_STATUS_SIZE, arrSize)}")
      (outArr, bcast)
    } else {
      (chunkedByteBuf.toArray, null)
    }
  }

  // 反序列化输出状态：serializeOutputStatuses 的逆操作
  def deserializeOutputStatuses[T <: ShuffleOutputStatus](
      bytes: Array[Byte], conf: SparkConf): Array[T] = {
    assert (bytes.length > 0)

    // 内部辅助：创建压缩解码流并反序列化对象
    def deserializeObject(in: InputStream): AnyRef = {
      val codec = CompressionCodec.createCodec(conf, conf.get(MAP_STATUS_COMPRESSION_CODEC))
      // ZStd 编解码器包装在 BufferedInputStream 中，避免小数据量时过多 JNI 调用的开销
      val objIn = new ObjectInputStream(codec.compressedInputStream(in))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    // 跳过第一个字节（标记位），根据标记位选择直接反序列化或从广播变量反序列化
    val in = new ByteArrayInputStream(bytes, 1, bytes.length - 1)
    bytes(0) match {
      case DIRECT =>
        deserializeObject(in).asInstanceOf[Array[T]]
      case BROADCAST =>
        try {
          // 反序列化广播变量，取出 .value 数组后再反序列化其中的实际数据
          val bcast = deserializeObject(in).asInstanceOf[Broadcast[Array[Array[Byte]]]]
          val actualSize = bcast.value.foldLeft(0L)(_ + _.length)
          logInfo(log"Broadcast outputstatuses size =" +
            log" ${MDC(BROADCAST_OUTPUT_STATUS_SIZE, bytes.length)}" +
            log", actual size = ${MDC(BROADCAST_OUTPUT_STATUS_SIZE, actualSize)}")
          val bcastIn = new ChunkedByteBuffer(bcast.value.map(ByteBuffer.wrap)).toInputStream()
          // 跳过 DIRECT 标记位，从偏移量 1 开始反序列化
          bcastIn.skip(1)
          deserializeObject(bcastIn).asInstanceOf[Array[T]]
        } catch {
          case e: IOException =>
            logWarning("Exception encountered during deserializing broadcasted" +
              " output statuses: ", e)
            throw new SparkException("Unable to deserialize broadcasted" +
              " output statuses", e)
        }
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * 将 map 状态数组转换为按 BlockManagerId 分组的 shuffle 块信息。
   * 若 Push-Based Shuffle 启用且有 MergeStatus，优先使用合并块位置，
   * 对于未被合并的"空洞"部分则回退到原始 map 输出块。
   * 若任何状态为 null（mapper 失败），抛出 FetchFailedException。
   */
  def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      mapStatuses: Array[MapStatus],
      startMapIndex : Int,
      endMapIndex: Int,
      mergeStatusesOpt: Option[Array[MergeStatus]] = None): MapSizesByExecutorId = {
    assert (mapStatuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ListBuffer[(BlockId, Long, Int)]]
    var enableBatchFetch = true
    // 仅当 reduce 任务需要拉取所有 map 输出时才使用 MergeStatus，
    // 因为合并分区中的块是随机顺序合并的，无法服务子范围请求。
    // 子范围请求通常表明存在倾斜分区，交由 AQE 处理。
    if (mergeStatusesOpt.exists(_.exists(_ != null)) && startMapIndex == 0
      && endMapIndex == mapStatuses.length) {
      // Push-Based Shuffle 启用时禁用批量拉取
      enableBatchFetch = false
      logDebug(s"Disable shuffle batch fetch as Push based shuffle is enabled for $shuffleId.")
      val mergeStatuses = mergeStatusesOpt.get
      for (partId <- startPartition until endPartition) {
        val mergeStatus = mergeStatuses(partId)
        if (mergeStatus != null && mergeStatus.totalSize > 0) {
          // 有可用的合并分区，使用 ShuffleMergedBlockId 标识这是一个合并后的块
          splitsByAddress.getOrElseUpdate(mergeStatus.location, ListBuffer()) +=
            ((ShuffleMergedBlockId(shuffleId, mergeStatus.shuffleMergeId, partId),
              mergeStatus.totalSize, SHUFFLE_PUSH_MAP_ID))
        }
      }

      // 对于合并分区中的"空洞"（未被合并的 mapper 输出），回退到拉取原始 map 块
      for ((mapStatus, mapIndex) <- mapStatuses.iterator.zipWithIndex) {
        validateStatus(mapStatus, shuffleId, startPartition)
        for (partId <- startPartition until endPartition) {
          // 对于未被合并（空洞）的 mapper shuffle 分区块，拉取原始 map 输出
          val mergeStatus = mergeStatuses(partId)
          if (mergeStatus == null || mergeStatus.totalSize == 0 ||
            !mergeStatus.tracker.contains(mapIndex)) {
            val size = mapStatus.getSizeForBlock(partId)
            if (size != 0) {
              splitsByAddress.getOrElseUpdate(mapStatus.location, ListBuffer()) +=
                ((ShuffleBlockId(shuffleId, mapStatus.mapId, partId), size, mapIndex))
            }
          }
        }
      }
    } else {
      val iter = mapStatuses.iterator.zipWithIndex
      for ((status, mapIndex) <- iter.slice(startMapIndex, endMapIndex)) {
        validateStatus(status, shuffleId, startPartition)
        for (part <- startPartition until endPartition) {
          val size = status.getSizeForBlock(part)
          if (size != 0) {
            splitsByAddress.getOrElseUpdate(status.location, ListBuffer()) +=
              ((ShuffleBlockId(shuffleId, status.mapId, part), size, mapIndex))
          }
        }
      }
    }

    MapSizesByExecutorId(splitsByAddress.iterator, enableBatchFetch)
  }

  /**
   * 给定 shuffle ID、分区 ID、map 状态数组和 bitmap，识别属于该合并分区或
   * 合并分区 chunk 的各原始 shuffle 块的元数据（位置、大小、mapIndex）。
   * 用于合并块拉取失败后回退到拉取原始块。
   */
  def getMapStatusesForMergeStatus(
      shuffleId: Int,
      partitionId: Int,
      mapStatuses: Array[MapStatus],
      tracker: RoaringBitmap): Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] = {
    assert (mapStatuses != null && tracker != null)
    val splitsByAddress = new HashMap[BlockManagerId, ListBuffer[(BlockId, Long, Int)]]
    for ((status, mapIndex) <- mapStatuses.zipWithIndex) {
      // 只添加属于该合并块（bitmap 包含的 mapIndex）的原始块
      if (tracker.contains(mapIndex)) {
        MapOutputTracker.validateStatus(status, shuffleId, partitionId)
        splitsByAddress.getOrElseUpdate(status.location, ListBuffer()) +=
          ((ShuffleBlockId(shuffleId, status.mapId, partitionId),
            status.getSizeForBlock(partitionId), mapIndex))
      }
    }
    splitsByAddress.iterator
  }

  // 校验 ShuffleOutputStatus 是否为 null，为 null 则表示对应分区的输出丢失，抛出异常
  def validateStatus(status: ShuffleOutputStatus, shuffleId: Int, partition: Int) : Unit = {
    if (status == null) {
      // scalastyle:off line.size.limit
      val errorMessage = log"Missing an output location for shuffle ${MDC(SHUFFLE_ID, shuffleId)} partition ${MDC(PARTITION_ID, partition)}"
      // scalastyle:on
      logError(errorMessage)
      throw new MetadataFetchFailedException(shuffleId, partition, errorMessage.message)
    }
  }
}

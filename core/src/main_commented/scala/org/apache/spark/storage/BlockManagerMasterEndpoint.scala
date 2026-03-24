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

import java.io.IOException
import java.util.{HashMap => JHashMap}
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, TimeoutException}
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{MapOutputTrackerMaster, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.RDD_CACHE_VISIBILITY_TRACKING_ENABLED
import org.apache.spark.network.shuffle.{ExternalBlockStoreClient, RemoteBlockPushResolver}
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedClusterMessages, CoarseGrainedSchedulerBackend}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * 文件说明：Driver端的块管理器主端点，负责跟踪整个集群中所有Executor的块管理器状态，
 * 维护块位置元数据，处理块注册、删除、定位等核心存储管理操作，是Spark存储层的核心协调组件。
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    externalBlockStoreClient: Option[ExternalBlockStoreClient],
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo],
    mapOutputTracker: MapOutputTrackerMaster,
    private val _shuffleManager: ShuffleManager,
    isDriver: Boolean)
  extends IsolatedThreadSafeRpcEndpoint with Logging {

  // ShuffleManager延迟初始化，允许用户jar自定义ShuffleManager，除测试外初始为null，后续从SparkEnv获取
  private lazy val shuffleManager = Option(_shuffleManager).getOrElse(SparkEnv.get.shuffleManager)

  // Executor ID到本地磁盘目录的映射缓存
  private val executorIdToLocalDirs =
    CacheBuilder
      .newBuilder()
      .maximumSize(conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE))
      .build[String, Array[String]]()

  // 外部Shuffle服务对应的块管理器ID到块状态的映射
  private val blockStatusByShuffleService =
    new mutable.HashMap[BlockManagerId, BlockStatusPerBlockId]

  // Executor ID到对应块管理器ID的映射
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // 正在退役中的块管理器集合
  private val decommissioningBlockManagerSet = new mutable.HashSet[BlockManagerId]

  // 块ID到持有该块的所有块管理器集合的映射
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  // Task ID到该任务生成的所有RDD块集合的映射
  private val tidToRddBlockIds = new mutable.HashMap[Long, mutable.HashSet[RDDBlockId]]
  // 记录尚未可见的RDD块，至少一个生成任务成功完成后才会移除并标记为可见
  private val invisibleRDDBlocks = new mutable.HashSet[RDDBlockId]

  // 主机名到Shuffle合并服务位置的映射，保留最近使用的合并位置，超过大小限制时移除最早的
  private val shuffleMergerLocations = new mutable.LinkedHashMap[String, BlockManagerId]()

  // 保留的合并位置最大缓存数量
  private val maxRetainedMergerLocations = conf.get(config.SHUFFLE_MERGER_MAX_RETAINED_LOCATIONS)

  // 异步请求线程池
  private val askThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool", 100)
  private implicit val askExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(askThreadPool)

  // 拓扑映射器初始化，用于获取节点拓扑信息，支持用户自定义实现
  private val topologyMapper = {
    val topologyMapperClassName = conf.get(
      config.STORAGE_REPLICATION_TOPOLOGY_MAPPER)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(log"Using ${MDC(CLASS_NAME, topologyMapperClassName)} for getting topology information")
    mapper
  }

  // 是否启用主动复制：当某个节点宕机后主动复制现有RDD块到其他节点
  val proactivelyReplicate = conf.get(config.STORAGE_REPLICATION_PROACTIVE)

  // RPC请求默认超时时间
  val defaultRpcTimeout = RpcUtils.askRpcTimeout(conf)

  // 是否启用基于推送的Shuffle
  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(conf, isDriver)

  logInfo("BlockManagerMasterEndpoint up")

  // 是否允许通过外部Shuffle服务删除Shuffle数据
  private val externalShuffleServiceRemoveShuffleEnabled: Boolean =
    externalBlockStoreClient.isDefined && conf.get(config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED)
  // 是否允许从外部Shuffle服务获取RDD块
  private val externalShuffleServiceRddFetchEnabled: Boolean =
    externalBlockStoreClient.isDefined && conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
  // 外部Shuffle服务端口
  private val externalShuffleServicePort: Int = StorageUtils.externalShuffleServicePort(conf)

  // Driver端CoarseGrainedScheduler后端端点引用
  private lazy val driverEndpoint =
    RpcUtils.makeDriverRef(CoarseGrainedSchedulerBackend.ENDPOINT_NAME, conf, rpcEnv)

  // 是否开启RDD缓存可见性追踪
  private val trackingCacheVisibility: Boolean = conf.get(RDD_CACHE_VISIBILITY_TRACKING_ENABLED)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(
      id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint, isReRegister) =>
      context.reply(
        register(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, endpoint, isReRegister))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>

      @inline def handleResult(success: Boolean): Unit = {
        // SPARK-30590: 仅当更新成功时才发送块更新事件，失败说明后续还会更新
        if (success) {
          listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
        }
        context.reply(success)
      }

      if (blockId.isShuffle) {
        updateShuffleBlockInfo(blockId, blockManagerId).foreach(handleResult)
      } else {
        handleResult(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      }

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsAndStatus(blockId, requesterHost) =>
      context.reply(getLocationsAndStatus(blockId, requesterHost))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askStorageEndpoints) =>
      context.reply(blockStatus(blockId, askStorageEndpoints))

    case GetShufflePushMergerLocations(numMergersNeeded, hostsToFilter) =>
      context.reply(getShufflePushMergerLocations(numMergersNeeded, hostsToFilter))

    case RemoveShufflePushMergerLocation(host) =>
      context.reply(removeShufflePushMergerLocation(host))

    case IsExecutorAlive(executorId) =>
      context.reply(blockManagerIdByExecutor.contains(executorId))

    case GetMatchingBlockIds(filter, askStorageEndpoints) =>
      context.reply(getMatchingBlockIds(filter, askStorageEndpoints))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case DecommissionBlockManagers(executorIds) =>
      // 将对应块管理器标记为退役，不再用于块复制和块迁移，不需要额外通知，Executor本身会处理
      val bms = executorIds.flatMap(blockManagerIdByExecutor.get)
      logInfo(log"Mark BlockManagers (${MDC(BLOCK_MANAGER_IDS, bms.mkString(", "))}) as " +
        log"being decommissioning.")
      decommissioningBlockManagerSet ++= bms
      context.reply(true)

    case GetReplicateInfoForRDDBlocks(blockManagerId) =>
      context.reply(getReplicateInfoForRDDBlocks(blockManagerId))

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    case UpdateRDDBlockTaskInfo(blockId, taskId) =>
      // 报告任务刚计算缓存完RDD块，记录块和任务的对应关系，仅开启可见性追踪时生效
      context.reply(updateRDDBlockTaskInfo(blockId, taskId))

    case GetRDDBlockVisibility(blockId) =>
      // 查询指定RDD块的可见性状态
      context.reply(isRDDBlockVisible(blockId))

    case UpdateRDDBlockVisibility(taskId, visible) =>
      // 任务完成后由DAGScheduler报告，根据任务是否成功，更新该任务生成RDD块的可见性
      context.reply(updateRDDBlockVisibility(taskId, visible))
  }

  /**
   * 查询RDD块是否可见，开启可见性追踪时仅当至少一个生成任务成功完成后才可见
   */
  private def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    if (trackingCacheVisibility) {
      blockLocations.containsKey(blockId) &&
        blockLocations.get(blockId).nonEmpty && !invisibleRDDBlocks.contains(blockId)
    } else {
      // 功能关闭时所有块始终可见
      true
    }
  }

  /**
   * 根据任务完成结果更新对应RDD块可见性，任务成功则标记所有生成的块为可见
   */
  private def updateRDDBlockVisibility(taskId: Long, visible: Boolean): Unit = {
    if (!trackingCacheVisibility) {
      // 功能关闭直接返回
      return
    }

    // TODO: SPARK-42582 任务失败时应该要求BlockManager驱逐该块，避免不确定计算导致结果不一致
    if (visible) {
      tidToRddBlockIds.get(taskId).foreach { blockIds =>
        blockIds.foreach { blockId =>
          // 从不可见集合移除，标记为可见
          invisibleRDDBlocks.remove(blockId)
          // 通知所有持有该块的BlockManager更新可见性状态
          val msg = MarkRDDBlockAsVisible(blockId)
          getLocations(blockId).flatMap(blockManagerInfo.get).foreach { managerInfo =>
            managerInfo.storageEndpoint.ask[Unit](msg)
          }
        }
      }
    }

    tidToRddBlockIds.remove(taskId)
  }

  /**
   * 记录RDD块和生成它的任务的对应关系
   */
  private def updateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long): Unit = {
    if (!trackingCacheVisibility) {
      // 功能关闭直接返回
      return
    }
    tidToRddBlockIds.getOrElseUpdate(taskId, new mutable.HashSet[RDDBlockId])
      .add(blockId)
  }

  /**
   * 处理删除块过程中的异常，仅在活跃Executor超时才抛出异常，其他情况记录日志并返回默认值
   * @param blockType 块类型：RDD/shuffle/broadcast/block，用于日志
   * @param blockId 块ID，用于日志
   * @param bmId 目标块管理器ID
   * @param defaultValue 删除失败时返回的默认值
   * @tparam  默认值类型，Int或Boolean
   * @return 默认值或抛出异常
   */
  private def handleBlockRemovalFailure[T](
      blockType: String,
      blockId: String,
      bmId: BlockManagerId,
      defaultValue: T): PartialFunction[Throwable, T] = {
    case e: IOException =>
      if (!SparkContext.getActive.map(_.isStopped).getOrElse(true)) {
        logWarning(log"Error trying to remove ${MDC(BLOCK_TYPE, blockType)} " +
          log"${MDC(BLOCK_ID, blockId)}" +
          log" from block manager ${MDC(BLOCK_MANAGER_ID, bmId)}", e)
      }
      defaultValue

    case t: TimeoutException =>
      val executorId = bmId.executorId
      val isAlive = try {
        // 查询Driver确认Executor是否还存活
        driverEndpoint.askSync[Boolean](CoarseGrainedClusterMessages.IsExecutorAlive(executorId))
      } catch {
        // 查询失败，不影响整体流程，返回false使用默认值
        case NonFatal(e) =>
          logError(log"Cannot determine whether executor " +
            log"${MDC(EXECUTOR_ID, executorId)} is alive or not.", e)
          false
      }
      if (!isAlive) {
        // Executor已经死亡，忽略超时返回默认值
        logWarning(log"Error trying to remove ${MDC(BLOCK_TYPE, blockId)} " +
          log"${MDC(BLOCK_ID, blockId)}. " +
          log"The executor ${MDC(EXECUTOR_ID, executorId)} may have been lost.", t)
        defaultValue
      } else {
        // Executor存活但超时，抛出异常通知DriverExecutor不健康
        throw t
      }
  }

  /**
   * 删除指定RDD的所有块元数据，并异步通知所有BlockManager删除实际块数据
   * @param rddId RDD ID
   * @return 每个BlockManager删除的块数量序列
   */
  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // 构造删除RDD消息
    val removeMsg = RemoveRdd(rddId)

    // 找到该RDD所有块，从元数据中移除
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    // 需要通过外部Shuffle服务删除的块集合
    val blocksToDeleteByShuffleService =
      new mutable.HashMap[BlockManagerId, mutable.HashSet[RDDBlockId]]

    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.remove(blockId)
      if (trackingCacheVisibility) {
        invisibleRDDBlocks.remove(blockId)
      }
      // 区分普通Executor块和外部Shuffle服务块
      val (bmIdsExtShuffle, bmIdsExecutor) = bms.partition(_.port == externalShuffleServicePort)
      val liveExecutorsForBlock = bmIdsExecutor.map(_.executorId).toSet
      // 原始Executor已经释放的RDD磁盘块，通过外部Shuffle服务删除
      bmIdsExtShuffle.foreach { bmIdForShuffleService =>
        if (!liveExecutorsForBlock.contains(bmIdForShuffleService.executorId)) {
          val blockIdsToDel = blocksToDeleteByShuffleService.getOrElseUpdate(bmIdForShuffleService,
            new mutable.HashSet[RDDBlockId]())
          blockIdsToDel += blockId
          blockStatusByShuffleService.get(bmIdForShuffleService).foreach { blockStatusForId =>
            blockStatusForId.remove(block
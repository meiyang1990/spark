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

import scala.collection.BuildFrom
import scala.collection.immutable.Iterable
import scala.concurrent.Future

import org.apache.spark.SparkConf
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * 块存储管理的客户端代理，负责与Driver端的BlockManagerMasterEndpoint通信，
 * 协调节点间块存储信息，维护所有Executor块存储的全局元数据视图，是所有块管理操作的入口点。
 * 在Driver和Executor端都会实例化，用于RPC调用Driver端的主管理端点。
 *
 * @param driverEndpoint Driver端BlockManagerMasterEndpoint的RPC引用
 * @param driverHeartbeatEndPoint Driver端心跳端点的RPC引用
 * @param conf Spark配置对象
 * @param isDriver 是否运行在Driver节点
 */
private[spark]
class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    var driverHeartbeatEndPoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
  extends Logging {

  // RPC调用超时配置
  val timeout = RpcUtils.askRpcTimeout(conf)

  // 块删除操作等待超时配置，用于清理器引用跟踪
  private val waitBlockRemovalTimeout =
    RpcTimeout(conf, CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT.key, "120s")

  /**
   * 从Driver端移除已死亡的Executor，仅在Driver端调用
   * @param execId 要移除的Executor ID
   */
  def removeExecutor(execId: String): Unit = {
    tell(RemoveExecutor(execId))
    logInfo(log"Removed ${MDC(EXECUTOR_ID, execId)} successfully in removeExecutor")
  }

  /**
   * 停用指定Executor集合对应的块管理器，非阻塞操作
   * @param executorIds 要停用的Executor ID列表
   */
  def decommissionBlockManagers(executorIds: Seq[String]): Unit = {
    driverEndpoint.ask[Boolean](DecommissionBlockManagers(executorIds))
  }

  /**
   * 获取指定块管理器上所有RDD块需要复制的信息
   * @param blockManagerId 目标块管理器ID
   * @return 需要复制的块信息列表
   */
  def getReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId): Seq[ReplicateBlock] = {
    driverEndpoint.askSync[Seq[ReplicateBlock]](GetReplicateInfoForRDDBlocks(blockManagerId))
  }

  /**
   * 异步请求Driver端移除已死亡的Executor，仅在Driver端调用，非阻塞
   * @param execId 要移除的Executor ID
   */
  def removeExecutorAsync(execId: String): Unit = {
    driverEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo(log"Removal of executor ${MDC(EXECUTOR_ID, execId)} requested")
  }

  /**
   * 向Driver注册当前BlockManager，获取包含拓扑信息的更新后的BlockManagerId
   * @param id 当前BlockManager ID（初始不包含拓扑信息）
   * @param localDirs 本地存储目录数组
   * @param maxOnHeapMemSize 堆内存最大可用大小
   * @param maxOffHeapMemSize 堆外内存最大可用大小
   * @param storageEndpoint 当前BlockManager端点RPC引用
   * @param isReRegister 是否为重注册
   * @return 填充拓扑信息后的更新BlockManagerId
   */
  def registerBlockManager(
      id: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      storageEndpoint: RpcEndpointRef,
      isReRegister: Boolean = false): BlockManagerId = {
    logInfo(log"Registering BlockManager ${MDC(BLOCK_MANAGER_ID, id)}")
    val updatedId = driverEndpoint.askSync[BlockManagerId](
      RegisterBlockManager(
        id,
        localDirs,
        maxOnHeapMemSize,
        maxOffHeapMemSize,
        storageEndpoint,
        isReRegister
      )
    )
    if (updatedId.executorId == BlockManagerId.INVALID_EXECUTOR_ID) {
      assert(isReRegister, "Got invalid executor id from non re-register case")
      logInfo(log"Re-register BlockManager ${MDC(BLOCK_MANAGER_ID, id)} failed")
    } else {
      logInfo(log"Registered BlockManager ${MDC(BLOCK_MANAGER_ID, updatedId)}")
    }
    updatedId
  }

  /**
   * 向Driver更新块信息，通知Driver块存储位置和大小变化
   * @param blockManagerId 块所在块管理器ID
   * @param blockId 块ID
   * @param storageLevel 块存储级别
   * @param memSize 块占用内存大小
   * @param diskSize 块占用磁盘大小
   * @return 更新是否成功
   */
  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    val res = driverEndpoint.askSync[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logDebug(s"Updated info of block $blockId")
    res
  }

  /**
   * 向Driver更新RDD块与任务的关联关系
   * @param blockId RDD块ID
   * @param taskId 计算生成该块的任务ID
   */
  def updateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long): Unit = {
    driverEndpoint.askSync[Unit](UpdateRDDBlockTaskInfo(blockId, taskId))
  }

  /**
   * 异步更新Driver中指定任务生成的RDD块可见性
   * @param taskId 任务ID
   * @param visible 是否可见
   */
  def updateRDDBlockVisibility(taskId: Long, visible: Boolean): Unit = {
    driverEndpoint.ask[Unit](UpdateRDDBlockVisibility(taskId, visible))
  }

  /**
   * 检查指定RDD块是否可见
   * @param blockId RDD块ID
   * @return 块是否可见
   */
  def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    driverEndpoint.askSync[Boolean](GetRDDBlockVisibility(blockId))
  }

  /**
   * 从Driver获取指定块所在的所有块管理器位置
   * @param blockId 目标块ID
   * @return 存储该块的块管理器ID列表
   */
  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /**
   * 从Driver获取指定块的位置和状态信息
   * @param blockId 目标块ID
   * @param requesterHost 请求者主机地址，用于位置感知优化
   * @return 块位置和状态信息，不存在则返回None
   */
  def getLocationsAndStatus(
      blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus] = {
    driverEndpoint.askSync[Option[BlockLocationsAndStatus]](
      GetLocationsAndStatus(blockId, requesterHost))
  }

  /**
   * 从Driver批量获取多个块所在的块管理器位置
   * @param blockIds 目标块ID数组
   * @return 每个块对应的块管理器位置列表，顺序与输入一致
   */
  def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    driverEndpoint.askSync[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

  /**
   * 检查Driver端元数据中是否存在指定块
   * @param blockId 目标块ID
   * @return 元数据中是否存在该块
   */
  def contains(blockId: BlockId): Boolean = {
    getLocations(blockId).nonEmpty
  }

  /**
   * 从Driver获取集群中当前节点之外的其他所有节点块管理器ID
   * @param blockManagerId 当前节点块管理器ID
   * @return 其他节点块管理器ID列表
   */
  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
  }

  /**
   * 获取用于基于推送Shuffle的合并器位置列表，返回符合要求的可用外部Shuffle合并服务位置
   * @param numMergersNeeded 需要的合并器数量
   * @param hostsToFilter 需要过滤掉的主机集合
   * @return 符合要求的块管理器ID列表
   */
  def getShufflePushMergerLocations(
      numMergersNeeded: Int,
      hostsToFilter: Set[String]): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](
      GetShufflePushMergerLocations(numMergersNeeded, hostsToFilter))
  }

  /**
   * 从Shuffle推送合并候选列表中移除指定主机，当该主机发生获取失败时触发，非阻塞操作
   * @param host 需要移除的主机地址
   */
  def removeShufflePushMergerLocation(host: String): Unit = {
    logInfo(log"Request to remove shuffle push merger location ${MDC(HOST, host)}")
    driverEndpoint.ask[Unit](RemoveShufflePushMergerLocation(host))
  }

  /**
   * 从Driver获取指定Executor的块管理器端点RPC引用
   * @param executorId 目标Executor ID
   * @return 端点RPC引用，不存在则返回None
   */
  def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    driverEndpoint.askSync[Option[RpcEndpointRef]](GetExecutorEndpointRef(executorId))
  }

  /**
   * 通知所有存储该块的块管理器删除指定块，仅能删除Driver已知的块
   * @param blockId 要删除的块ID
   */
  def removeBlock(blockId: BlockId): Unit = {
    driverEndpoint.askSync[Boolean](RemoveBlock(blockId))
  }

  /**
   * 删除指定RDD的所有块，支持阻塞和非阻塞两种模式
   * @param rddId 要删除的RDD ID
   * @param blocking 是否阻塞等待删除完成
   */
  def removeRdd(rddId: Int, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId))
    future.failed.foreach(e =>
      logWarning(log"Failed to remove RDD ${MDC(RDD_ID, rddId)} - " +
        log"${MDC(ERROR, e.getMessage)}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      waitBlockRemovalTimeout.awaitResult(future)
    }
  }

  /**
   * 删除指定Shuffle的所有块，支持阻塞和非阻塞两种模式
   * @param shuffleId 要删除的Shuffle ID
   * @param blocking 是否阻塞等待删除完成
   */
  def removeShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    future.failed.foreach(e =>
      logWarning(log"Failed to remove shuffle ${MDC(SHUFFLE_ID, shuffleId)} - " +
        log"${MDC(ERROR, e.getMessage)}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      waitBlockRemovalTimeout.awaitResult(future)
    }
  }

  /**
   * 删除指定广播变量的所有块，支持阻塞和非阻塞两种模式
   * @param broadcastId 要删除的广播变量ID
   * @param removeFromMaster 是否同时从Driver元数据中移除
   * @param blocking 是否阻塞等待删除完成
   */
  def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    future.failed.foreach(e =>
      logWarning(log"Failed to remove broadcast ${MDC(BROADCAST_ID, broadcastId)}" +
        log" with removeFromMaster = ${MDC(REMOVE_FROM_MASTER, removeFromMaster)} - " +
        log"${MDC(ERROR, e.getMessage)}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
      waitBlockRemovalTimeout.awaitResult(future)
    }
  }

  /**
   * 获取所有块管理器的内存状态
   * @return 块管理器ID到(最大分配内存, 剩余可用内存)的映射表
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    if (driverEndpoint == null) return Map.empty
    driverEndpoint.askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  /**
   * 获取所有块管理器的存储状态
   * @return 所有块管理器的存储状态数组
   */
  def getStorageStatus: Array[StorageStatus] = {
    if (driverEndpoint == null) return Array.empty
    driverEndpoint.askSync[Array[StorageStatus]](GetStorageStatus)
  }

  /**
   * 获取指定块在所有块管理器上的状态，潜在开销较大，仅用于测试
   * @param blockId 目标块ID
   * @param askStorageEndpoints 是否主动询问所有块管理器获取最新状态，当Driver可能未收到所有块管理器报告时使用
   * @return 块管理器ID到块状态的映射表
   */
  def getBlockStatus(
      blockId: BlockId,
      askStorageEndpoints: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askStorageEndpoints)
    /*
     * 为避免潜在死锁，使用异步Future，因为主端点不应该阻塞等待块管理器响应，
     * 而块管理器可能正在等待主端点响应之前的请求
     */
    val response = driverEndpoint.
      askSync[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    val cbf =
      implicitly[
        BuildFrom[Iterable[Future[Option[BlockStatus]]],
        Option[BlockStatus],
        Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence(futures)(cbf, ThreadUtils.sameThread))
    if (blockStatus == null) {
      throw SparkCoreErrors.blockStatusQueryReturnedNullError(blockId)
    }
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  /**
   * 获取所有符合过滤条件的块ID，潜在开销较大，仅用于测试
   * @param filter 块ID过滤函数
   * @param askStorageEndpoints 是否主动询问所有块管理器获取最新状态
   * @return 符合条件的块ID列表
   */
  def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askStorageEndpoints: Boolean): Seq[BlockId] = {
    val msg = GetMatchingBlockIds(filter, askStorageEndpoints)
    val future = driverEndpoint.askSync[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
  }

  /**
   * 停止Driver端点，仅在Spark Driver节点调用
   */
  def stop(): Unit = {
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      if (driverHeartbeatEndPoint.askSync[Boolean](StopBlockManagerMaster)) {
        driverHeartbeatEndPoint = null
      } else {
        logWarning("Failed to stop BlockManagerMasterHeartbeatEndpoint")
      }
      logInfo("BlockManagerMaster stopped")
    }
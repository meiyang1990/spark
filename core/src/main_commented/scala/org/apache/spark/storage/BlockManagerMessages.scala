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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * 块管理器消息定义对象，定义了块管理器主节点和从节点之间RPC通信的所有消息类型
 * 用于整个集群存储模块的协调和块信息同步
 */
private[spark] object BlockManagerMessages {
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the master to storage endpoints.
  //////////////////////////////////////////////////////////////////////////////////
  /**
   * 所有发往块管理器主节点存储端点的消息基 trait
   */
  sealed trait ToBlockManagerMasterStorageEndpoint

  /**
   * 从指定存储端点删除指定块，仅能删除主节点已知的块
   * @param blockId 待删除块的ID
   */
  case class RemoveBlock(blockId: BlockId) extends ToBlockManagerMasterStorageEndpoint

  /**
   * 复制因Executor故障丢失的块，保证副本数满足要求
   * @param blockId 待复制块的ID
   * @param replicas 目标复制到的块管理器列表
   * @param maxReplicas 最大副本数
   */
  case class ReplicateBlock(blockId: BlockId, replicas: Seq[BlockManagerId], maxReplicas: Int)
    extends ToBlockManagerMasterStorageEndpoint

  /**
   * 通知块管理器准备退役的单例消息
   */
  case object DecommissionBlockManager extends ToBlockManagerMasterStorageEndpoint

  /**
   * 删除指定RDD的所有块
   * @param rddId 待删除RDD的ID
   */
  case class RemoveRdd(rddId: Int) extends ToBlockManagerMasterStorageEndpoint

  /**
   * 删除指定Shuffle的所有块
   * @param shuffleId 待删除Shuffle的ID
   */
  case class RemoveShuffle(shuffleId: Int) extends ToBlockManagerMasterStorageEndpoint

  /**
   * 删除指定广播变量的所有块
   * @param broadcastId 待删除广播变量的ID
   * @param removeFromDriver 是否同时从Driver端删除，默认为true
   */
  case class RemoveBroadcast(broadcastId: Long, removeFromDriver: Boolean = true)
    extends ToBlockManagerMasterStorageEndpoint

  /**
   * 将RDD块标记为可见，供其他节点读取
   * @param blockId 待标记的RDD块ID
   */
  case class MarkRDDBlockAsVisible(blockId: RDDBlockId) extends ToBlockManagerMasterStorageEndpoint

  /**
   * Driver发送给Executor的触发线程转储的单例消息
   */
  case object TriggerThreadDump extends ToBlockManagerMasterStorageEndpoint

  /**
   * Driver发送给Executor的获取堆直方图的单例消息
   */
  case object TriggerHeapHistogram extends ToBlockManagerMasterStorageEndpoint

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from storage endpoints to the master.
  //////////////////////////////////////////////////////////////////////////////////
  /**
   * 所有从存储端点发往块管理器主节点的消息基 trait
   */
  sealed trait ToBlockManagerMaster

  /**
   * 块管理器向主节点注册的消息
   * @param blockManagerId 待注册块管理器的ID
   * @param localDirs 块管理器的本地磁盘目录
   * @param maxOnHeapMemSize 堆内存最大可用大小
   * @param maxOffHeapMemSize 堆外内存最大可用大小
   * @param sender 注册方的RPC端点引用
   * @param isReRegister 是否是重新注册
   */
  case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      sender: RpcEndpointRef,
      isReRegister: Boolean)
    extends ToBlockManagerMaster

  /**
   * 更新块信息的消息，支持Java序列化
   * @param blockManagerId 块所在块管理器ID
   * @param blockId 块ID
   * @param storageLevel 块存储级别
   * @param memSize 块占用内存大小
   * @param diskSize 块占用磁盘大小
   */
  case class UpdateBlockInfo(
      var blockManagerId: BlockManagerId,
      var blockId: BlockId,
      var storageLevel: StorageLevel,
      var memSize: Long,
      var diskSize: Long)
    extends ToBlockManagerMaster
    with Externalizable {

    def this() = this(null, null, null, 0, 0)  // For deserialization only

    override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
      // 序列化块管理器ID
      blockManagerId.writeExternal(out)
      // 序列化块名称
      out.writeUTF(blockId.name)
      // 序列化存储级别
      storageLevel.writeExternal(out)
      // 序列化内存大小
      out.writeLong(memSize)
      // 序列化磁盘大小
      out.writeLong(diskSize)
    }

    override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
      // 反序列化块管理器ID
      blockManagerId = BlockManagerId(in)
      // 反序列化块ID
      blockId = BlockId(in.readUTF())
      // 反序列化存储级别
      storageLevel = StorageLevel(in)
      // 反序列化内存大小
      memSize = in.readLong()
      // 反序列化磁盘大小
      diskSize = in.readLong()
    }
  }

  // 更新 RDD Block 任务信息
  case class UpdateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long) extends ToBlockManagerMaster

  // 更新 RDD Block 可见性
  case class UpdateRDDBlockVisibility(taskId: Long, visible: Boolean) extends ToBlockManagerMaster

  // 获取 RDD Block 可见性
  case class GetRDDBlockVisibility(blockId: RDDBlockId) extends ToBlockManagerMaster

  // 获取指定 Block 的所有存储位置
  case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster

  /**
   * 获取指定 Block 的位置和存储状态
   * @param blockId 请求的块ID
   * @param requesterHost 请求方所在主机地址
   */
  case class GetLocationsAndStatus(blockId: BlockId, requesterHost: String)
    extends ToBlockManagerMaster

  /**
   * `GetLocationsAndStatus` 请求的响应消息
   *
   * @param locations 块所在的所有块管理器位置列表
   * @param status 块的存储状态
   * @param localDirs 如果 block 持久化到磁盘且与请求的 executor 在同一主机上，
   *                  则 localDirs 为 Some，缓存数据会在这些目录的某个文件中，
   *                  否则为 None。用于短路本地读取优化
   */
  case class BlockLocationsAndStatus(
      locations: Seq[BlockManagerId],
      status: BlockStatus,
      localDirs: Option[Array[String]]) {
    assert(locations.nonEmpty)
  }

  // 批量获取多个 Block 的存储位置
  case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster

  // 获取当前节点的对等块管理器列表，用于块复制
  case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  // 获取指定 Executor 的RPC端点引用
  case class GetExecutorEndpointRef(executorId: String) extends ToBlockManagerMaster

  // 删除指定 Executor 所有块的信息
  case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

  // 停止块管理器主节点的单例消息
  case object StopBlockManagerMaster extends ToBlockManagerMaster

  // 获取所有块管理器内存状态的单例消息
  case object GetMemoryStatus extends ToBlockManagerMaster

  // 获取所有块管理器存储状态的单例消息
  case object GetStorageStatus extends ToBlockManagerMaster

  // 对指定Executor列表执行块管理器退役操作
  case class DecommissionBlockManagers(executorIds: Seq[String]) extends ToBlockManagerMaster

  // 获取指定块管理器需要复制的RDD块信息
  case class GetReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId)
    extends ToBlockManagerMaster

  /**
   * 获取指定块的存储状态
   * @param blockId 请求的块ID
   * @param askStorageEndpoints 是否询问存储端点获取最新状态，默认为true
   */
  case class GetBlockStatus(blockId: BlockId, askStorageEndpoints: Boolean = true)
    extends ToBlockManagerMaster

  /**
   * 获取匹配过滤条件的所有块ID
   * @param filter 块ID过滤函数
   * @param askStorageEndpoints 是否询问存储端点获取最新状态，默认为true
   */
  case class GetMatchingBlockIds(filter: BlockId => Boolean, askStorageEndpoints: Boolean = true)
    extends ToBlockManagerMaster

  // 块管理器发送给主节点的心跳消息
  case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  // 检查指定Executor是否存活的消息
  case class IsExecutorAlive(executorId: String) extends ToBlockManagerMaster

  /**
   * 获取满足要求的Shuffle推送合并器位置
   * @param numMergersNeeded 需要的合并器数量
   * @param hostsToFilter 需要排除的主机集合
   */
  case class GetShufflePushMergerLocations(numMergersNeeded: Int, hostsToFilter: Set[String])
    extends ToBlockManagerMaster

  // 删除指定主机上失效的Shuffle推送合并器位置
  case class RemoveShufflePushMergerLocation(host: String) extends ToBlockManagerMaster

}
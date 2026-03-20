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

private[spark] object BlockManagerMessages {
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the master to storage endpoints.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMasterStorageEndpoint

  // Remove a block from the storage endpoints that have it. This can only be used to remove
  // blocks that the master knows about.
  case class RemoveBlock(blockId: BlockId) extends ToBlockManagerMasterStorageEndpoint

  // Replicate blocks that were lost due to executor failure
  case class ReplicateBlock(blockId: BlockId, replicas: Seq[BlockManagerId], maxReplicas: Int)
    extends ToBlockManagerMasterStorageEndpoint

  case object DecommissionBlockManager extends ToBlockManagerMasterStorageEndpoint

  // Remove all blocks belonging to a specific RDD.
  case class RemoveRdd(rddId: Int) extends ToBlockManagerMasterStorageEndpoint

  // Remove all blocks belonging to a specific shuffle.
  case class RemoveShuffle(shuffleId: Int) extends ToBlockManagerMasterStorageEndpoint

  // Remove all blocks belonging to a specific broadcast.
  case class RemoveBroadcast(broadcastId: Long, removeFromDriver: Boolean = true)
    extends ToBlockManagerMasterStorageEndpoint

  // Mark a rdd block as visible.
  case class MarkRDDBlockAsVisible(blockId: RDDBlockId) extends ToBlockManagerMasterStorageEndpoint

  /**
   * Driver to Executor message to trigger a thread dump.
   */
  case object TriggerThreadDump extends ToBlockManagerMasterStorageEndpoint

  /**
   * Driver to Executor message to get a heap histogram.
   */
  case object TriggerHeapHistogram extends ToBlockManagerMasterStorageEndpoint

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from storage endpoints to the master.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      sender: RpcEndpointRef,
      isReRegister: Boolean)
    extends ToBlockManagerMaster

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
      blockManagerId.writeExternal(out)
      out.writeUTF(blockId.name)
      storageLevel.writeExternal(out)
      out.writeLong(memSize)
      out.writeLong(diskSize)
    }

    override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
      blockManagerId = BlockManagerId(in)
      blockId = BlockId(in.readUTF())
      storageLevel = StorageLevel(in)
      memSize = in.readLong()
      diskSize = in.readLong()
    }
  }

  // 更新 RDD Block 任务信息
  case class UpdateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long) extends ToBlockManagerMaster

  // 更新 RDD Block 可见性
  case class UpdateRDDBlockVisibility(taskId: Long, visible: Boolean) extends ToBlockManagerMaster

  // 获取 RDD Block 可见性
  case class GetRDDBlockVisibility(blockId: RDDBlockId) extends ToBlockManagerMaster

  // 获取 Block 位置
  case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster

  // 获取 Block 位置和状态
  case class GetLocationsAndStatus(blockId: BlockId, requesterHost: String)
    extends ToBlockManagerMaster

  /**
   * `GetLocationsAndStatus` 请求的响应消息
   *
   * @param localDirs 如果 block 持久化到磁盘且与请求的 executor 在同一主机上，
   *                  则 localDirs 为 Some，缓存数据会在这些目录的某个文件中，
   *                  否则为 None。
   */
  case class BlockLocationsAndStatus(
      locations: Seq[BlockManagerId],
      status: BlockStatus,
      localDirs: Option[Array[String]]) {
    assert(locations.nonEmpty)
  }

  // 获取多个 Block ID 的位置
  case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster

  // 获取对等 BlockManager
  case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  // 获取 Executor 端点引用
  case class GetExecutorEndpointRef(executorId: String) extends ToBlockManagerMaster

  // 删除 Executor
  case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

  // 停止 BlockManagerMaster
  case object StopBlockManagerMaster extends ToBlockManagerMaster

  // 获取内存状态
  case object GetMemoryStatus extends ToBlockManagerMaster

  // 获取存储状态
  case object GetStorageStatus extends ToBlockManagerMaster

  // 退役多个 BlockManager
  case class DecommissionBlockManagers(executorIds: Seq[String]) extends ToBlockManagerMaster

  // 获取 RDD Block 的复制信息
  case class GetReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId)
    extends ToBlockManagerMaster

  // 获取 Block 状态
  case class GetBlockStatus(blockId: BlockId, askStorageEndpoints: Boolean = true)
    extends ToBlockManagerMaster

  // 获取匹配的 Block ID
  case class GetMatchingBlockIds(filter: BlockId => Boolean, askStorageEndpoints: Boolean = true)
    extends ToBlockManagerMaster

  // BlockManager 心跳
  case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  // 检查 Executor 是否存活
  case class IsExecutorAlive(executorId: String) extends ToBlockManagerMaster

  // 获取 Shuffle Push Merger 位置
  case class GetShufflePushMergerLocations(numMergersNeeded: Int, hostsToFilter: Set[String])
    extends ToBlockManagerMaster

  // 删除 Shuffle Push Merger 位置
  case class RemoveShufflePushMergerLocation(host: String) extends ToBlockManagerMaster

}

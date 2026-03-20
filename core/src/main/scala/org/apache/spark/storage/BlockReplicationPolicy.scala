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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._

/**
 * ::DeveloperApi::
 * BlockReplicationPolicy 提供了对 peer 序列进行优先级排序以复制 block 的逻辑。
 * BlockManager 会按返回顺序向每个 peer 复制，直到达到所需的复制顺序。
 * 如果复制失败，会再次调用 prioritize() 获取新的优先级排序。
 */
@DeveloperApi
trait BlockReplicationPolicy {

  /**
   * 对一组候选 peer 进行优先级排序的方法
   *
   * @param blockManagerId 当前 BlockManager 的 ID，用于自我识别
   * @param peers BlockManager 的 peer 列表
   * @param peersReplicatedTo 已复制到的 peer 集合
   * @param blockId 正在复制的 block 的 BlockId。如果需要可以作为随机性来源。
   * @param numReplicas 需要复制到的 peer 数量
   * @return 优先级排序的 peer 列表。peer 的索引越低，优先级越高。
   *         返回列表的大小最多为 `numPeersToReplicateTo`。
   */
  def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId]
}

/**
 * Block 复制工具类
 */
object BlockReplicationUtils {
  /**
   * 使用 Robert Floyd 的采样算法。在 O(n) 时间内找到随机样本，同时最小化空间使用。
   * 详见 <a href="https://math.stackexchange.com/q/178690">这里</a>。
   *
   * @param n 总索引数
   * @param m 需要的样本数
   * @param r 随机数生成器
   * @return m 个随机唯一索引的列表
   */
  private def getSampleIds(n: Int, m: Int, r: Random): List[Int] = {
    val indices = (n - m + 1 to n).foldLeft(mutable.LinkedHashSet.empty[Int]) {case (set, i) =>
      val t = r.nextInt(i) + 1
      if (set.contains(t)) set.union(Set(i)) else set.union(Set(t))
    }
    indices.map(_ - 1).toList
  }

  /**
   * 从 elems 中获取大小为 m 的随机样本
   *
   * @param elems 元素序列
   * @param m 需要的样本数
   * @param r 随机数生成器
   * @tparam T 元素类型
   * @return 大小为 m 的随机列表。如果 elems 中少于 m 个元素，则随机洗牌 elems
   */
  def getRandomSample[T](elems: Seq[T], m: Int, r: Random): List[T] = {
    if (elems.size > m) {
      getSampleIds(elems.size, m, r).map(elems(_))
    } else {
      r.shuffle(elems).toList
    }
  }
}

/**
 * 随机 Block 复制策略。
 * 这是一个基本实现，只确保在可能的情况下将 block 放在不同的主机上。
 */
@DeveloperApi
class RandomBlockReplicationPolicy
  extends BlockReplicationPolicy
  with Logging {

  /**
   * 对一组候选 peer 进行优先级排序的方法。这是一个基本实现，
   * 只确保在可能的情况下将 block 放在不同的主机上。
   *
   * @param blockManagerId 当前 BlockManager 的 ID，用于自我识别
   * @param peers BlockManager 的 peer 列表
   * @param peersReplicatedTo 已复制到的 peer 集合
   * @param blockId 正在复制的 block 的 BlockId。如果需要可以作为随机性来源。
   * @param numReplicas 需要复制到的 peer 数量
   * @return 优先级排序的 peer 列表。peer 的索引越低，优先级越高
   */
  override def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId] = {
    val random = new Random(blockId.hashCode)
    logDebug(s"Input peers : ${peers.mkString(", ")}")
    val prioritizedPeers = if (peers.size > numReplicas) {
      BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    } else {
      if (peers.size < numReplicas) {
        logWarning(log"Expecting ${MDC(NUM_REPLICAS, numReplicas)} " +
          log"replicas with only ${MDC(NUM_PEERS, peers.size)} peer/s.")
      }
      random.shuffle(peers).toList
    }
    logDebug(s"Prioritized peers : ${prioritizedPeers.mkString(", ")}")
    prioritizedPeers
  }
}

/**
 * 基本 Block 复制策略。
 * 复制 HDFS 的 block 复制行为。在需要特定数量副本的情况下，
 * 选择机架内的一个 peer、机架外的一个 peer，剩余的随机选择，
 * 按此顺序直到满足副本数量要求。
 * 这在总复制因子为 3 时效果最好，类似 HDFS。
 */
@DeveloperApi
class BasicBlockReplicationPolicy
  extends BlockReplicationPolicy
    with Logging {

  /**
   * 对一组候选 peer 进行优先级排序的方法。此实现复制 HDFS 的 block 复制行为。
   * 在需要特定数量副本的情况下，选择机架内的一个 peer、机架外的一个 peer，
   * 剩余的随机选择，按此顺序直到满足副本数量要求。
   * 这在总复制因子为 3 时效果最好，类似 HDFS。
   *
   * @param blockManagerId 当前 BlockManager 的 ID，用于自我识别
   * @param peers BlockManager 的 peer 列表
   * @param peersReplicatedTo 已复制到的 peer 集合
   * @param blockId 正在复制的 block 的 BlockId。如果需要可以作为随机性来源。
   * @param numReplicas 需要复制到的 peer 数量
   * @return 优先级排序的 peer 列表。peer 的索引越低，优先级越高
   */
  override def prioritize(
      blockManagerId: BlockManagerId,
      peers: Seq[BlockManagerId],
      peersReplicatedTo: mutable.HashSet[BlockManagerId],
      blockId: BlockId,
      numReplicas: Int): List[BlockManagerId] = {

    logDebug(s"Input peers : $peers")
    logDebug(s"BlockManagerId : $blockManagerId")

    val random = new Random(blockId.hashCode)

    // 如果 block 没有拓扑信息，无法做太多处理，只能随机洗牌
    // 如果有拓扑信息，根据 peersReplicatedTo 和 numReplicas 选择所需的 peer
    if (blockManagerId.topologyInfo.isEmpty || numReplicas == 0) {
      // block 没有拓扑信息。最好的做法是随机选择 peer
      BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
    } else {
      // 有拓扑信息，查看 peersReplicatedTo 中还需要做什么
      val doneWithinRack = peersReplicatedTo.exists(_.topologyInfo == blockManagerId.topologyInfo)
      val doneOutsideRack = peersReplicatedTo.exists { p =>
        p.topologyInfo.isDefined && p.topologyInfo != blockManagerId.topologyInfo
      }

      if (doneOutsideRack && doneWithinRack) {
        // 已完成，只返回随机样本
        BlockReplicationUtils.getRandomSample(peers, numReplicas, random)
      } else {
        // 分离机架内和机架外的 peer
        val (inRackPeers, outOfRackPeers) = peers
            .filter(_.host != blockManagerId.host)
            .partition(_.topologyInfo == blockManagerId.topologyInfo)

        val peerWithinRack = if (doneWithinRack) {
          // 机架内复制已完成，不需要更多 peer
          Seq.empty
        } else {
          if (inRackPeers.isEmpty) {
            Seq.empty
          } else {
            Seq(inRackPeers(random.nextInt(inRackPeers.size)))
          }
        }

        val peerOutsideRack = if (doneOutsideRack || numReplicas - peerWithinRack.size <= 0) {
          Seq.empty
        } else {
          if (outOfRackPeers.isEmpty) {
            Seq.empty
          } else {
            Seq(outOfRackPeers(random.nextInt(outOfRackPeers.size)))
          }
        }

        val priorityPeers = peerWithinRack ++ peerOutsideRack
        val numRemainingPeers = numReplicas - priorityPeers.size
        val remainingPeers = if (numRemainingPeers > 0) {
          val rPeers = peers.filter(p => !priorityPeers.contains(p))
          BlockReplicationUtils.getRandomSample(rPeers, numRemainingPeers, random)
        } else {
          Seq.empty
        }

        (priorityPeers ++ remainingPeers).toList
      }

    }
  }

}

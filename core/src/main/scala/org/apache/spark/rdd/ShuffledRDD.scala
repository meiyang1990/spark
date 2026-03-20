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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

/**
 * ShuffledRDD 的分区类，仅包含分区索引
 */
private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 *
 * ShuffledRDD 是 Shuffle 操作产生的 RDD，是理解 Spark Shuffle 机制的关键类。
 *
 * Shuffle 的触发场景：
 *   - reduceByKey、groupByKey、aggregateByKey 等聚合操作
 *   - join、cogroup 等关联操作
 *   - repartition、coalesce(shuffle=true) 等重分区操作
 *   - sortByKey 等排序操作
 *
 * Shuffle 的工作原理：
 *   1. Map 阶段：上游任务按 Partitioner 将数据写入本地磁盘（可能经过 Map 端聚合）
 *   2. Shuffle 阶段：数据通过网络传输到下游任务
 *   3. Reduce 阶段：下游任务读取并处理 Shuffle 数据
 *
 * 核心属性：
 *   - partitioner：决定数据如何分布到各个分区
 *   - aggregator：可选的聚合器，用于 Map 端和 Reduce 端的聚合
 *   - keyOrdering：可选的 key 排序器，用于排序操作
 *   - mapSideCombine：是否在 Map 端进行预聚合（优化性能）
 *
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {

  // 用户指定的序列化器，用于 Shuffle 数据的序列化
  private var userSpecifiedSerializer: Option[Serializer] = None

  // Key 的排序器，用于 sortByKey 等需要排序的操作
  private var keyOrdering: Option[Ordering[K]] = None

  // 聚合器，定义如何在 Map 端和 Reduce 端聚合数据
  private var aggregator: Option[Aggregator[K, V, C]] = None

  // 是否在 Map 端进行预聚合（类似于 MapReduce 的 Combiner）
  private var mapSideCombine: Boolean = false

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  /**
   * 返回 ShuffleDependency（宽依赖）
   * ShuffleDependency 包含了 Shuffle 所需的所有信息
   */
  override def getDependencies: Seq[Dependency[_]] = {
    // 选择合适的序列化器
    val serializer = userSpecifiedSerializer.getOrElse {
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        // Map 端聚合后，Value 类型变为 C（聚合结果类型）
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        // 无 Map 端聚合，Value 类型保持为 V
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    // 创建 ShuffleDependency，这是 Spark Shuffle 的核心数据结构
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  // 设置分区器，这是 K-V RDD 的重要属性
  override val partitioner = Some(part)

  /**
   * 根据分区器的分区数创建分区数组
   * 每个分区对应一个 Reduce 任务
   */
  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  /**
   * 获取分区的首选位置
   * 通过 MapOutputTracker 查询 Shuffle 数据的位置，优先从数据量大的位置读取
   */
  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  /**
   * 计算分区数据，这是 Shuffle Read 的核心逻辑。
   * 通过 ShuffleManager 的 Reader 从各个 Map 输出读取属于本分区的数据
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    // 使用 ShuffleReader 读取 Shuffle 数据
    SparkEnv.get.shuffleManager.getReader(
      dep.shuffleHandle, split.index, split.index + 1, context, metrics)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  /**
   * 清除对父 RDD 的引用，允许父 RDD 被垃圾回收
   */
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * ShuffledRDD 不是 Barrier RDD，因为 Shuffle 边界会打断 Barrier Stage
   */
  private[spark] override def isBarrier(): Boolean = false
}

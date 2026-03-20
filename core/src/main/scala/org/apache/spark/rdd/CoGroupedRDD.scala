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

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{CompactBuffer, ExternalAppendOnlyMap}

/**
 * The references to rdd and splitIndex are transient because redundant information is stored
 * in the CoGroupedRDD object.  Because CoGroupedRDD is serialized separately from
 * CoGroupPartition, if rdd and splitIndex aren't transient, they'll be included twice in the
 * task closure.
 *
 * NarrowCoGroupSplitDep —— 窄依赖分区信息包装类
 *
 * 【设计目的】
 * 在 CoGroupedRDD 中，如果某个父 RDD 与 CoGroupedRDD 使用相同的 Partitioner，
 * 则它们之间是窄依赖（OneToOneDependency），不需要 Shuffle。
 * 此类用于存储这种窄依赖的分区信息。
 *
 * 【transient 字段】
 * rdd 和 splitIndex 被标记为 transient，因为：
 * - CoGroupedRDD 和 CoGroupPartition 分开序列化
 * - 如果不标记为 transient，这些信息会被序列化两次
 * - split 字段在 writeObject 时会从 rdd.partitions(splitIndex) 更新
 *
 * @param rdd 父 RDD（transient，不序列化）
 * @param splitIndex 分区索引（transient，不序列化）
 * @param split 实际的分区对象（序列化时更新）
 */
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    // 在序列化时更新分区引用，确保获取最新的分区信息
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

/**
 * Stores information about the narrow dependencies used by a CoGroupedRdd.
 *
 * @param narrowDeps maps to the dependencies variable in the parent RDD: for each one to one
 *                   dependency in dependencies, narrowDeps has a NarrowCoGroupSplitDep (describing
 *                   the partition for that dependency) at the corresponding index. The size of
 *                   narrowDeps should always be equal to the number of parents.
 *
 * CoGroupPartition —— CoGroupedRDD 的分区类
 *
 * 【分区结构】
 * narrowDeps 数组的长度等于父 RDD 的数量：
 * - 如果第 i 个父 RDD 是窄依赖，narrowDeps(i) = Some(NarrowCoGroupSplitDep)
 * - 如果第 i 个父 RDD 是宽依赖（Shuffle），narrowDeps(i) = None
 *
 * @param index 分区索引
 * @param narrowDeps 窄依赖分区信息数组
 */
private[spark] class CoGroupPartition(
    override val index: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override def hashCode(): Int = index
  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * An RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 *
 * @note This is an internal API. We recommend users use RDD.cogroup(...) instead of
 * instantiating this directly.
 *
 * CoGroupedRDD —— 多个 RDD 按 Key 分组的核心实现
 *
 * 【核心功能】
 * 将多个 RDD 按照相同的 key 进行分组，每个 key 对应一个数组，
 * 数组的第 i 个元素是第 i 个 RDD 中该 key 的所有 value。
 *
 * 【输出格式】
 * (K, Array[Iterable[_]])
 * - K：键
 * - Array[Iterable[_]]：长度等于父 RDD 数量，每个元素是对应 RDD 中该 key 的 values
 *
 * 【使用场景】
 * cogroup 是 join、leftOuterJoin、rightOuterJoin、fullOuterJoin 的底层实现基础：
 * - join: cogroup 后对两组 values 做笛卡尔积
 * - leftOuterJoin: cogroup 后，如果右侧为空则配 None
 * - ...
 *
 * 【依赖类型】
 * 对于每个父 RDD：
 * - 如果父 RDD 的 Partitioner 与本 RDD 相同：窄依赖（OneToOneDependency），无需 Shuffle
 * - 否则：宽依赖（ShuffleDependency），需要 Shuffle
 *
 * 这就是为什么在 join 之前对两个 RDD 使用相同的 Partitioner 可以提升性能！
 *
 * 【内部类型定义】
 * - CoGroup = CompactBuffer[Any]：存储某个 RDD 中某个 key 的所有 value
 * - CoGroupValue = (Any, Int)：中间状态，(value, 来源 RDD 编号)
 * - CoGroupCombiner = Array[CoGroup]：聚合器，按 RDD 编号存储各组 value
 *
 * @param rdds 父 RDD 序列
 * @param part 输出分区器
 */
@DeveloperApi
class CoGroupedRDD[K: ClassTag](
    @transient var rdds: Seq[RDD[_ <: Product2[K, _]]],
    part: Partitioner)
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {

  // For example, `(k, a) cogroup (k, b)` produces k -> Array(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Array as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  // CoGroup：存储单个 RDD 中某 key 的所有 value（使用 CompactBuffer 优化小集合内存）
  private type CoGroup = CompactBuffer[Any]
  // CoGroupValue：中间值，包含 (value, 依赖编号) 以便知道该 value 来自哪个 RDD
  private type CoGroupValue = (Any, Int)  // Int is dependency number
  // CoGroupCombiner：聚合器，是一个数组，索引 i 对应第 i 个 RDD 的 value 集合
  private type CoGroupCombiner = Array[CoGroup]

  private var serializer: Serializer = SparkEnv.get.serializer

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = serializer
    this
  }

  /**
   * getDependencies —— 构建依赖关系
   *
   * 【依赖判断逻辑】
   * 对每个父 RDD 检查其 Partitioner：
   * - 相同 Partitioner → OneToOneDependency（窄依赖，不 Shuffle）
   * - 不同 Partitioner → ShuffleDependency（宽依赖，需要 Shuffle）
   */
  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {
        // 窄依赖：父 RDD 的 Partitioner 与目标相同，无需重分区
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        // 宽依赖：需要 Shuffle 重分区
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
  }

  /**
   * getPartitions —— 创建分区数组
   *
   * 每个分区记录对应的窄依赖信息（宽依赖通过 Shuffle 读取，不需要在分区中记录）
   */
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- array.indices) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      // 为每个分区创建 CoGroupPartition，记录窄依赖的分区映射
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None  // 宽依赖，不记录（通过 ShuffleReader 读取）
          case _ =>
            // 窄依赖，记录父 RDD 对应分区的信息
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  // 返回 Partitioner，因为 cogroup 结果是按 key 分区的
  override val partitioner: Some[Partitioner] = Some(part)

  /**
   * compute —— 计算分区数据
   *
   * 【执行流程】
   * 1. 收集所有父 RDD 在该分区的数据：
   *    - 窄依赖：直接从父 RDD 读取
   *    - 宽依赖：通过 ShuffleReader 读取 Shuffle 数据
   *
   * 2. 使用 ExternalAppendOnlyMap 按 key 聚合：
   *    - 将每个 (key, value) 转换为 (key, CoGroupValue)
   *    - CoGroupValue = (value, 依赖编号)
   *    - 按 key 聚合到 CoGroupCombiner 中
   *
   * 3. 返回 (key, Array[Iterable[_]]) 的迭代器
   */
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length

    // A list of (rdd iterator, dependency number) pairs
    // 收集所有父 RDD 在该分区的迭代器，并记录其依赖编号
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      // 窄依赖：直接从父 RDD 的对应分区读取
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))

      // 宽依赖：通过 ShuffleReader 读取 Shuffle 数据
      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of shuffle
        val metrics = context.taskMetrics().createTempShuffleReadMetrics()
        val it = SparkEnv.get.shuffleManager
          .getReader(
            shuffleDependency.shuffleHandle, split.index, split.index + 1, context, metrics)
          .read()
        rddIterators += ((it, depNum))
    }

    // 使用 ExternalAppendOnlyMap 进行聚合（支持溢出到磁盘，处理大数据量）
    val map = createExternalMap(numRdds)
    for ((it, depNum) <- rddIterators) {
      // 将每个 (key, value) 转换为 (key, (value, depNum))，以便知道来自哪个 RDD
      map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
    }
    // 更新 Shuffle 读取指标
    context.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    // 返回支持中断的迭代器
    new InterruptibleIterator(context,
      map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
  }

  /**
   * createExternalMap —— 创建用于 cogroup 聚合的 ExternalAppendOnlyMap
   *
   * 【聚合逻辑】
   * - createCombiner：遇到新 key 时，创建 Array[CoGroup]，并将 value 放入对应位置
   * - mergeValue：将新 value 追加到对应 RDD 的 CoGroup 中
   * - mergeCombiners：合并两个 CoGroupCombiner（合并各位置的 CoGroup）
   *
   * @param numRdds 父 RDD 的数量，决定 CoGroupCombiner 数组的大小
   */
  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {

    // 遇到新 key 时创建累加器：初始化长度为 numRdds 的数组
    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1  // value._2 是依赖编号
      newCombiner
    }
    // 合并新 value 到累加器：追加到对应依赖编号的 CoGroup
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }
    // 合并两个累加器：将各位置的 CoGroup 合并
    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {
        var depNum = 0
        while (depNum < numRdds) {
          combiner1(depNum) ++= combiner2(depNum)
          depNum += 1
        }
        combiner1
      }
    new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
  }

  // 清理依赖，帮助 GC
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

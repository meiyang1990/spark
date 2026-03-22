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
 * 窄依赖分区信息包装类，用于存储CoGroupedRDD中窄依赖的父RDD分区信息
 * 
 * rdd和splitIndex标记为transient的原因：
 * CoGroupedRDD和CoGroupPartition分开序列化，如果不标记transient，这些信息会被重复序列化到任务闭包
 * 
 * @param rdd 父RDD（transient，不参与序列化）
 * @param splitIndex 分区索引（transient，不参与序列化）
 * @param split 实际的分区对象，序列化时会重新更新
 */
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 任务序列化时更新父分区引用，确保获取最新分区信息
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

/**
 * CoGroupedRDD的分区实现类，存储当前分区对应的窄依赖信息
 * 
 * @param index 分区索引
 * @param narrowDeps 窄依赖分区信息数组，长度等于父RDD数量：
 *                    对应位置为Some表示该父RDD是窄依赖，None表示该父RDD是宽依赖（Shuffle）
 */
private[spark] class CoGroupPartition(
    override val index: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override def hashCode(): Int = index
  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * 多RDD按Key分组聚合的RDD实现，是cogroup操作的核心底层实现，也是各类join操作的基础
 * 
 * 对于每个key，输出结果为一个元组，包含所有父RDD中该key对应的value列表
 * 对于已经使用相同Partitioner的父RDD，会自动优化为窄依赖，避免不必要的Shuffle
 * 
 * @note 这是内部API，推荐用户直接使用RDD.cogroup(...)，不建议直接实例化此类
 * @param rdds 待分组的父RDD序列，每个RDD元素为(key, value)结构
 * @param part 输出结果使用的分区器
 */
@DeveloperApi
class CoGroupedRDD[K: ClassTag](
    @transient var rdds: Seq[RDD[_ <: Product2[K, _]]],
    part: Partitioner)
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {

  // 存储单个RDD中某key的所有value，使用CompactBuffer优化小集合内存占用
  private type CoGroup = CompactBuffer[Any]
  // 中间值结构，存储(value, 父RDD依赖编号)，用于标识value来源
  private type CoGroupValue = (Any, Int)
  // 聚合结果数组，索引对应父RDD编号，每个位置存储对应RDD中该key的所有value
  private type CoGroupCombiner = Array[CoGroup]

  private var serializer: Serializer = SparkEnv.get.serializer

  /**
   * 设置Shuffle使用的序列化器，null表示使用默认配置（spark.serializer）
   */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = serializer
    this
  }

  /**
   * 构建CoGroupedRDD与父RDD的依赖关系
   * 
   * 对每个父RDD自动判断依赖类型：
   * - 父RDD分区器与当前RDD相同 → 窄依赖（OneToOneDependency），无需Shuffle
   * - 父RDD分区器与当前RDD不同 → 宽依赖（ShuffleDependency），需要Shuffle重分区
   */
  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
  }

  /**
   * 生成CoGroupedRDD的所有分区，每个分区记录对应的窄依赖分区信息
   */
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- array.indices) {
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            // 宽依赖，通过Shuffle读取数据，无需记录分区信息
            None
          case _ =>
            // 窄依赖，记录父RDD对应分区的信息
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  /**
   * 计算分区数据，按key聚合所有父RDD的value
   * 
   * 执行流程：
   * 1. 收集所有父RDD当前分区的数据：窄依赖直接读取，宽依赖通过Shuffle读取
   * 2. 使用ExternalAppendOnlyMap按key聚合，区分不同RDD来源
   * 3. 返回最终聚合结果迭代器
   */
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length

    // 存储(父RDD迭代器, 依赖编号)对
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // 窄依赖直接从父RDD对应分区读取数据
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // 宽依赖从Shuffle读取map端输出数据
        val metrics = context.taskMetrics().createTempShuffleReadMetrics()
        val it = SparkEnv.get.shuffleManager
          .getReader(
            shuffleDependency.shuffleHandle, split.index, split.index + 1, context, metrics)
          .read()
        rddIterators += ((it, depNum))
    }

    // 使用支持磁盘溢出的ExternalAppendOnlyMap完成聚合
    val map = createExternalMap(numRdds)
    for ((it, depNum) <- rddIterators) {
      // 将数据转换为(key, (value, 依赖编号))格式插入聚合器
      map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
    }
    // 更新任务指标统计
    context.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    // 返回支持任务中断的迭代器
    new InterruptibleIterator(context,
      map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
  }

  /**
   * 创建用于cogroup聚合的ExternalAppendOnlyMap，定义三个核心聚合函数
   * 
   * @param numRdds 父RDD数量，决定聚合结果数组长度
   */
  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {

    // 新key遇到时创建组合器：初始化长度为numRdds的空数组
    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1
      newCombiner
    }
    // 合并新值到已有组合器：将value追加到对应R编号位置
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }
    // 合并两个组合器：将两个组合器对应位置的value集合合并
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

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
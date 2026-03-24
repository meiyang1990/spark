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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

/**
 * 文件说明：ShuffledRDD实现类，位于Spark核心模块，代表Shuffle操作后生成的结果RDD，是Spark Shuffle机制的核心组成部分
 * 核心职责：封装Shuffle下游阶段的分区信息和数据读取逻辑，为Shuffle任务执行提供数据抽象
 */

/**
 * ShuffledRDD的分区实现类，仅保存分区索引，每个分区对应一个下游Reduce任务
 */
private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
}

/**
 * :: DeveloperApi ::
 * Shuffle操作生成的结果RDD，用于存储重分区后的数据
 * 
 * 核心职责：封装Shuffle后RDD的分区信息、依赖关系和数据读取逻辑，是所有会产生宽依赖的操作的结果载体
 * 触发场景：reduceByKey、groupByKey、join、repartition等操作都会生成ShuffledRDD
 * 
 * @param prev 父RDD，即Shuffle上游的输入数据
 * @param part 分区器，决定数据如何分布到下游分区
 * @tparam K Key类型
 * @tparam V Value类型
 * @tparam C 聚合结果类型
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {

  // 用户指定的Shuffle数据序列化器，None则使用默认配置
  private var userSpecifiedSerializer: Option[Serializer] = None

  // Key排序器，用于需要按Key排序的Shuffle操作（如sortByKey）
  private var keyOrdering: Option[Ordering[K]] = None

  // 聚合器，定义Map端和Reduce端的聚合逻辑
  private var aggregator: Option[Aggregator[K, V, C]] = None

  // 是否开启Map端预聚合，开启后可减少Shuffle传输数据量，类似MapReduce的Combiner
  private var mapSideCombine: Boolean = false

  /**
   * 设置Shuffle使用的自定义序列化器，返回当前对象支持链式调用
   * @param serializer 自定义序列化器，传null则使用默认配置
   * @return 当前ShuffledRDD实例
   */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  /**
   * 设置Shuffle使用的Key排序器，返回当前对象支持链式调用
   * @param keyOrdering Key排序器
   * @return 当前ShuffledRDD实例
   */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /**
   * 设置Shuffle使用的聚合器，返回当前对象支持链式调用
   * @param aggregator 聚合器实例
   * @return 当前ShuffledRDD实例
   */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /**
   * 设置是否开启Map端预聚合，返回当前对象支持链式调用
   * @param mapSideCombine 是否开启预聚合
   * @return 当前ShuffledRDD实例
   */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  /**
   * 获取当前RDD的依赖，返回ShuffleDependency（宽依赖），包含Shuffle所需全部元信息
   * @return 仅包含一个ShuffleDependency的序列
   */
  override def getDependencies: Seq[Dependency[_]] = {
    // 根据是否开启Map端预聚合选择对应类型的序列化器
    val serializer = userSpecifiedSerializer.getOrElse {
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        // 开启预聚合后，值类型变为聚合结果类型C
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        // 未开启预聚合，值类型保持原始V
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    // 创建并返回ShuffleDependency，这是Spark中表示宽依赖的核心数据结构
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  // 当前RDD使用的分区器，ShuffledRDD一定有分区器
  override val partitioner = Some(part)

  /**
   * 根据分区器的分区数生成ShuffledRDD的所有分区，每个分区对应一个下游Reduce任务
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  /**
   * 获取指定分区的首选存放位置，根据数据分布优先选择数据量多的节点，实现数据本地性优化
   * @param partition 目标分区
   * @return 首选位置列表
   */
  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  /**
   * 计算指定分区的数据，核心逻辑是通过ShuffleReader读取上游Map输出的对应分区数据
   * @param split 待计算的分区
   * @param context 任务上下文
   * @return 分区内所有(Key, 聚合结果)的迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()
    // 获取ShuffleManager并读取对应分区数据
    SparkEnv.get.shuffleManager.getReader(
      dep.shuffleHandle, split.index, split.index + 1, context, metrics)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  /**
   * 清除依赖后释放对父RDD的引用，帮助垃圾回收
   */
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * ShuffledRDD本身不属于Barrier RDD，Shuffle边界会打断Barrier Stage的执行
   * @return 返回false，表示当前不是Barrier RDD
   */
  private[spark] override def isBarrier(): Boolean = false
}
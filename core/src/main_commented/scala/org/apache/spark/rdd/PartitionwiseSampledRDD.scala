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

import java.util.Random

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.util.Utils
import org.apache.spark.util.random.RandomSampler

/**
 * 按分区抽样的RDD分区实现，封装父RDD分区和当前分区的抽样随机种子
 * @param prev 父RDD对应的原分区
 * @param seed 当前分区抽样使用的随机种子
 */
private[spark]
class PartitionwiseSampledRDDPartition(val prev: Partition, val seed: Long)
  extends Partition with Serializable {
  override val index: Int = prev.index
}

/**
 * 按分区独立抽样生成的抽样RDD，对父RDD每个分区独立执行抽样操作，每个分区使用不同的随机种子保证随机性
 * 用于实现Spark中按分区的随机采样操作，保持数据分布特性的同时控制抽样规模
 * 
 * @param prev 待抽样的父RDD
 * @param sampler 随机抽样器实例，用于执行实际抽样逻辑
 * @param preservesPartitioning 是否保留父RDD的分区器
 * @param seed 全局随机种子，用于生成各个分区的独立种子
 * @tparam T 输入RDD元素类型
 * @tparam U 抽样后RDD元素类型
 */
private[spark] class PartitionwiseSampledRDD[T: ClassTag, U: ClassTag](
    prev: RDD[T],
    sampler: RandomSampler[T, U],
    preservesPartitioning: Boolean,
    @transient private val seed: Long = Utils.random.nextLong)
  extends RDD[U](prev) {

  // 如果保留分区则继承父RDD的分区器，否则不设置分区器
  @transient override val partitioner = if (preservesPartitioning) prev.partitioner else None

  /**
   * 生成抽样RDD的分区，每个父分区对应一个新分区并分配独立随机种子
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = {
    val random = new Random(seed)
    firstParent[T].partitions.map(x => new PartitionwiseSampledRDDPartition(x, random.nextLong()))
  }

  /**
   * 获取分区的优先位置，继承父分区的位置偏好以实现数据本地化
   * @param split 当前抽样分区
   * @return 优先位置列表
   */
  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[PartitionwiseSampledRDDPartition].prev)

  /**
   * 对当前分区执行抽样计算，使用独立种子对父分区数据抽样
   * @param splitIn 当前抽样分区
   * @param context 任务上下文
   * @return 抽样结果迭代器
   */
  override def compute(splitIn: Partition, context: TaskContext): Iterator[U] = {
    val split = splitIn.asInstanceOf[PartitionwiseSampledRDDPartition]
    val thisSampler = sampler.clone
    thisSampler.setSeed(split.seed)
    thisSampler.sample(firstParent[T].iterator(split.prev, context))
  }

  /**
   * 获取输出结果的确定性级别，如果父RDD输出本身无序则当前输出为不确定，否则继承父类确定性级别
   * @return 确定性级别枚举值
   */
  override protected def getOutputDeterministicLevel = {
    if (prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
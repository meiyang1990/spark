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

import org.apache.spark.{Partition, TaskContext}

/**
 * 文件概述：MapPartitionsRDD 实现类，是 Spark 核心 RDD 类型之一，用于对父 RDD 每个分区执行用户自定义转换函数
 *
 * 核心职责：将用户定义的分区级转换作用于父 RDD，是 map/filter/flatMap 等大多数窄转换操作的底层实现
 * 核心特点：属于窄依赖，每个子分区仅依赖一个父分区，分区数量与父 RDD 一致，可在同一 Stage 内流水线执行
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false,
    val preservesPartitionSizes: Boolean = false)
  extends RDD[U](prev) {

  /**
   * 分区器继承逻辑：如果转换不修改key，继承父RDD的分区器，否则不保留分区器
   * 对K-V类型RDD很重要，保留分区器可避免后续shuffle操作
   */
  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  /**
   * 获取分区数组：直接复用父RDD的分区信息，分区数量和结构保持不变
   * @return 父RDD的分区数组
   */
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * 计算指定分区：对父RDD对应分区的迭代器应用用户自定义转换函数
   * 惰性求值，不加载整个分区到内存，基于迭代器流式处理
   * @param split 待计算的分区
   * @param context 任务上下文
   * @return 转换后的输出迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  /**
   * 清除依赖引用：断开对父RDD的引用，帮助垃圾回收回收不再使用的内存
   */
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * 判断是否为Barrier阶段：当前RDD来自RDDBarrier，或者依赖的上游RDD是Barrier模式
   * Barrier模式用于分布式同步执行，比如深度学习分布式训练
   */
  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())

  /**
   * 获取输出确定性级别：判断当前RDD输出结果是否确定
   * 如果转换是顺序敏感，且父RDD输出顺序不确定，则当前结果也不确定
   * @return 输出确定性级别
   */
  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
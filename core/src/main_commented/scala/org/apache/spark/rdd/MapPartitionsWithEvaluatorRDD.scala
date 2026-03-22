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

import org.apache.spark.{Partition, PartitionEvaluatorFactory, TaskContext}

/**
 * 基于自定义分区评估器工厂实现的RDD，用于对父RDD的每个分区应用自定义评估逻辑
 * 为Spark核心计算提供可插拔的分区处理能力，支持自定义分区计算逻辑
 *
 * @param prev 父RDD，输入数据来源
 * @param evaluatorFactory 分区评估器工厂，用于创建每个任务的分区评估器
 * @param preservesPartitionSizes 是否保留原分区大小，用于分区输出大小保持不变的场景
 * @tparam T 父RDD元素类型
 * @tparam U 输出RDD元素类型
 */
private[spark] class MapPartitionsWithEvaluatorRDD[T : ClassTag, U : ClassTag](
    var prev: RDD[T],
    evaluatorFactory: PartitionEvaluatorFactory[T, U],
    val preservesPartitionSizes: Boolean = false)
  extends RDD[U](prev) {

  /**
   * 获取当前RDD的所有分区，直接复用父RDD的分区信息
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * 计算指定分区的数据，使用自定义评估器处理输入分区数据
   * @param split 待计算的分区
   * @param context 任务上下文
   * @return 处理后的输出元素迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    // 通过工厂创建分区评估器实例
    val evaluator = evaluatorFactory.createEvaluator()
    // 获取父分区的输入迭代器
    val input = firstParent[T].iterator(split, context)
    // 执行评估计算并返回结果
    evaluator.eval(split.index, input)
  }

  /**
   * 清除依赖引用，帮助GC回收不再使用的对象
   */
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}
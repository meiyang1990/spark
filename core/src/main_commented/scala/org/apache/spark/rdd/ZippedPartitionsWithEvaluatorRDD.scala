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
 * 使用自定义分区评估器对两个RDD按分区进行拉链操作的RDD实现
 * 核心职责：将两个RDD对应分区交给用户自定义评估器处理，生成结果分区
 * 仅Spark内部使用，用于支持自定义分区级拉链计算逻辑
 * 
 * @tparam T 输入两个RDD的元素类型
 * @tparam U 输出结果RDD的元素类型
 * @param rdd1 第一个待拉链的输入RDD
 * @param rdd2 第二个待拉链的输入RDD
 * @param evaluatorFactory 分区评估器工厂，用于创建处理分区的评估器实例
 */
private[spark] class ZippedPartitionsWithEvaluatorRDD[T : ClassTag, U : ClassTag](
    var rdd1: RDD[T],
    var rdd2: RDD[T],
    evaluatorFactory: PartitionEvaluatorFactory[T, U])
  extends ZippedPartitionsBaseRDD[U](rdd1.context, List(rdd1, rdd2)) {

  /**
   * 计算指定分区的数据，返回结果迭代器
   * 负责从两个父RDD获取对应分区迭代器，交给自定义评估器处理并返回结果
   * 
   * @param split 当前需要计算的分区
   * @param context 任务上下文信息
   * @return 计算结果元素的迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    // 通过工厂创建分区评估器实例
    val evaluator = evaluatorFactory.createEvaluator()
    // 获取当前拉链分区对应的两个父分区
    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    // 调用评估器处理两个分区，返回结果迭代器
    evaluator.eval(
      split.index,
      rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context))
  }
}
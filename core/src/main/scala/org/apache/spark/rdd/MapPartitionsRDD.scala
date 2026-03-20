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
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * MapPartitionsRDD 是 Spark 中最常用的 RDD 类型之一，
 * 几乎所有的转换操作（map、filter、flatMap 等）都会创建 MapPartitionsRDD。
 *
 * 特点：
 *   - 窄依赖（OneToOneDependency）：每个子分区只依赖一个父分区
 *   - 分区数与父 RDD 相同
 *   - 可以在同一个 Stage 内流水线执行
 *
 * 设计亮点：
 *   - 使用函数 f: (TaskContext, partitionIndex, Iterator[T]) => Iterator[U]
 *   - 支持惰性计算，不需要将整个分区数据加载到内存
 *   - 通过 preservesPartitioning 标记是否保持分区器（对 K-V 操作很重要）
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 *                              是否保持分区器（只有当函数不修改 key 时才为 true）
 * @param isFromBarrier Indicates whether this RDD is transformed from an RDDBarrier, a stage
 *                      containing at least one RDDBarrier shall be turned into a barrier stage.
 *                      是否来自 RDDBarrier（用于 Barrier 执行模式）
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 *                         函数是否对顺序敏感（有状态函数通常是顺序敏感的）
 * @param preservesPartitionSizes Whether the input function preserves the number of rows in each
 *                                partition. This is true for 1:1 element mappings like `map`.
 *                                Used to optimize `RDD.zipWithIndex` by counting rows on a
 *                                cheaper ancestor RDD instead of the immediate parent.
 *                                是否保持分区大小（用于优化 zipWithIndex）
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false,
    val preservesPartitionSizes: Boolean = false)
  extends RDD[U](prev) {

  // 如果函数保持分区特性（不修改 key），则继承父 RDD 的分区器
  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  // 分区与父 RDD 完全相同（窄依赖的特点）
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * 计算分区：对父 RDD 的迭代器应用转换函数
   * 这是 MapPartitionsRDD 的核心方法
   */
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  // 清除对父 RDD 的引用，允许垃圾回收
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  // 判断是否为 Barrier RDD（来自 RDDBarrier 或父 RDD 是 Barrier）
  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())

  /**
   * 计算输出的确定性级别
   * 如果函数是顺序敏感的，且父 RDD 输出无序，则结果是不确定的
   */
  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}

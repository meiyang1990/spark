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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

/**
 * 为ZippedWithIndexRDD定义的分区实现，保留父RDD分区信息并记录该分区起始索引
 * @param prev 父RDD对应的分区
 * @param startIndex 该分区内第一个元素的全局起始索引
 */
private[spark]
class ZippedWithIndexRDDPartition(val prev: Partition, val startIndex: Long)
  extends Partition with Serializable {
  override val index: Int = prev.index
}

/**
 * 为父RDD的每个元素附加全局索引生成新RDD，分区顺序和分区内元素顺序共同决定索引值。
 * 第一个分区第一个元素索引为0，后续元素依次递增，跨分区连续编号。
 * 
 * @param prev 待添加索引的父RDD
 * @tparam T 父RDD元素类型
 */
private[spark]
class ZippedWithIndexRDD[T: ClassTag](prev: RDD[T]) extends RDD[(T, Long)](prev) {

  /**
   * 递归向上查找保留分区大小且可直接获取分区大小的祖先RDD，避免重复计算
   * @param rdd 当前递归查找的RDD
   * @return 符合条件的祖先RDD，若无则返回当前RDD
   */
  private def getAncestorWithSamePartitionSizes(rdd: RDD[_]): RDD[_] = {
    rdd match {
      case c: RDD[_] if c.isCheckpointed || c.getStorageLevel != StorageLevel.NONE => c
      case m: MapPartitionsRDD[_, _] if m.preservesPartitionSizes =>
        getAncestorWithSamePartitionSizes(m.prev)
      case m: MapPartitionsWithEvaluatorRDD[_, _] if m.preservesPartitionSizes =>
        getAncestorWithSamePartitionSizes(m.prev)
      case _ => rdd
    }
  }

  /** 存储每个分区的全局起始索引 */
  @transient private val startIndices: Array[Long] = {
    val n = prev.partitions.length
    if (n == 0) {
      // 无分区时返回空数组
      Array.empty
    } else if (n == 1) {
      // 单分区起始索引固定为0
      Array(0L)
    } else {
      // 查找可复用分区大小的祖先RDD，减少计算
      val ancestor = getAncestorWithSamePartitionSizes(prev)
      // 分布式计算前n-1个分区的元素数量，累加得到每个分区的起始索引
      ancestor.context.runJob(
        ancestor,
        Utils.getIteratorSize _,
        0 until n - 1 // 不需要统计最后一个分区的大小
      ).scanLeft(0L)(_ + _)
    }
  }

  /**
   * 生成ZippedWithIndexRDD的分区列表，包装父RDD分区并关联起始索引
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = {
    firstParent[T].partitions.map(x => new ZippedWithIndexRDDPartition(x, startIndices(x.index)))
  }

  /**
   * 获取分区的偏好位置，继承父RDD分区的位置偏好
   * @param split 当前ZippedWithIndexRDD分区
   * @return 偏好位置列表
   */
  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[ZippedWithIndexRDDPartition].prev)

  /**
   * 计算当前分区元素，为每个元素附加全局索引
   * @param splitIn 当前分区
   * @param context 任务上下文
   * @return 元素与索引的二元组迭代器
   */
  override def compute(splitIn: Partition, context: TaskContext): Iterator[(T, Long)] = {
    val split = splitIn.asInstanceOf[ZippedWithIndexRDDPartition]
    val parentIter = firstParent[T].iterator(split.prev, context)
    Utils.getIteratorZipWithIndex(parentIter, split.startIndex)
  }
}
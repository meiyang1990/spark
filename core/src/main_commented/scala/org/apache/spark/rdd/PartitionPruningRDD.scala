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

import org.apache.spark.{NarrowDependency, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi

/**
 * PartitionPruningRDD使用的分区实现，包装父RDD中保留的原始分区
 * @param idx 当前RDD中的分区索引
 * @param parentSplit 父RDD对应的原始分区
 */
private[spark] class PartitionPruningRDDPartition(idx: Int, val parentSplit: Partition)
  extends Partition {
  override val index = idx
}


/**
 * PartitionPruningRDD到父RDD的依赖实现，仅保留父RDD中满足过滤条件的分区
 * @tparam T 父RDD元素类型
 * @param rdd 父RDD
 * @param partitionFilterFunc 分区过滤函数，输入为父分区索引，输出true表示保留该分区
 */
private[spark] class PruneDependency[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean)
  extends NarrowDependency[T](rdd) {

  @transient
  // 过滤父RDD分区，生成当前RDD的分区列表，保持与原父分区的映射关系
  val partitions: Array[Partition] = rdd.partitions
    .filter(s => partitionFilterFunc(s.index)).zipWithIndex
    .map { case(split, idx) => new PartitionPruningRDDPartition(idx, split) : Partition }

  // 获取当前分区对应的父分区索引
  override def getParents(partitionId: Int): List[Int] = {
    List(partitions(partitionId).asInstanceOf[PartitionPruningRDDPartition].parentSplit.index)
  }
}


/**
 * :: DeveloperApi ::
 * 用于对父RDD进行分区裁剪的RDD实现，通过过滤不包含所需数据的分区，避免在这些分区上启动计算任务
 * 典型应用场景：对于按范围分区的RDD，当查询带有范围过滤条件时，可以裁剪掉不包含查询范围的分区，减少任务执行量
 * @tparam T RDD元素类型
 * @param prev 父RDD
 * @param partitionFilterFunc 分区过滤函数，输入为父分区索引，返回true表示保留该分区，false表示裁剪
 */
@DeveloperApi
class PartitionPruningRDD[T: ClassTag](
    prev: RDD[T],
    partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

  /**
   * 计算当前分区的数据，直接委托给父RDD对应原始分区进行计算
   * @param split 当前裁剪后的分区
   * @param context 任务上下文
   * @return 分区数据迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(
      split.asInstanceOf[PartitionPruningRDDPartition].parentSplit, context)
  }

  /**
   * 获取裁剪后剩余的分区列表
   * @return 分区数组
   */
  override protected def getPartitions: Array[Partition] =
    dependencies.head.asInstanceOf[PruneDependency[T]].partitions
}


/**
 * :: DeveloperApi ::
 * PartitionPruningRDD的辅助创建工具，用于编译时无法确定元素类型T的场景
 */
@DeveloperApi
object PartitionPruningRDD {

  /**
   * 创建PartitionPruningRDD实例，适用于编译时泛型类型未知的场景
   * @param rdd 待裁剪的父RDD
   * @param partitionFilterFunc 分区过滤函数
   * @tparam T RDD元素类型
   * @return 裁剪后的PartitionPruningRDD实例
   */
  def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean): PartitionPruningRDD[T] = {
    new PartitionPruningRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
  }
}
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
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * UnionRDD的分区实现
 * 
 * 每个UnionPartition对应一个父RDD中的某个分区，保持父分区的位置信息
 * @param idx 本分区在UnionRDD中的索引
 * @param rdd 本分区所属的父RDD
 * @param parentRddIndex 父RDD在UnionRDD的父RDD列表中的索引
 * @param parentRddPartitionIndex 本分区对应父RDD中的分区索引
 */
private[spark] class UnionPartition[T: ClassTag](
    idx: Int,
    @transient private val rdd: RDD[T],
    val parentRddIndex: Int,
    @transient private val parentRddPartitionIndex: Int)
  extends Partition {

  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex)

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 序列化时重新获取父分区引用，保证状态最新
    parentPartition = rdd.partitions(parentRddPartitionIndex)
    oos.defaultWriteObject()
  }
}

/**
 * UnionRDD的伴生对象，提供全局并行计算线程池
 */
object UnionRDD {
  // 用于多父RDD场景下并行计算分区信息的ForkJoin线程池
  private[spark] lazy val partitionEvalTaskSupport =
    new ForkJoinTaskSupport(ThreadUtils.newForkJoinPool("partition-eval-task-support", 8))
}

/**
 * 代表多个RDD合并结果的RDD，实现union操作
 * 
 * 将多个RDD合并为一个逻辑RDD，不做实际数据移动，直接复用父RDD的分区，是轻量级操作
 * 总分区数为所有父RDD分区数之和，保持原有分区的位置偏好
 * 
 * @param sc Spark上下文实例
 * @param rdds 需要合并的RDD列表
 */
@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  // visible for testing
  // 标识是否需要并行计算分区列表，当父RDD数量超过配置阈值时启用
  private[spark] val isPartitionListingParallel: Boolean =
    rdds.length > conf.get(RDD_PARALLEL_LISTING_THRESHOLD)

  /**
   * 生成UnionRDD的所有分区
   * @return 分区数组，每个元素对应一个父RDD中的分区
   */
  override def getPartitions: Array[Partition] = {
    // 父RDD数量超过阈值时使用并行集合加速分区创建
    val parRDDs = if (isPartitionListingParallel) {
      // scalastyle:off parvector
      val parArray = new ParVector(rdds.toVector)
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      // scalastyle:on parvector
      parArray
    } else {
      rdds
    }
    // 提前计算总分区数，初始化结果数组
    val array = new Array[Partition](parRDDs.iterator.map(_.partitions.length).sum)
    var pos = 0
    // 遍历所有父RDD及其分区，逐个创建UnionPartition
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  /**
   * 生成UnionRDD对所有父RDD的依赖关系
   * @return 依赖列表，每个父RDD对应一个RangeDependency
   */
  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      // 每个父RDD的分区在当前RDD中占用连续的索引区间，使用RangeDependency描述映射关系
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps.toSeq
  }

  /**
   * 计算当前分区的数据
   * @param s 当前UnionRDD分区
   * @param context 任务上下文
   * @return 分区数据迭代器，直接复用父RDD分区的迭代器
   */
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  /**
   * 获取分区的首选位置，直接复用父分区的位置偏好
   * @param s 当前UnionRDD分区
   * @return 首选位置列表
   */
  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  /**
   * 清理依赖引用，帮助垃圾回收
   */
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
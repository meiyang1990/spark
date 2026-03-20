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
 * Partition for UnionRDD.
 *
 * @param idx index of the partition
 * @param rdd the parent RDD this partition refers to
 * @param parentRddIndex index of the parent RDD this partition refers to
 * @param parentRddPartitionIndex index of the partition within the parent RDD
 *                                this partition refers to
 *
 * UnionPartition —— UnionRDD 的分区类
 *
 * 【分区结构】
 * UnionRDD 的分区是所有父 RDD 分区的并集。
 * 每个 UnionPartition 对应某个父 RDD 的某个分区。
 *
 * 例如：
 * RDD1 有 3 个分区 [0, 1, 2]
 * RDD2 有 2 个分区 [0, 1]
 * UnionRDD 有 5 个分区：
 *   - 分区 0 → RDD1 的分区 0
 *   - 分区 1 → RDD1 的分区 1
 *   - 分区 2 → RDD1 的分区 2
 *   - 分区 3 → RDD2 的分区 0
 *   - 分区 4 → RDD2 的分区 1
 *
 * @param idx UnionRDD 中的分区索引
 * @param rdd 对应的父 RDD（transient，不序列化）
 * @param parentRddIndex 父 RDD 在 rdds 数组中的索引
 * @param parentRddPartitionIndex 在父 RDD 中的分区索引（transient）
 */
private[spark] class UnionPartition[T: ClassTag](
    idx: Int,
    @transient private val rdd: RDD[T],
    val parentRddIndex: Int,
    @transient private val parentRddPartitionIndex: Int)
  extends Partition {

  // 父 RDD 中的实际分区对象
  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex)

  // 获取首选位置：代理到父 RDD 的 preferredLocations
  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    // 序列化时更新父分区引用，确保获取最新信息
    parentPartition = rdd.partitions(parentRddPartitionIndex)
    oos.defaultWriteObject()
  }
}

/**
 * UnionRDD 伴生对象
 *
 * 提供并行计算分区信息的线程池支持
 */
object UnionRDD {
  // 用于并行计算分区列表的线程池（当 RDD 数量超过阈值时使用）
  private[spark] lazy val partitionEvalTaskSupport =
    new ForkJoinTaskSupport(ThreadUtils.newForkJoinPool("partition-eval-task-support", 8))
}

/**
 * UnionRDD —— 多个 RDD 的合并操作
 *
 * 【核心特性】
 * 1. 窄依赖：UnionRDD 与所有父 RDD 都是 RangeDependency（窄依赖）
 *    - 无需 Shuffle，直接读取父 RDD 的对应分区
 *    - 这使得 union 成为 Spark 中最轻量的操作之一
 *
 * 2. 分区数 = 所有父 RDD 分区数之和
 *
 * 【使用示例】
 * val rdd1 = sc.parallelize(1 to 3)
 * val rdd2 = sc.parallelize(4 to 6)
 * val unionRDD = rdd1.union(rdd2)  // 结果包含 1 到 6
 *
 * 【注意事项】
 * - union 不会去重，如需去重请使用 union.distinct()
 * - union 不会改变分区数，只是简单合并
 *
 * 【RangeDependency 原理】
 * 每个父 RDD 的分区在 UnionRDD 中占据一段连续的索引范围：
 * RangeDependency(rdd, 0, pos, rdd.partitions.length)
 * 表示父 RDD 的分区 [0, length) 映射到 UnionRDD 的分区 [pos, pos+length)
 *
 * @param sc SparkContext
 * @param rdds 要合并的 RDD 序列
 */
@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  // visible for testing
  // 当父 RDD 数量超过阈值时，并行计算分区列表以提高效率
  private[spark] val isPartitionListingParallel: Boolean =
    rdds.length > conf.get(RDD_PARALLEL_LISTING_THRESHOLD)

  /**
   * getPartitions —— 创建分区数组
   *
   * 将所有父 RDD 的分区依次编号，创建对应的 UnionPartition
   */
  override def getPartitions: Array[Partition] = {
    // 如果 RDD 数量很多，使用并行集合加速分区计算
    val parRDDs = if (isPartitionListingParallel) {
      // scalastyle:off parvector
      val parArray = new ParVector(rdds.toVector)
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      // scalastyle:on parvector
      parArray
    } else {
      rdds
    }
    // 计算所有父 RDD 的分区总数，分配结果数组
    val array = new Array[Partition](parRDDs.iterator.map(_.partitions.length).sum)
    var pos = 0
    // 遍历所有父 RDD 及其分区，创建 UnionPartition
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  /**
   * getDependencies —— 构建依赖关系
   *
   * 【依赖类型：RangeDependency】
   * 每个父 RDD 对应一个 RangeDependency：
   * - 父 RDD 的分区 [0, length) 映射到 UnionRDD 的分区 [pos, pos+length)
   * - 这是窄依赖，compute 时直接读取父 RDD 的对应分区
   */
  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      // RangeDependency(父RDD, 父RDD起始分区, 子RDD起始分区, 分区数量)
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps.toSeq
  }

  /**
   * compute —— 计算分区数据
   *
   * 直接代理到对应父 RDD 的对应分区，非常简单高效
   */
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    // parent[T](idx) 获取第 idx 个父 RDD，然后调用其 iterator
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  /**
   * getPreferredLocations —— 获取首选位置
   *
   * 代理到对应的 UnionPartition 的 preferredLocations 方法
   */
  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  // 清理依赖，帮助 GC
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

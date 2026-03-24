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

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.util.Utils

/**
 * 多RDD按分区拉链对应的分区实现类，保存多个父RDD对应索引的分区信息
 * @param idx 当前分区索引
 * @param rdds 参与拉链的所有父RDD列表
 * @param preferredLocations 当前分区的最优位置列表
 */
private[spark] class ZippedPartitionsPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]],
    @transient val preferredLocations: Seq[String])
  extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))
  def partitions: Seq[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 序列化任务时更新父分区引用，保证序列化时拿到最新分区信息
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    oos.defaultWriteObject()
  }
}

/**
 * 多RDD按分区拉链的基类RDD，实现公共的分区划分和位置选择逻辑
 * 要求所有参与拉链的RDD分区数量必须一致，按索引对应拉链分区
 * @tparam V 输出元素类型
 * @param sc Spark上下文
 * @param rdds 参与拉链的父RDD列表
 * @param preservesPartitioning 是否保留父RDD的分区器
 */
private[spark] abstract class ZippedPartitionsBaseRDD[V: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[_]],
    preservesPartitioning: Boolean = false)
  extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

  override val partitioner =
    if (preservesPartitioning) firstParent[Any].partitioner else None

  override def getPartitions: Array[Partition] = {
    // 获取第一个RDD的分区数，要求所有RDD分区数必须一致
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
    }
    // 为每个分区索引创建对应的ZippedPartitionsPartition
    Array.tabulate[Partition](numParts) { i =>
      // 收集每个RDD对应分区的最优位置
      val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i)))
      // 优先取所有分区位置交集，保证数据本地化；如果无交集则取所有位置的并集去重
      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
      new ZippedPartitionsPartition(i, rdds, locs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    // 返回预计算好的最优位置列表
    s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    // 帮助GC回收不再使用的RDD引用
    rdds = null
  }
}

/**
 * 两个RDD按分区拉链的RDD实现，对每个索引对应的两个分区应用用户自定义函数生成输出
 * @tparam A 第一个RDD元素类型
 * @tparam B 第二个RDD元素类型
 * @tparam V 输出元素类型
 * @param sc Spark上下文
 * @param f 用户自定义分区拉链函数，输入两个父分区迭代器，输出结果迭代器
 * @param rdd1 第一个父RDD
 * @param rdd2 第二个父RDD
 * @param preservesPartitioning 是否保留分区器
 */
private[spark] class ZippedPartitionsRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

/**
 * 三个RDD按分区拉链的RDD实现
 * @tparam A 第一个RDD元素类型
 * @tparam B 第二个RDD元素类型
 * @tparam C 第三个RDD元素类型
 * @tparam V 输出元素类型
 * @param sc Spark上下文
 * @param f 用户自定义分区拉链函数，输入三个父分区迭代器，输出结果迭代器
 * @param rdd1 第一个父RDD
 * @param rdd2 第二个父RDD
 * @param rdd3 第三个父RDD
 * @param preservesPartitioning 是否保留分区器
 */
private[spark] class ZippedPartitionsRDD3
  [A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    f = null
  }
}

/**
 * 四个RDD按分区拉链的RDD实现
 * @tparam A 第一个RDD元素类型
 * @tparam B 第二个RDD元素类型
 * @tparam C 第三个RDD元素类型
 * @tparam D 第四个RDD元素类型
 * @tparam V 输出元素类型
 * @param sc Spark上下文
 * @param f 用户自定义分区拉链函数，输入四个父分区迭代器，输出结果迭代器
 * @param rdd1 第一个父RDD
 * @param rdd2 第二个父RDD
 * @param rdd3 第三个父RDD
 * @param rdd4 第四个父RDD
 * @param preservesPartitioning 是否保留分区器
 */
private[spark] class ZippedPartitionsRDD4
  [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    var rdd4: RDD[D],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3, rdd4), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context),
      rdd4.iterator(partitions(3), context))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    rdd4 = null
    f = null
  }
}
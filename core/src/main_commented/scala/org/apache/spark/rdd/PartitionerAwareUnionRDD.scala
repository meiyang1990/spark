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
 * 分区感知联合RDD的分区实现，维护所有父RDD对应索引分区的引用
 */
private[spark]
class PartitionerAwareUnionRDDPartition(
    @transient val rdds: Seq[RDD[_]],
    override val index: Int
  ) extends Partition {
  var parents = rdds.map(_.partitions(index)).toArray

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 任务序列化时更新父分区引用，确保获取最新分区信息
    parents = rdds.map(_.partitions(index)).toArray
    oos.defaultWriteObject()
  }
}

/**
 * 分区感知联合RDD的抽象基类，封装多个父RDD按相同索引合并分区的通用逻辑
 * @tparam T RDD元素类型
 */
private[spark]
abstract class PartitionerAwareUnionRDDBase[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]
  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.nonEmpty, "RDDs cannot be empty")

  override val partitioner = rdds.head.partitioner

  override def getPartitions: Array[Partition] = {
    val numPartitions = partitioner.get.numPartitions
    (0 until numPartitions).map { index =>
      new PartitionerAwareUnionRDDPartition(rdds, index)
    }.toArray
  }

  /**
   * 计算当前分区的最优位置，选择所有父RDD对应分区中出现次数最多的节点位置
   */
  override def getPreferredLocations(s: Partition): Seq[String] = {
    logDebug("Finding preferred location for " + this + ", partition " + s.index)
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) =>
        // 获取当前最新的优先位置（从DAGScheduler获取，非静态缓存）
        val parentLocations = currPrefLocs(rdd, part)
        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
        parentLocations
    }
    val location = if (locations.isEmpty) {
      None
    } else {
      // 找到出现次数最多的位置作为当前分区的优先位置
      Some(locations.groupBy(x => x).maxBy(_._2.length)._1)
    }
    logDebug("Selected location for " + this + ", partition " + s.index + " = " + location)
    location.toSeq
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

  /**
   * 从DAGScheduler获取当前最新的分区优先位置，而非使用静态缓存的位置
   */
  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
  }
}

/**
 * 分区感知联合RDD，将多个使用相同分区器划分的RDD合并为一个RDD，同时保留原有分区结构。
 * 每个合并后分区对应所有父RDD的相同索引分区，分区优先位置选择父分区中最常见的节点位置，
 * 能够减少数据移动，提升计算局部性。
 */
private[spark]
class PartitionerAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var _rdds: Seq[RDD[T]]
  ) extends PartitionerAwareUnionRDDBase(sc, _rdds) {
  require(_rdds.forall(_.partitioner.isDefined))
  require(_rdds.flatMap(_.partitioner).toSet.size == 1,
    "Parent RDDs have different partitioners: " + _rdds.flatMap(_.partitioner))
}

/**
 * 适配SQL场景的分区感知联合RDD，与[[PartitionerAwareUnionRDD]]类似，但不强制要求
 * 所有父RDD都定义相同分区器。
 * 该设计用于满足Spark SQL中ShuffledRowRDD未明确定义分区器，但实际分区已经一致的场景，
 * 实际分区正确性由调用方通过检查SQL输出分区保证。
 */
private[spark]
class SQLPartitioningAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var _rdds: Seq[RDD[T]],
    val numPartitions: Int
  ) extends PartitionerAwareUnionRDDBase(sc, _rdds) {
  require(partitioner.isEmpty || partitioner.get.numPartitions == numPartitions,
    "Partitioner of parent RDDs does not match the number of partitions: " +
      s"expected $numPartitions, but got ${partitioner.map(_.numPartitions).getOrElse("none")}")

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { index =>
      new PartitionerAwareUnionRDDPartition(_rdds, index)
    }.toArray
  }
}
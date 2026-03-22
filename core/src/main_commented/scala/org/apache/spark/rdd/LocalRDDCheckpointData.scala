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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * 基于Spark缓存层实现的RDD本地检查点实现类
 *
 * 本地检查点通过跳过将RDD数据写入可靠容错存储的昂贵步骤，用容错能力换取性能提升。
 * 检查点数据会被写入执行器节点的本地临时块存储，适用于需要频繁截断长血缘 lineage 的场景（例如GraphX图计算）。
 */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
   * 执行本地检查点，确保RDD所有分区都已缓存，后续可从缓存恢复
   * @return 完成检查点后的新CheckpointRDD
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel

    // 要求存储级别必须使用磁盘，否则内存驱逐可能导致数据丢失
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // 不是所有动作都会计算RDD的所有分区（例如take操作），为了正确性，必须计算并缓存所有未计算的缺失分区
    // TODO: 避免在此处额外提交一个作业（SPARK-8582）
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    // 筛选出尚未存储在块管理器中的分区索引
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    // 如果存在缺失分区，提交作业计算这些分区并缓存
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices.toImmutableArraySeq)
    }

    // 返回基于本地缓存的检查点RDD
    new LocalCheckpointRDD[T](rdd)
  }

}

/**
 * 本地RDD检查点工具类，提供存储级别转换功能
 */
private[spark] object LocalRDDCheckpointData {

  // 默认存储级别：内存和磁盘共存
  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
   * 将输入存储级别转换为强制使用磁盘的存储级别，保证本地检查点数据不会因内存驱逐丢失
   *
   * 只要执行器不宕机，就能保证RDD可以多次正确重新计算。如果只使用内存缓存，相关块被驱逐后检查点数据就会丢失。
   * 该方法是幂等操作，多次调用结果一致。
   *
   * @param level 输入的原始存储级别
   * @return 强制开启磁盘使用的新存储级别
   */
  def transformStorageLevel(level: StorageLevel): StorageLevel = {
    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}
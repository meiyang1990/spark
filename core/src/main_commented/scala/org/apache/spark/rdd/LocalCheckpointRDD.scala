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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.storage.RDDBlockId

/**
 * 文件说明: 本地检查点RDD实现，仅作为占位符用于在本地检查点缓存丢失时提供清晰的错误信息
 * 
 * 本地检查点是将RDD分区缓存存储在执行器本地内存/磁盘，不会写入可靠的检查点存储。
 * 正常情况下原始RDD分区已经缓存，不会需要计算该RDD。只有当执行器故障或用户手动取消缓存时才会触发计算。
 * 本类仅负责在这种异常场景下抛出带有明确信息的错误，帮助用户定位问题。
 *
 * @param sc 活动的Spark上下文实例
 * @param rddId 被本地检查点处理的原RDD ID
 * @param numPartitions 原RDD的分区数量
 */
private[spark] class LocalCheckpointRDD[T: ClassTag](
    sc: SparkContext,
    rddId: Int,
    numPartitions: Int)
  extends CheckpointRDD[T](sc) {

  /**
   * 从原RDD构造LocalCheckpointRDD
   * @param rdd 需要做本地检查点的原RDD
   */
  def this(rdd: RDD[T]) = {
    this(rdd.context, rdd.id, rdd.partitions.length)
  }

  /**
   * 生成LocalCheckpointRDD的分区列表，对应原RDD的每个分区
   * @return 分区数组，每个分区对应原RDD的一个分区
   */
  protected override def getPartitions: Array[Partition] = {
    (0 until numPartitions).toArray.map { i => new CheckpointRDDPartition(i) }
  }

  /**
   * 计算指定分区，本方法仅在缓存丢失时被调用，直接抛出明确的错误信息
   *
   * 正常场景下原RDD分区已经被缓存，不会走到这个分支。只有当原RDD被用户手动unpersist，
   * 或者存储分区的执行器故障退出时，才会尝试从检查点恢复，此时就会调用本方法抛出错误。
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    throw SparkCoreErrors.checkpointRDDBlockIdNotFoundError(RDDBlockId(rddId, partition.index))
  }

}
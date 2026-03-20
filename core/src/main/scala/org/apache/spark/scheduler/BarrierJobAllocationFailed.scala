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
// 这个文件已经全部加上中文注释

package org.apache.spark.scheduler

import org.apache.spark.SparkException
import org.apache.spark.internal.config.DYN_ALLOCATION_ENABLED

/**
 * 提交包含Barrier Stage的作业时，未通过必要检查时抛出的异常。
 */
private[spark] class BarrierJobAllocationFailed(message: String) extends SparkException(message)

/**
 * Barrier Stage中使用了不支持的RDD链模式时抛出的异常。
 */
private[spark] class BarrierJobUnsupportedRDDChainException
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)

/**
 * 在启用动态资源分配的情况下运行Barrier Stage时抛出的异常。
 */
private[spark] class BarrierJobRunWithDynamicAllocationException
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION)

/**
 * Barrier Stage所需的槽位数超过集群当前可用总槽位数时抛出的异常。
 * @param requiredConcurrentTasks 所需的并发任务数
 * @param maxConcurrentTasks 集群当前最大并发任务数
 */
private[spark] class BarrierJobSlotsNumberCheckFailed(
    val requiredConcurrentTasks: Int,
    val maxConcurrentTasks: Int)
  extends BarrierJobAllocationFailed(
    BarrierJobAllocationFailed.ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)

private[spark] object BarrierJobAllocationFailed {

  // Barrier Stage使用不支持的RDD链模式时的错误信息
  val ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN =
    "[SPARK-24820][SPARK-24821]: Barrier execution mode does not allow the following pattern of " +
      "RDD chain within a barrier stage:\n1. Ancestor RDDs that have different number of " +
      "partitions from the resulting RDD (e.g. union()/coalesce()/first()/take()/" +
      "PartitionPruningRDD). A workaround for first()/take() can be barrierRdd.collect().head " +
      "(scala) or barrierRdd.collect()[0] (python).\n" +
      "2. An RDD that depends on multiple barrier RDDs (e.g. barrierRdd1.zip(barrierRdd2))."

  // 在启用动态资源分配时运行Barrier Stage的错误信息
  val ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION =
    "[SPARK-24942]: Barrier execution mode does not support dynamic resource allocation for " +
      "now. You can disable dynamic resource allocation by setting Spark conf " +
      s""""${DYN_ALLOCATION_ENABLED.key}" to "false"."""

  // Barrier Stage所需槽位数超过当前集群总数时的错误信息
  val ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER =
    "[SPARK-24819]: Barrier execution mode does not allow run a barrier stage that requires " +
      "more slots than the total number of slots in the cluster currently. Please init a new " +
      "cluster with more resources(e.g. CPU, GPU) or repartition the input RDD(s) to reduce " +
      "the number of slots required to run this barrier stage."
}

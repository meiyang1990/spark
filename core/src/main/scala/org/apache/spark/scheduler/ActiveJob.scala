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

package org.apache.spark.scheduler

import java.util.Properties

import org.apache.spark.JobArtifactSet
import org.apache.spark.util.CallSite

/**
 * DAGScheduler中正在运行的作业。作业分为两种类型：
 * 1. 结果作业（result job）：计算一个ResultStage来执行action操作
 * 2. Map阶段作业（map-stage job）：在下游Stage提交之前计算ShuffleMapStage的Map输出，
 *    用于自适应查询规划，在提交后续Stage之前查看Map输出统计信息
 *
 * 通过本类的finalStage字段来区分这两种类型的作业。
 *
 * 作业仅跟踪客户端通过DAGScheduler的submitJob或submitMapStage方法直接提交的"叶子"Stage。
 * 但是，任何类型的作业都可能导致其他前置Stage的执行（因为DAG中的RDD依赖），
 * 多个作业可能共享某些前置Stage。这些依赖关系在DAGScheduler内部管理。
 *
 * @param jobId 作业的唯一ID
 * @param finalStage 此作业计算的Stage（对于action是ResultStage，对于submitMapStage是ShuffleMapStage）
 * @param callSite 作业在用户程序中的发起位置（显示在UI上）
 * @param listener 作业中任务完成或作业失败时的通知监听器
 * @param artifacts 此作业可能使用的Artifact集合
 * @param properties 附加到作业的调度属性，如公平调度器池名称
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val callSite: CallSite,
    val listener: JobListener,
    val artifacts: JobArtifactSet,
    val properties: Properties) {

  /**
   * 此作业需要计算的分区数。注意：对于first()和lookup()等action操作，
   * ResultStage可能不需要计算目标RDD的所有分区。
   */
  val numPartitions = finalStage match {
    case r: ResultStage => r.partitions.length
    case m: ShuffleMapStage => m.numPartitions
  }

  /** 记录Stage中哪些分区已经完成 */
  val finished = Array.fill[Boolean](numPartitions)(false)

  /** 已完成的分区数量 */
  var numFinished = 0
}

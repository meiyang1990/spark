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

package org.apache.spark

import java.util.Arrays

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.util.Utils

/**
 * Job 和 Stage 进度的低级状态查询 API。
 *
 * 这些 API 提供弱一致性语义，使用者应做好处理空/缺失信息的准备。
 * 例如 Job 的 Stage ID 可能已知，但状态 API 可能还没有这些 Stage 的详情。
 *
 * 为限制内存使用，仅提供最近 spark.ui.retainedStages 个 Stage
 * 和 spark.ui.retainedJobs 个 Job 的信息。
 */
class SparkStatusTracker private[spark] (sc: SparkContext, store: AppStatusStore) {

  /**
   * 获取指定 Job Group 中所有已知 Job 的 ID 列表。
   * jobGroup 为 null 时返回不属于任何 Job Group 的 Job。
   * 返回结果可能包含运行中、失败和已完成的 Job。
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = {
    val expected = Option(jobGroup)
    store.jobsList(null).filter(_.jobGroup == expected).map(_.jobId).toArray
  }

  /** 获取带有指定 Tag 的所有已知 Job 的 ID 列表 */
  def getJobIdsForTag(jobTag: String): Array[Int] = {
    store.jobsList(null).filter(_.jobTags.contains(jobTag)).map(_.jobId).toArray
  }

  /** 获取所有活跃 Stage 的 ID 数组 */
  def getActiveStageIds(): Array[Int] = {
    store.stageList(Arrays.asList(StageStatus.ACTIVE)).map(_.stageId).toArray
  }

  /** 获取所有活跃 Job 的 ID 数组 */
  def getActiveJobIds(): Array[Int] = {
    store.jobsList(Arrays.asList(JobExecutionStatus.RUNNING)).map(_.jobId).toArray
  }

  /** 获取指定 Job 的信息，找不到或已被 GC 回收时返回 None */
  def getJobInfo(jobId: Int): Option[SparkJobInfo] = {
    store.asOption(store.job(jobId)).map { job =>
      new SparkJobInfoImpl(jobId, job.stageIds.toArray, job.status)
    }
  }

  /** 获取指定 Stage 最近一次尝试的信息，找不到时返回 None */
  def getStageInfo(stageId: Int): Option[SparkStageInfo] = {
    store.asOption(store.lastStageAttempt(stageId)).map { stage =>
      new SparkStageInfoImpl(
        stageId,
        stage.attemptId,
        stage.submissionTime.map(_.getTime()).getOrElse(0L),
        stage.name,
        stage.numTasks,
        stage.numActiveTasks,
        stage.numCompleteTasks,
        stage.numFailedTasks)
    }
  }

  /**
   * 获取所有已知 Executor 的信息（包括 Driver），
   * 包含主机、端口、缓存大小、运行任务数和内存指标。
   */
  def getExecutorInfos: Array[SparkExecutorInfo] = {
    store.executorList(true).map { exec =>
      val (host, port) = Utils.parseHostPort(exec.hostPort)
      val cachedMem = exec.memoryMetrics.map { mem =>
        mem.usedOnHeapStorageMemory + mem.usedOffHeapStorageMemory
      }.getOrElse(0L)

      new SparkExecutorInfoImpl(
        host,
        port,
        cachedMem,
        exec.activeTasks,
        exec.memoryMetrics.map(_.usedOnHeapStorageMemory).getOrElse(0L),
        exec.memoryMetrics.map(_.usedOffHeapStorageMemory).getOrElse(0L),
        exec.memoryMetrics.map(_.totalOnHeapStorageMemory).getOrElse(0L),
        exec.memoryMetrics.map(_.totalOffHeapStorageMemory).getOrElse(0L))
    }.toArray
  }
}

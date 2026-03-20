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

import scala.concurrent.Promise

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, CallSite}

/**
 * DAGScheduler可以处理的事件类型。DAGScheduler使用事件队列架构，
 * 任何线程都可以发布事件（如任务完成或新作业提交），
 * 但只有一个"逻辑"线程读取这些事件并做出决策，这大大简化了同步问题。
 */
private[scheduler] sealed trait DAGSchedulerEvent

/** 在目标RDD上提交了一个产生结果的作业 */
private[scheduler] case class JobSubmitted(
    jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    artifactSet: JobArtifactSet,
    properties: Properties = null)
  extends DAGSchedulerEvent

/** 作为独立作业提交了一个Map Stage */
private[scheduler] case class MapStageSubmitted(
  jobId: Int,
  dependency: ShuffleDependency[_, _, _],
  callSite: CallSite,
  listener: JobListener,
  artifactSet: JobArtifactSet,
  properties: Properties = null)
  extends DAGSchedulerEvent

/** 取消指定Stage */
private[scheduler] case class StageCancelled(
    stageId: Int,
    reason: Option[String])
  extends DAGSchedulerEvent

/** 取消指定作业 */
private[scheduler] case class JobCancelled(
    jobId: Int,
    reason: Option[String])
  extends DAGSchedulerEvent

/** 取消指定作业组 */
private[scheduler] case class JobGroupCancelled(
    groupId: String,
    cancelFutureJobs: Boolean = false,
    reason: Option[String])
  extends DAGSchedulerEvent

/** 按标签取消作业 */
private[scheduler] case class JobTagCancelled(
    tagName: String,
    reason: Option[String],
    cancelledJobs: Option[Promise[Seq[ActiveJob]]]) extends DAGSchedulerEvent

/** 取消所有作业 */
private[scheduler] case object AllJobsCancelled extends DAGSchedulerEvent

/** 清理指定查询的作业 */
private[scheduler] case class CleanupQueryJobs(executionId: Long) extends DAGSchedulerEvent

/** 任务开始执行事件 */
private[scheduler]
case class BeginEvent(task: Task[_], taskInfo: TaskInfo) extends DAGSchedulerEvent

/** 正在获取任务结果的事件 */
private[scheduler]
case class GettingResultEvent(taskInfo: TaskInfo) extends DAGSchedulerEvent

/** 任务完成事件，包含结果、累加器更新和度量峰值 */
private[scheduler] case class CompletionEvent(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Seq[AccumulatorV2[_, _]],
    metricPeaks: Array[Long],
    taskInfo: TaskInfo)
  extends DAGSchedulerEvent

/** 新增Executor事件 */
private[scheduler] case class ExecutorAdded(execId: String, host: String) extends DAGSchedulerEvent

/** Executor丢失事件 */
private[scheduler] case class ExecutorLost(execId: String, reason: ExecutorLossReason)
  extends DAGSchedulerEvent

/** Worker被移除事件 */
private[scheduler] case class WorkerRemoved(workerId: String, host: String, message: String)
  extends DAGSchedulerEvent

/** Stage失败事件 */
private[scheduler]
case class StageFailed(stageId: Int, reason: String, exception: Option[Throwable])
  extends DAGSchedulerEvent

/** TaskSet失败事件 */
private[scheduler]
case class TaskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable])
  extends DAGSchedulerEvent

/** 重新提交失败的Stage */
private[scheduler] case object ResubmitFailedStages extends DAGSchedulerEvent

/** 推测执行任务提交事件 */
private[scheduler]
case class SpeculativeTaskSubmitted(task: Task[_], taskIndex: Int = -1) extends DAGSchedulerEvent

/** 不可调度的TaskSet添加事件 */
private[scheduler]
case class UnschedulableTaskSetAdded(stageId: Int, stageAttemptId: Int)
  extends DAGSchedulerEvent

/** 不可调度的TaskSet移除事件 */
private[scheduler]
case class UnschedulableTaskSetRemoved(stageId: Int, stageAttemptId: Int)
  extends DAGSchedulerEvent

/** 注册Shuffle合并状态事件 */
private[scheduler] case class RegisterMergeStatuses(
    stage: ShuffleMapStage, mergeStatuses: Seq[(Int, MergeStatus)])
  extends DAGSchedulerEvent

/** Shuffle合并完成事件 */
private[scheduler] case class ShuffleMergeFinalized(stage: ShuffleMapStage)
  extends DAGSchedulerEvent

/** Shuffle推送完成事件 */
private[scheduler] case class ShufflePushCompleted(
    shuffleId: Int, shuffleMergeId: Int, mapIndex: Int)
  extends DAGSchedulerEvent

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

import java.util.Properties
import javax.annotation.Nullable

import scala.collection.Map

import org.apache.spark.TaskEndReason
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo}

/** Stage 提交事件 */
@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent

/** Stage 完成事件 */
@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

/** 任务开始事件 */
@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent

/** 任务正在获取结果事件 */
@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

/** 推测执行任务提交事件 */
@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(
    stageId: Int,
    stageAttemptId: Int = 0)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // didn't stored taskIndex
  private var _taskIndex: Int = -1
  private var _partitionId: Int = -1

  def taskIndex: Int = _taskIndex
  def partitionId: Int = _partitionId

  def this(stageId: Int, stageAttemptId: Int, taskIndex: Int, partitionId: Int) = {
    this(stageId, stageAttemptId)
    _partitionId = partitionId
    _taskIndex = taskIndex
  }
}

/** 任务结束事件 */
@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskExecutorMetrics: ExecutorMetrics,
    // 任务失败时 taskMetrics 可能为 null
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent

/** 作业开始事件 */
@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // 注意：为了向后兼容，保留了只存储 stageIds 而非 StageInfos 的旧版本字段
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

/** 作业结束事件 */
@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
  extends SparkListenerEvent

/** 环境变量更新事件 */
@DeveloperApi
case class SparkListenerEnvironmentUpdate(
    environmentDetails: Map[String, collection.Seq[(String, String)]])
  extends SparkListenerEvent

/** BlockManager 添加事件 */
@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}

/** BlockManager 移除事件 */
@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent

/** RDD 取消持久化事件 */
@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

/** Executor 添加事件 */
@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent

/** Executor 移除事件 */
@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent

/** Executor 被加入黑名单事件（已弃用，使用 SparkListenerExecutorExcluded 代替） */
@DeveloperApi
@deprecated("use SparkListenerExecutorExcluded instead", "3.1.0")
case class SparkListenerExecutorBlacklisted(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

/** Executor 被排除事件 */
@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcluded(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorBlacklistedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcludedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklistedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcludedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
  extends SparkListenerEvent


@DeveloperApi
case class SparkListenerExecutorUnexcluded(time: Long, executorId: String)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklisted(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcluded(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeUnexcluded(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetAdded(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetRemoved(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

@DeveloperApi
@Since("3.2.0")
case class SparkListenerMiscellaneousProcessAdded(time: Long, processId: String,
    info: MiscellaneousProcessDetails) extends SparkListenerEvent

/**
 * Periodic updates from executors.
 * @param execId executor id
 * @param accumUpdates sequence of (taskId, stageId, stageAttemptId, accumUpdates)
 * @param executorUpdates executor level per-stage metrics updates
 *
 * @since 3.1.0
 */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty)
  extends SparkListenerEvent

/**
 * Peak metric values for the executor for the stage, written to the history log at stage
 * completion.
 * @param execId executor id
 * @param stageId stage id
 * @param stageAttemptId stage attempt
 * @param executorMetrics executor level metrics peak values
 */
@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(
    time: Long,
    exitCode: Option[Int] = None) extends SparkListenerEvent

/**
 * An internal class that describes the metadata of an event log.
 */
@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerResourceProfileAdded(resourceProfile: ResourceProfile)
  extends SparkListenerEvent

/**
 * 监听 Spark 调度器事件的接口。大多数应用应直接继承 SparkListener 或 SparkFirehoseListener，
 * 而不是实现此接口。
 *
 * 注意：这是一个内部接口，可能在不同的 Spark 版本中发生变化。
 */
private[spark] trait SparkListenerInterface {

  /** 当 Stage 完成（成功或失败）时调用 */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  /** 当 Stage 被提交时调用 */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  /** 当任务开始时调用 */
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  /** 当任务开始远程获取结果时调用（不需要远程获取结果的任务不会触发） */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  /** 当任务结束时调用 */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  /** 当作业开始时调用 */
  def onJobStart(jobStart: SparkListenerJobStart): Unit

  /** 当作业结束时调用 */
  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  /** 当环境属性更新时调用 */
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  /** 当新的 BlockManager 加入时调用 */
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

  /** 当已有的 BlockManager 被移除时调用 */
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  /** 当应用手动取消 RDD 持久化时调用 */
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  /** 当应用启动时调用 */
  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  /** 当应用结束时调用 */
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  /** 当 Driver 在心跳中接收到 Executor 的任务指标时调用 */
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  /**
   * 当指定 (Executor, Stage) 组合的峰值内存指标可用时调用。
   * 注意：仅在从事件日志读取时存在（如历史服务器中），在运行中的应用中不会被调用。
   */
  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  /** 当 Driver 注册新的 Executor 时调用 */
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

  /** 当 Driver 移除 Executor 时调用 */
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  /** 当 Driver 在应用级别排除 Executor 时调用（已弃用） */
  @deprecated("use onExecutorExcluded instead", "3.1.0")
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   */
  def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  @deprecated("use onExecutorExcludedForStage instead", "3.1.0")
  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  @deprecated("use onNodeExcludedForStage instead", "3.1.0")
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  def onNodeExcludedForStage(nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  @deprecated("use onExecutorUnexcluded instead", "3.1.0")
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  @deprecated("use onNodeExcluded instead", "3.1.0")
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
  def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  @deprecated("use onNodeUnexcluded instead", "3.1.0")
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit

  /**
   * Called when a taskset becomes unschedulable due to exludeOnFailure and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit

  /**
   * Called when an unschedulable taskset becomes schedulable and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit

  /**
   * Called when the driver receives a block update info.
   */
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  /**
   * Called when a speculative task is submitted
   */
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   */
  def onOtherEvent(event: SparkListenerEvent): Unit

  /**
   * Called when a Resource Profile is added to the manager.
   */
  def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit
}


/**
 * :: DeveloperApi ::
 * SparkListenerInterface 的默认实现，所有回调方法均为空操作。
 * 用户可以继承此类并只重写感兴趣的方法。
 *
 * 注意：这是一个内部接口，可能在不同的 Spark 版本中发生变化。
 */
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }
  override def onExecutorExcluded(
      executorExcluded: SparkListenerExecutorExcluded): Unit = { }

  override def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }
  override def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit = { }

  override def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }
  override def onNodeExcludedForStage(
      nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }
  override def onExecutorUnexcluded(
      executorUnexcluded: SparkListenerExecutorUnexcluded): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }
  override def onNodeExcluded(
      nodeExcluded: SparkListenerNodeExcluded): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }
  override def onNodeUnexcluded(
      nodeUnexcluded: SparkListenerNodeUnexcluded): Unit = { }

  override def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = { }

  override def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = { }
}

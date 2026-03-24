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

package org.apache.spark.status

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.{HashSet, TreeSet}
import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler.{AccumulableInfo, StageInfo, TaskInfo}
import org.apache.spark.status.api.v1
import org.apache.spark.storage.{RDDInfo, StorageLevel}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{AccumulatorContext, Utils}
import org.apache.spark.util.Utils.weakIntern
import org.apache.spark.util.collection.OpenHashSet

/**
 * 文件概要：Spark应用运行时状态实体定义，提供各种运行中实体（Job、Stage、Task、Executor等）的可变状态存储，
 * 定期将更新后的不可变视图写入应用状态存储，供UI和监控使用。
 */

/**
 * 运行中实体抽象基类，所有运行时状态实体的父类，负责管理实体更新并写入状态存储
 */
private[spark] abstract class LiveEntity {

  var lastWriteTime = -1L

  /**
   * 将当前实体更新写入状态存储
   * @param store 状态跟踪存储
   * @param now 当前时间戳
   * @param checkTriggers 是否检查存储大小触发清理
   */
  def write(store: ElementTrackingStore, now: Long, checkTriggers: Boolean = false): Unit = {
    // 首次写入总是检查触发条件，因为新增元素可能导致元素类型超出最大数量限制
    store.write(doUpdate(), checkTriggers || lastWriteTime == -1L)
    lastWriteTime = now
  }

  /**
   * 返回更新后的实体数据，用于写入状态存储
   * @return 更新后的实体数据对象
   */
  protected def doUpdate(): Any

}

/**
 * 运行中Job实体，存储Job运行时可变状态
 * @param jobId Job ID
 * @param name Job名称
 * @param description Job描述
 * @param submissionTime 提交时间
 * @param stageIds 包含的Stage ID列表
 * @param jobGroup Job组标识
 * @param jobTags Job标签列表
 * @param numTasks 总任务数
 * @param sqlExecutionId SQL执行ID（如果是SQL Job）
 */
private class LiveJob(
    val jobId: Int,
    name: String,
    description: Option[String],
    val submissionTime: Option[Date],
    val stageIds: Seq[Int],
    jobGroup: Option[String],
    jobTags: Seq[String],
    numTasks: Int,
    sqlExecutionId: Option[Long]) extends LiveEntity {

  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0

  // 已完成任务索引，每个元素打包存储stage ID和任务索引到一个Long值
  val completedIndices = new OpenHashSet[Long]()

  var killedTasks = 0
  var killedSummary: Map[String, Int] = Map()

  var skippedTasks = 0
  var skippedStages = Set[Int]()

  var status = JobExecutionStatus.RUNNING
  var completionTime: Option[Date] = None

  var completedStages: Set[Int] = Set()
  var activeStages = 0
  var failedStages = 0

  override protected def doUpdate(): Any = {
    val info = new v1.JobData(
      jobId,
      name,
      description,
      submissionTime,
      completionTime,
      stageIds,
      jobGroup,
      jobTags,
      status,
      numTasks,
      activeTasks,
      completedTasks,
      skippedTasks,
      failedTasks,
      killedTasks,
      completedIndices.size,
      activeStages,
      completedStages.size,
      skippedStages.size,
      failedStages,
      killedSummary)
    new JobDataWrapper(info, skippedStages, sqlExecutionId)
  }

}

/**
 * 运行中Task实体，存储Task运行时可变状态
 * @param info Task基本信息
 * @param stageId 所属Stage ID
 * @param stageAttemptId 所属Stage尝试ID
 * @param lastUpdateTime 最后更新时间
 */
private class LiveTask(
    var info: TaskInfo,
    stageId: Int,
    stageAttemptId: Int,
    lastUpdateTime: Option[Long]) extends LiveEntity {

  import LiveEntityHelpers._

  // Task指标未上报时使用特殊值，写入存储时会在TaskDataWrapper中检查处理
  private var metrics: v1.TaskMetrics = createMetrics(default = -1L)

  var errorMessage: Option[String] = None

  /**
   * 更新Task指标，返回新旧指标的差值
   * @param metrics 新的Task指标
   * @return 差值指标
   */
  def updateMetrics(metrics: TaskMetrics): v1.TaskMetrics = {
    if (metrics != null) {
      val old = this.metrics
      val newMetrics = createMetrics(
        metrics.executorDeserializeTime,
        metrics.executorDeserializeCpuTime,
        metrics.executorRunTime,
        metrics.executorCpuTime,
        metrics.resultSize,
        metrics.jvmGCTime,
        metrics.resultSerializationTime,
        metrics.memoryBytesSpilled,
        metrics.diskBytesSpilled,
        metrics.peakExecutionMemory,
        metrics.inputMetrics.bytesRead,
        metrics.inputMetrics.recordsRead,
        metrics.outputMetrics.bytesWritten,
        metrics.outputMetrics.recordsWritten,
        metrics.shuffleReadMetrics.remoteBlocksFetched,
        metrics.shuffleReadMetrics.localBlocksFetched,
        metrics.shuffleReadMetrics.fetchWaitTime,
        metrics.shuffleReadMetrics.remoteBytesRead,
        metrics.shuffleReadMetrics.remoteBytesReadToDisk,
        metrics.shuffleReadMetrics.localBytesRead,
        metrics.shuffleReadMetrics.recordsRead,
        metrics.shuffleReadMetrics.corruptMergedBlockChunks,
        metrics.shuffleReadMetrics.mergedFetchFallbackCount,
        metrics.shuffleReadMetrics.remoteMergedBlocksFetched,
        metrics.shuffleReadMetrics.localMergedBlocksFetched,
        metrics.shuffleReadMetrics.remoteMergedChunksFetched,
        metrics.shuffleReadMetrics.localMergedChunksFetched,
        metrics.shuffleReadMetrics.remoteMergedBytesRead,
        metrics.shuffleReadMetrics.localMergedBytesRead,
        metrics.shuffleReadMetrics.remoteReqsDuration,
        metrics.shuffleReadMetrics.remoteMergedReqsDuration,
        metrics.shuffleWriteMetrics.bytesWritten,
        metrics.shuffleWriteMetrics.writeTime,
        metrics.shuffleWriteMetrics.recordsWritten)

      this.metrics = newMetrics

      // 仅当旧指标包含有效信息时才计算差值，否则新指标本身就是差值
      if (old.executorDeserializeTime >= 0L) {
        subtractMetrics(newMetrics, old)
      } else {
        newMetrics
      }
    } else {
      null
    }
  }

  override protected def doUpdate(): Any = {
    // 计算任务运行时长：已完成任务使用总时长，未完成任务计算到当前时间的运行时长
    val duration = if (info.finished) {
      info.duration
    } else {
      info.timeRunning(lastUpdateTime.getOrElse(System.currentTimeMillis()))
    }

    val hasMetrics = metrics.executorDeserializeTime >= 0

    /**
     * SPARK-26260: 对于失败任务，将指标存储为负值以避免任务汇总计算。TaskDataWrapper的toApi方法会还原为实际值
     */
    val taskMetrics: v1.TaskMetrics = if (hasMetrics && !info.successful) {
      makeNegative(metrics)
    } else {
      metrics
    }

    new TaskDataWrapper(
      info.taskId,
      info.index,
      info.attemptNumber,
      info.partitionId,
      info.launchTime,
      if (info.gettingResult) info.gettingResultTime else -1L,
      duration,
      weakIntern(info.executorId),
      weakIntern(info.host),
      weakIntern(info.status),
      weakIntern(info.taskLocality.toString()),
      info.speculative,
      newAccumulatorInfos(info.accumulables),
      errorMessage,

      hasMetrics,
      taskMetrics.executorDeserializeTime,
      taskMetrics.executorDeserializeCpuTime,
      taskMetrics.executorRunTime,
      taskMetrics.executorCpuTime,
      taskMetrics.resultSize,
      taskMetrics.jvmGcTime,
      taskMetrics.resultSerializationTime,
      taskMetrics.memoryBytesSpilled,
      taskMetrics.diskBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.bytesRead,
      taskMetrics.inputMetrics.recordsRead,
      taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.outputMetrics.recordsWritten,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.fetchWaitTime,
      taskMetrics.shuffleReadMetrics.remoteBytesRead,
      taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      taskMetrics.shuffleReadMetrics.localBytesRead,
      taskMetrics.shuffleReadMetrics.recordsRead,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.corruptMergedBlockChunks,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.mergedFetchFallbackCount,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedBlocksFetched,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.localMergedBlocksFetched,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedChunksFetched,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.localMergedChunksFetched,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedBytesRead,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.localMergedBytesRead,
      taskMetrics.shuffleReadMetrics.remoteReqsDuration,
      taskMetrics.shuffleReadMetrics.shufflePushReadMetrics.remoteMergedReqsDuration,
      taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.writeTime,
      taskMetrics.shuffleWriteMetrics.recordsWritten,

      stageId,
      stageAttemptId)
  }

}

/**
 * 运行中资源配置实体，存储ResourceProfile运行时信息
 * @param resourceProfileId 资源配置ID
 * @param executorResources Executor资源请求
 * @param taskResources Task资源请求
 * @param maxTasksPerExecutor 每个Executor最大任务数
 */
private class LiveResourceProfile(
    val resourceProfileId: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest],
    val maxTasksPerExecutor: Option[Int]) extends LiveEntity {

  def toApi(): v1.ResourceProfileInfo = {
    new v1.ResourceProfileInfo(resourceProfileId, executorResources, taskResources)
  }

  override protected def doUpdate(): Any = {
    new ResourceProfileWrapper(toApi())
  }
}

/**
 * 运行中Executor实体，存储Executor运行时可变状态
 * @param executorId Executor ID
 * @param _addTime 添加时间戳
 */
private[spark] class LiveExecutor(val executorId: String, _addTime: Long) extends LiveEntity {

  var hostPort: String = null
  var host: String = null
  var isActive = true
  var totalCores = 0

  val addTime = new Date(_addTime)
  var removeTime: Date = null
  var removeReason: String = null

  var rddBlocks = 0
  var memoryUsed = 0L
  var diskUsed = 0L
  var maxTasks = 0
  var maxMemory = 0L

  var totalTasks = 0
  var activeTasks = 0
  var completedTasks = 0
  var failedTasks = 0
  var totalDuration = 0L
  var totalGcTime = 0L
  var totalInputBytes = 0L
  var totalShuffleRead = 0L
  var totalShuffleWrite = 0L
  var isExcluded = false
  var excludedInStages: Set[Int] = TreeSet()

  var executorLogs = Map[String, String]()
  var attributes = Map[String, String]()
  var resources = Map[String, ResourceInformation]()

  // 内存指标可能未记录（例如旧事件日志），如果totalOnHeap未初始化则存储不包含该信息
  var totalOnHeap = -1L
  var totalOffHeap = 0L
  var usedOnHeap = 0L
  var usedOffHeap = 0L

  var resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID

  def hasMemoryInfo: Boolean = totalOnHeap >= 0L

  // Executor级指标峰值
  val peakExecutorMetrics = new ExecutorMetrics()

  def hostname: String = if (host != null) host else Utils.parseHostPort(hostPort)._1

  override protected def doUpdate(): Any = {
    // 仅当内存信息有效时封装内存指标
    val memoryMetrics = if (totalOnHeap >= 0) {
      Some(new v1.MemoryMetrics(usedOnHeap, usedOffHeap, totalOnHeap, totalOffHeap))
    } else {
      None
    }

    val info = new v1.ExecutorSummary(
      executorId,
      if (hostPort != null) hostPort else host,
      isActive,
      rddBlocks,
      memoryUsed,
      diskUsed,
      totalCores,
      maxTasks,
      activeTasks,
      failedTasks,
      completedTasks,
      totalTasks,
      totalDuration,
      totalGcTime,
      totalInputBytes,
      totalShuffleRead,
      totalShuffleWrite,
      isExcluded,
      maxMemory,
      addTime,
      Option(removeTime),
      Option(removeReason),
      executorLogs,
      memoryMetrics,
      excludedInStages,
      Some(peakExecutorMetrics).filter(_.isSet()),
      attributes,
      resources,
      resourceProfileId,
      isExcluded,
      excludedInStages)
    new ExecutorSummaryWrapper(info)
  }
}

/**
 * Executor上Stage运行汇总实体，存储单个Executor在单个Stage上的运行状态
 * @param stageId Stage ID
 * @param attemptId Stage尝试ID
 * @param executorId Executor ID
 */
private class LiveExecutorStageSummary(
    stageId: Int,
    attemptId: Int,
    executorId: String) extends LiveEntity {

  import LiveEntityHelpers._

  var taskTime = 0L
  var succeededTasks = 0
  var failedTasks = 0
  var killedTasks = 0
  var isExcluded = false

  var metrics = createMetrics(default = 0L)

  val peakExecutorMetrics = new ExecutorMetrics()

  override protected def doUpdate(): Any = {
    val info = new v1.ExecutorStageSummary(
      taskTime,
      failedTasks,
      succeededTasks,
      killedTasks,
      metrics.inputMetrics.bytesRead,
      metrics.inputMetrics.recordsRead,
      metrics.outputMetrics.bytesWritten,
      metrics.outputMetrics.recordsWritten,
      metrics.shuffleReadMetrics.remoteBytesRead + metrics.shuffleReadMetrics.localBytesRead,
      metrics.shuffleReadMetrics.recordsRead,
      metrics.shuffleWriteMetrics.bytesWritten,
      metrics.shuffleWriteMetrics.recordsWritten,
      metrics.memoryBytesSpilled,
      metrics.diskBytesSpilled,
      isExcluded,
      Some(peakExecutorMetrics).filter(_.isSet()),
      isExcluded)
    new ExecutorStageSummaryWrapper(stageId, attemptId, executorId, info)
  }

}

/**
 * Stage推测执行汇总实体，存储Stage推测执行的任务状态统计
 * @param stageId Stage ID
 * @param attemptId Stage尝试ID
 */
private class LiveSpeculationStageSummary(
    stageId: Int,
    attemptId: Int) extends LiveEntity {

  var numTasks = 0
  var numActiveTasks = 0
  var numCompletedTasks = 0
  var numFailedTasks = 0
  var numKilledTasks = 0

  override protected def doUpdate(): Any = {
    val info = new v1.SpeculationStageSummary(
      numTasks,
      numActiveTasks,
      numCompletedTasks,
      numFailedTasks,
      numKilledTasks
    )
    new SpeculationStageSummaryWrapper(stage
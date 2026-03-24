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

import java.io.File
import java.io.IOException
import java.util.{List => JList}

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import org.apache.spark.{JobExecutionStatus, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.internal.config.Status.LIVE_UI_LOCAL_STORE_DIR
import org.apache.spark.status.AppStatusUtils.getQuantilesValue
import org.apache.spark.status.api.v1
import org.apache.spark.storage.FallbackStorage.FALLBACK_BLOCK_MANAGER_ID
import org.apache.spark.ui.scope._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

/**
 * 应用状态存储服务，封装KVStore提供统一的状态数据访问API，用于Spark UI展示运行状态
 *
 * @param store 底层KV存储实例
 * @param listener 可选，实时应用的状态监听器，用于快速获取活动阶段信息
 * @param storePath 可选，磁盘存储目录路径，关闭时需要清理
 */
private[spark] class AppStatusStore(
    val store: KVStore,
    val listener: Option[AppStatusListener] = None,
    val storePath: Option[File] = None) {

  /**
   * 获取当前Spark应用基本信息
   * @return 应用基本信息对象
   */
  def applicationInfo(): v1.ApplicationInfo = {
    try {
      // 应用启动过程中可能还未写入信息，需要处理空数据
      Utils.tryWithResource(
        store.view(classOf[ApplicationInfoWrapper])
          .max(1)
          .closeableIterator()
      ) { it =>
        it.next().info
      }
    } catch {
      case _: NoSuchElementException =>
        throw new NoSuchElementException("Failed to get the application information. " +
          "If you are starting up Spark, please wait a while until it's ready.")
    }
  }

  /**
   * 获取应用运行环境信息
   * @return 应用环境信息对象
   */
  def environmentInfo(): v1.ApplicationEnvironmentInfo = {
    val klass = classOf[ApplicationEnvironmentInfoWrapper]
    store.read(klass, klass.getName()).info
  }

  /**
   * 获取所有资源配置信息列表
   * @return 资源配置信息序列
   */
  def resourceProfileInfo(): Seq[v1.ResourceProfileInfo] = {
    KVUtils.mapToSeq(store.view(classOf[ResourceProfileWrapper]))(_.rpInfo)
  }

  /**
   * 根据指定状态过滤获取作业列表
   * @param statuses 需要过滤的作业状态列表，为null或空则返回所有作业
   * @return 过滤后的作业数据序列
   */
  def jobsList(statuses: JList[JobExecutionStatus]): Seq[v1.JobData] = {
    val it = KVUtils.mapToSeq(store.view(classOf[JobDataWrapper]).reverse())(_.info)
    if (statuses != null && !statuses.isEmpty()) {
      it.filter { job => statuses.contains(job.status) }
    } else {
      it
    }
  }

  /**
   * 根据作业ID获取作业数据
   * @param jobId 作业ID
   * @return 作业数据对象
   */
  def job(jobId: Int): v1.JobData = {
    store.read(classOf[JobDataWrapper], jobId).info
  }

  /**
   * 根据作业ID获取作业数据以及关联的SQL执行ID
   * 如果没有关联SQL执行则返回None
   * @param jobId 作业ID
   * @return (作业数据, 关联SQL执行ID
   */
  def jobWithAssociatedSql(jobId: Int): (v1.JobData, Option[Long]) = {
    val data = store.read(classOf[JobDataWrapper], jobId)
    (data.info, data.sqlExecutionId)
  }

  /**
   * 获取所有执行器摘要列表
   * @param activeOnly 是否只返回活跃执行器
   * @return 执行器摘要序列
   */
  def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val base = store.view(classOf[ExecutorSummaryWrapper])
    val filtered = if (activeOnly) {
      // 使用active索引过滤活跃执行器
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    KVUtils.mapToSeq(filtered)(_.info)
      .filter(_.id != FALLBACK_BLOCK_MANAGER_ID.executorId)
      .map(replaceExec)
  }

  /**
   * 修正Driver执行器的GC时间和运行时间统计
   * @param origin 原始执行器摘要
   * @return 修正后的执行器摘要
   */
  private def replaceExec(origin: v1.ExecutorSummary): v1.ExecutorSummary = {
    if (origin.id == SparkContext.DRIVER_IDENTIFIER) {
      replaceDriverGcTime(origin, extractGcTime(origin), extractAppTime)
    } else {
      origin
    }
  }

  private def replaceDriverGcTime(source: v1.ExecutorSummary,
    totalGcTime: Option[Long], totalAppTime: Option[Long]): v1.ExecutorSummary = {
    new v1.ExecutorSummary(source.id, source.hostPort, source.isActive, source.rddBlocks,
      source.memoryUsed, source.diskUsed, source.totalCores, source.maxTasks, source.activeTasks,
      source.failedTasks, source.completedTasks, source.totalTasks,
      totalAppTime.getOrElse(source.totalDuration),
      totalGcTime.getOrElse(source.totalGCTime),
      source.totalInputBytes, source.totalShuffleRead,
      source.totalShuffleWrite, source.isBlacklisted, source.maxMemory, source.addTime,
      source.removeTime, source.removeReason, source.executorLogs, source.memoryMetrics,
      source.blacklistedInStages, source.peakMemoryMetrics, source.attributes, source.resources,
      source.resourceProfileId, source.isExcluded, source.excludedInStages)
  }

  /**
   * 从峰值内存指标中提取Driver总GC时间
   * @param source Driver执行器摘要
   * @return GC时间选项，无数据则返回None
   */
  private def extractGcTime(source: v1.ExecutorSummary): Option[Long] = {
    source.peakMemoryMetrics.map(_.getMetricValue("TotalGCTime"))
  }

  /**
   * 计算Driver应用运行总时长
   * @return 运行时长选项，应用未结束则返回None
   */
  private def extractAppTime: Option[Long] = {
    var startTime = 0L
    // -1表示应用尚未结束，这里存储的是事件写入时间
    var endTime = 0L
    try {
      val appInfo = applicationInfo()
      startTime = appInfo.attempts.head.startTime.getTime()
      endTime = appInfo.attempts.head.endTime.getTime()
    } catch {
      // 应用启动过早，尚未写入应用信息，等待后续获取
      case _: NoSuchElementException =>
    }
    if (endTime == 0) {
      None
    } else if (endTime < 0) {
      // 应用仍在运行，计算当前已运行时间
      Option(System.currentTimeMillis() - startTime)
    } else {
      // 应用已结束，使用结束时间减启动时间
      Option(endTime - startTime)
    }
  }

  /**
   * 获取所有杂项进程摘要列表
   * @param activeOnly 是否只返回活跃进程
   * @return 进程摘要序列
   */
  def miscellaneousProcessList(activeOnly: Boolean): Seq[v1.ProcessSummary] = {
    val base = store.view(classOf[ProcessSummaryWrapper])
    val filtered = if (activeOnly) {
      base.index("active").reverse().first(true).last(true)
    } else {
      base
    }
    KVUtils.mapToSeq(filtered)(_.info)
  }

  /**
   * 根据执行器ID获取执行器摘要
   * @param executorId 执行器ID
   * @return 执行器摘要对象
   */
  def executorSummary(executorId: String): v1.ExecutorSummary = {
    store.read(classOf[ExecutorSummaryWrapper], executorId).info
  }

  /**
   * 获取所有活跃阶段列表，供控制台进度条快速渲染使用
   * 仅在实时应用中返回有效数据，历史应用返回空列表
   * @return 活跃阶段数据序列
   */
  def activeStages(): Seq[v1.StageData] = {
    listener.map(_.activeStages()).getOrElse(Nil)
  }

  /**
   * 根据阶段状态过滤获取所有阶段数据列表
   * @param statuses 需要过滤的阶段状态列表
   * @param details 是否需要包含详细任务信息
   * @param withSummaries 是否需要包含任务分位数指标汇总
   * @param unsortedQuantiles 需要计算的分位数数组
   * @param taskStatus 需要过滤的任务状态列表
   * @return 处理后的阶段数据序列
   */
  def stageList(
    statuses: JList[v1.StageStatus],
    details: Boolean = false,
    withSummaries: Boolean = false,
    unsortedQuantiles: Array[Double] = Array.empty,
    taskStatus: JList[v1.TaskStatus] = List().asJava): Seq[v1.StageData] = {
    val quantiles = unsortedQuantiles.sorted
    val it = KVUtils.mapToSeq(store.view(classOf[StageDataWrapper]).reverse())(_.info)
    val ret = if (statuses != null && !statuses.isEmpty()) {
      it.filter { s => statuses.contains(s.status) }
    } else {
      it
    }

    ret.map { s =>
      newStageData(s, withDetail = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = quantiles)
    }
  }

  /**
   * 根据阶段ID获取该阶段所有尝试的数据
   * @param stageId 阶段ID
   * @param details 是否需要包含详细任务信息
   * @param taskStatus 需要过滤的任务状态列表
   * @param withSummaries 是否需要包含任务分位数指标汇总
   * @param unsortedQuantiles 需要计算的分位数数组
   * @return 该阶段所有尝试的数据序列
   */
  def stageData(
    stageId: Int,
    details: Boolean = false,
    taskStatus: JList[v1.TaskStatus] = List().asJava,
    withSummaries: Boolean = false,
    unsortedQuantiles: Array[Double] = Array.empty[Double]): Seq[v1.StageData] = {
    KVUtils.mapToSeq(store.view(classOf[StageDataWrapper]).index("stageId")
      .first(stageId).last(stageId)) { s =>
      newStageData(s.info, withDetail = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = unsortedQuantiles)
    }
  }

  /**
   * 获取指定阶段ID最新一次尝试的数据
   * @param stageId 阶段ID
   * @return 最新尝试的阶段数据
   */
  def lastStageAttempt(stageId: Int): v1.StageData = {
    val it = store.view(classOf[StageDataWrapper])
      .index("stageId")
      .reverse()
      .first(stageId)
      .last(stageId)
      .closeableIterator()
    try {
      if (it.hasNext()) {
        it.next().info
      } else {
        throw new NoSuchElementException(s"No stage with id $stageId")
      }
    } finally {
      it.close()
    }
  }

  /**
   * 获取指定阶段ID和尝试ID的详细阶段数据，同时返回关联的作业ID列表
   * @param stageId 阶段ID
   * @param stageAttemptId 尝试ID
   * @param details 是否需要包含详细任务信息
   * @param taskStatus 需要过滤的任务状态列表
   * @param withSummaries 是否需要包含任务分位数指标汇总
   * @param unsortedQuantiles 需要计算的分位数数组
   * @return (处理后的阶段数据, 关联作业ID列表)
   */
  def stageAttempt(
      stageId: Int, stageAttemptId: Int,
      details: Boolean = false,
      taskStatus: JList[v1.TaskStatus] = List().asJava,
      withSummaries: Boolean = false,
      unsortedQuantiles: Array[Double] = Array.empty[Double]): (v1.StageData, Seq[Int]) = {
    val stageKey = Array(stageId, stageAttemptId)
    val stageDataWrapper = store.read(classOf[StageDataWrapper], stageKey)
    val stage = newStageData(stageDataWrapper.info, withDetail = details, taskStatus = taskStatus,
      withSummaries = withSummaries, unsortedQuantiles = unsortedQuantiles)
    (stage, stageDataWrapper.jobIds.toSeq)
  }

  /**
   * 获取指定阶段尝试的任务总数
   * @param stageId 阶段ID
   * @param stageAttemptId 尝试ID
   * @return 任务总数
   */
  def taskCount(stageId: Int, stageAttemptId: Int): Long = {
    store.count(classOf[TaskDataWrapper], "stage", Array(stageId, stageAttemptId))
  }

  /**
   * 获取指定阶段尝试的 locality 分布统计
   * @param stageId 阶段ID
   * @param stageAttemptId 尝试ID
   * @return locality分布Map，key为locality级别，value为对应任务数
   */
  def localitySummary(stageId: Int, stageAttemptId: Int): Map[String, Long] = {
    store.read(classOf[StageDataWrapper], Array(stageId, stageAttemptId)).locality
  }

  /**
   * 计算指定阶段尝试所有任务指标的分位数分布
   * 仅对0.05间隔的标准分位数会缓存结果，避免重复扫描全量任务数据
   * @param stageId 阶段ID
   * @param stageAttemptId 尝试ID
   * @param unsortedQuantiles 需要计算的分位数数组
   * @return 任务指标分位数分布对象，无任务数据时返回None
   */
  def taskSummary(
      stageId: Int,
      stageAttemptId: Int,
      unsortedQuantiles: Array[Double]): Option[v1.TaskMetricDistributions] = {
    val stageKey = Array(stageId, stageAttemptId)
    val quantiles = unsortedQuantiles.sorted.toImmutableArraySeq

    // 统计有效任务数量，使用skip减少反序列化开销
    val count = {
      Utils.tryWithResource(
        store.view(classOf[TaskDataWrapper])
          .parent(stageKey)
          .index(TaskIndexNames.EXEC_RUN_TIME)
          .first(0L)
          .closeableIterator()
      ) { it =>
        var _count = 0L
        while (it.hasNext()) {
          _count += 1
          it.skip(1)
        }
        _count
      }
    }

    if (count <= 0) {
      return None
    }

    // 检查已有缓存分位数，仅当任务数量匹配才使用缓存
    val cachedQuantiles = quantiles.filter(shouldCacheQuantile).flatMap { q =>
      val qkey = Array(stageId, stageAttemptId, quantileToString(q))
      asOption(store.read(classOf[CachedQuantile], qkey)).filter(_.taskCount == count)
    }

    // 所有分位数都已缓存，直接返回结果
    if (cachedQuantiles.size == quantiles.size) {
      def toValues(fn: CachedQuantile => Double): IndexedSeq[Double] = cachedQuantiles.map(fn)

      val distributions = new v1.TaskMetricDistributions(
        quantiles = quantiles,
        duration = toValues(_.duration),
        executorDeserializeTime = toValues(_.executorDeserializeTime),
        executorDeserializeCpuTime = toValues(_.executorDeserializeCpuTime),
        executorRunTime = toValues(_.executorRunTime),
        executorCpuTime = toValues(_.executorCpuTime),
        resultSize = toValues(_.resultSize),
        jvmGcTime = toValues(_.jvmGcTime),
        resultSerializationTime = toValues(_.resultSerializationTime),
        gettingResultTime = toValues(_.gettingResultTime),
        schedulerDelay = toValues(_.schedulerDelay),
        peakExecution
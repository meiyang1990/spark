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
package org.apache.spark.executor

import java.lang.Long.{MAX_VALUE => LONG_MAX_VALUE}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLongArray

import scala.collection.mutable.HashMap

import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * A class that polls executor metrics, and tracks their peaks per task and per stage.
 * Each executor keeps an instance of this class.
 * The poll method polls the executor metrics, and is either run in its own thread or
 * called by the executor's heartbeater thread, depending on configuration.
 * The class keeps two ConcurrentHashMaps that are accessed (via its methods) by the
 * executor's task runner threads concurrently with the polling thread. One thread may
 * update one of these maps while another reads it, so the reading thread may not get
 * the latest metrics, but this is ok.
 * We track executor metric peaks per stage, as well as per task. The per-stage peaks
 * are sent in executor heartbeats. That way, we get incremental updates of the metrics
 * as the tasks are running, and if the executor dies we still have some metrics. The
 * per-task peaks are sent in the task result at task end. These are useful for short
 * tasks. If there are no heartbeats during the task, we still get the metrics polled
 * for the task.
 *
 * Executor 指标轮询器，负责定期采集 Executor 的资源指标并追踪每个 Task 和 Stage 的峰值。
 *
 * 【核心功能】
 * 1. 定期轮询 Executor 的内存、CPU 等资源指标
 * 2. 追踪每个运行中 Task 的指标峰值（在 TaskResult 中返回）
 * 3. 追踪每个活跃 Stage 的指标峰值（在心跳中增量汇报）
 *
 * 【设计考量】
 * - 双重追踪：同时按 Task 和 Stage 两个维度追踪峰值
 *   - Stage 级峰值：通过心跳增量发送，即使 Executor 崩溃也能保留部分指标
 *   - Task 级峰值：随 TaskResult 发送，对于短任务特别有用（可能没有心跳周期）
 * - 并发安全：使用 ConcurrentHashMap 和 AtomicLongArray 处理并发读写
 *   - Task 线程可能在轮询的同时更新/读取映射表
 *   - 允许读取时获取略旧的数据，这是可接受的
 *
 * 【轮询机制】
 * - 如果 pollingInterval > 0：启动独立的轮询线程定期采集
 * - 如果 pollingInterval = 0：由 Heartbeater 线程在每次心跳时调用 poll()
 *
 * @param memoryManager the memory manager used by the executor.
 * @param pollingInterval the polling interval in milliseconds.
 */
private[spark] class ExecutorMetricsPoller(
    memoryManager: MemoryManager,
    pollingInterval: Long,
    executorMetricsSource: Option[ExecutorMetricsSource]) extends Logging {

  // Stage 的复合键类型：(stageId, stageAttemptId)
  type StageKey = (Int, Int)
  // Task Count and Metric Peaks：封装运行中任务计数和指标峰值
  private[executor] case class TCMP(count: Long, peaks: AtomicLongArray)

  // Stage -> (运行中任务数, 指标峰值) 的映射
  // 使用 ConcurrentHashMap 支持轮询线程和 Task 线程的并发访问
  private[executor] val stageTCMP = new ConcurrentHashMap[StageKey, TCMP]

  // Task ID -> 指标峰值的映射
  private val taskMetricPeaks = new ConcurrentHashMap[Long, AtomicLongArray]

  // 轮询调度器：仅当 pollingInterval > 0 时创建独立线程
  private val poller =
    if (pollingInterval > 0) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-metrics-poller"))
    } else {
      None
    }

  /**
   * Function to poll executor metrics.
   * On start, if pollingInterval is positive, this is scheduled to run at that interval.
   * Otherwise, this is called by the reportHeartBeat function defined in Executor and passed
   * to its Heartbeater.
   *
   * 执行一次指标采集：获取当前指标值，更新所有活跃 Stage 和 Task 的峰值
   */
  def poll(): Unit = {
    // Note: Task runner threads may update stageTCMP or read from taskMetricPeaks concurrently
    // with this function via calls to methods of this class.

    // 获取当前所有指标的最新值
    val latestMetrics = ExecutorMetrics.getCurrentMetrics(memoryManager)
    // 更新 MetricsSource 的快照，供 Metrics 系统暴露
    executorMetricsSource.foreach(_.updateMetricsSnapshot(latestMetrics))

    // 更新峰值：使用 getAndAccumulate + math.max 原子地更新为较大值
    def updatePeaks(metrics: AtomicLongArray): Unit = {
      (0 until metrics.length).foreach { i =>
        metrics.getAndAccumulate(i, latestMetrics(i), math.max)
      }
    }

    // 更新所有活跃 Stage 的峰值
    stageTCMP.forEachValue(LONG_MAX_VALUE, v => updatePeaks(v.peaks))

    // 更新所有运行中 Task 的峰值
    taskMetricPeaks.forEachValue(LONG_MAX_VALUE, updatePeaks)
  }

  /** Starts the polling thread. */
  /** 启动轮询线程（仅当配置了独立轮询间隔时生效） */
  def start(): Unit = {
    poller.foreach { exec =>
      val pollingTask: Runnable = () => Utils.logUncaughtExceptions(poll())
      exec.scheduleAtFixedRate(pollingTask, 0L, pollingInterval, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Called by TaskRunner#run.
   *
   * Task 开始时调用：初始化 Task 的指标峰值追踪，并更新 Stage 的运行任务计数
   */
  def onTaskStart(taskId: Long, stageId: Int, stageAttemptId: Int): Unit = {
    // 为 Task 创建指标峰值数组
    taskMetricPeaks.put(taskId, new AtomicLongArray(ExecutorMetricType.numMetrics))

    // 更新 Stage 的状态：如果是该 Stage 的第一个任务则创建条目，否则增加计数
    val countAndPeaks = stageTCMP.compute((stageId, stageAttemptId), (k: StageKey, v: TCMP) =>
      if (v == null) {
        TCMP(1L, new AtomicLongArray(ExecutorMetricType.numMetrics))
      } else {
        TCMP(v.count + 1, v.peaks)
      })
    logDebug(s"stageTCMP: ($stageId, $stageAttemptId) -> ${countAndPeaks.count}")
  }

  /**
   * Called by TaskRunner#run. It should only be called if onTaskStart has been called with
   * the same arguments.
   *
   * Task 完成时调用：减少 Stage 的运行任务计数，移除 Task 的指标追踪
   */
  def onTaskCompletion(taskId: Long, stageId: Int, stageAttemptId: Int): Unit = {
    // 减少 Stage 的运行任务计数
    def decrementCount(stage: StageKey, countAndPeaks: TCMP): TCMP = {
      val countValue = countAndPeaks.count - 1
      assert(countValue >= 0, "task count shouldn't below 0")
      logDebug(s"stageTCMP: (${stage._1}, ${stage._2}) -> " + countValue)
      TCMP(countValue, countAndPeaks.peaks)
    }

    stageTCMP.computeIfPresent((stageId, stageAttemptId), decrementCount)

    // 移除 Task 的指标峰值追踪
    taskMetricPeaks.remove(taskId)
  }

  /**
   * Called by TaskRunner#run.
   *
   * 获取指定 Task 的指标峰值，用于填充 TaskResult
   */
  def getTaskMetricPeaks(taskId: Long): Array[Long] = {
    // 如果 taskId 无效或 Task 被 kill 导致 onTaskStart 未调用，返回全零数组
    val currentPeaks = taskMetricPeaks.get(taskId) // may be null
    val metricPeaks = new Array[Long](ExecutorMetricType.numMetrics) // initialized to zeros
    if (currentPeaks != null) {
      ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
        metricPeaks(i) = currentPeaks.get(i)
      }
    }
    metricPeaks
  }


  /**
   * Called by the reportHeartBeat function defined in Executor and passed to its Heartbeater.
   * It resets the metric peaks in stageTCMP before returning the executor updates.
   * Thus, the executor updates contains the per-stage metric peaks since the last heartbeat
   * (the last time this method was called).
   *
   * 获取并重置 Stage 级指标峰值，用于心跳汇报。
   * 返回自上次心跳以来的峰值增量，并将内部峰值重置为零以便下次统计。
   */
  def getExecutorUpdates(): HashMap[StageKey, ExecutorMetrics] = {
    val executorUpdates = new HashMap[StageKey, ExecutorMetrics]

    // 获取当前峰值并重置为零
    def getUpdateAndResetPeaks(k: StageKey, v: TCMP): TCMP = {
      executorUpdates.put(k, new ExecutorMetrics(v.peaks))
      TCMP(v.count, new AtomicLongArray(ExecutorMetricType.numMetrics))
    }

    stageTCMP.replaceAll(getUpdateAndResetPeaks)

    // 清理已无运行任务的 Stage 条目
    def removeIfInactive(k: StageKey, v: TCMP): TCMP = {
      if (v.count == 0) {
        logDebug(s"removing (${k._1}, ${k._2}) from stageTCMP")
        null
      } else {
        v
      }
    }

    // 移除计数为零的 Stage 条目，避免内存泄漏
    executorUpdates.foreach { case (k, _) =>
      stageTCMP.computeIfPresent(k, removeIfInactive)
    }

    executorUpdates
  }

  /** Stops the polling thread. */
  /** 停止轮询线程，等待最多 10 秒完成清理 */
  def stop(): Unit = {
    poller.foreach { exec =>
      exec.shutdown()
      exec.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}

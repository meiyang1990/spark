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
 * 文件说明：Executor 指标轮询服务，属于Spark核心执行模块，负责定期采集Executor资源使用指标，
 * 同时按Task和Stage两个维度追踪指标峰值，用于监控和指标汇报。
 *
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
 * 【核心功能】
 * 1. 定期轮询 Executor 的内存、CPU 等资源指标
 * 2. 追踪每个运行中 Task 的指标峰值（在 TaskResult 中返回给Driver）
 * 3. 追踪每个活跃 Stage 的指标峰值（在Executor心跳中增量汇报给Driver）
 *
 * 【设计考量】
 * - 双重追踪：同时按 Task 和 Stage 两个维度追踪峰值
 *   - Stage 级峰值：通过心跳增量发送，即使 Executor 崩溃也能保留部分已汇报指标
 *   - Task 级峰值：随 TaskResult 发送，对于短任务特别有用（短任务生命周期内可能不会触发心跳）
 * - 并发安全：使用 ConcurrentHashMap 和 AtomicLongArray 处理并发读写
 *   - Task 执行线程可能在轮询的同时更新/读取映射表
 *   - 允许读取时获取略旧的数据，这在监控场景下是可接受的
 *
 * 【轮询机制】
 * - 如果 pollingInterval > 0：启动独立的轮询线程定期采集
 * - 如果 pollingInterval = 0：由 Heartbeater 线程在每次心跳时调用 poll()
 *
 * @param memoryManager Executor使用的内存管理器，用于获取内存使用指标
 * @param pollingInterval 指标轮询间隔，单位毫秒
 * @param executorMetricsSource 指标来源，可选，用于将指标暴露给Spark的Metrics系统
 */
private[spark] class ExecutorMetricsPoller(
    memoryManager: MemoryManager,
    pollingInterval: Long,
    executorMetricsSource: Option[ExecutorMetricsSource]) extends Logging {

  // Stage 唯一标识复合键：(stageId, stageAttemptId)
  type StageKey = (Int, Int)
  // 封装Stage的运行中任务计数和当前指标峰值
  private[executor] case class TCMP(count: Long, peaks: AtomicLongArray)

  // Stage到(运行任务计数, 指标峰值)的并发映射表
  private[executor] val stageTCMP = new ConcurrentHashMap[StageKey, TCMP]

  // Task ID到任务指标峰值的并发映射表
  private val taskMetricPeaks = new ConcurrentHashMap[Long, AtomicLongArray]

  // 独立轮询线程调度器，仅当配置了正的轮询间隔时创建
  private val poller =
    if (pollingInterval > 0) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-metrics-poller"))
    } else {
      None
    }

  /**
   * 执行一次指标采集，获取当前Executor最新指标，更新所有活跃Stage和Task的峰值记录
   * 
   * 当配置独立轮询间隔时，该方法会被定时调度执行；否则由Executor心跳线程在心跳时调用
   */
  def poll(): Unit = {
    // Note: Task runner threads may update stageTCMP or read from taskMetricPeaks concurrently
    // with this function via calls to methods of this class.

    // 获取当前所有Executor指标的最新值
    val latestMetrics = ExecutorMetrics.getCurrentMetrics(memoryManager)
    // 更新MetricsSource的指标快照，供Metrics系统对外暴露
    executorMetricsSource.foreach(_.updateMetricsSnapshot(latestMetrics))

    // 原子更新峰值：如果新值大于当前记录，则更新为新值
    def updatePeaks(metrics: AtomicLongArray): Unit = {
      (0 until metrics.length).foreach { i =>
        metrics.getAndAccumulate(i, latestMetrics(i), math.max)
      }
    }

    // 更新所有活跃Stage的峰值
    stageTCMP.forEachValue(LONG_MAX_VALUE, v => updatePeaks(v.peaks))

    // 更新所有运行中Task的峰值
    taskMetricPeaks.forEachValue(LONG_MAX_VALUE, updatePeaks)
  }

  /** 启动轮询线程，仅当配置了独立轮询间隔时生效 */
  def start(): Unit = {
    poller.foreach { exec =>
      val pollingTask: Runnable = () => Utils.logUncaughtExceptions(poll())
      exec.scheduleAtFixedRate(pollingTask, 0L, pollingInterval, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Task启动时调用，由TaskRunner触发，初始化Task指标追踪并更新Stage任务计数
   * 
   * @param taskId 当前启动的任务ID
   * @param stageId 任务所属Stage ID
   * @param stageAttemptId 任务所属Stage尝试ID
   */
  def onTaskStart(taskId: Long, stageId: Int, stageAttemptId: Int): Unit = {
    // 初始化当前Task的峰值数组，初始值全为0
    taskMetricPeaks.put(taskId, new AtomicLongArray(ExecutorMetricType.numMetrics))

    // 更新Stage状态：如果是该Stage第一个任务则新建条目，否则增加运行任务计数
    val countAndPeaks = stageTCMP.compute((stageId, stageAttemptId), (k: StageKey, v: TCMP) =>
      if (v == null) {
        TCMP(1L, new AtomicLongArray(ExecutorMetricType.numMetrics))
      } else {
        TCMP(v.count + 1, v.peaks)
      })
    logDebug(s"stageTCMP: ($stageId, $stageAttemptId) -> ${countAndPeaks.count}")
  }

  /**
   * Task完成时调用，由TaskRunner触发，减少Stage运行任务计数，移除Task指标追踪
   * 必须保证onTaskStart已被调用过
   * 
   * @param taskId 完成的任务ID
   * @param stageId 任务所属Stage ID
   * @param stageAttemptId 任务所属Stage尝试ID
   */
  def onTaskCompletion(taskId: Long, stageId: Int, stageAttemptId: Int): Unit = {
    // 减少Stage运行任务计数
    def decrementCount(stage: StageKey, countAndPeaks: TCMP): TCMP = {
      val countValue = countAndPeaks.count - 1
      assert(countValue >= 0, "task count shouldn't below 0")
      logDebug(s"stageTCMP: (${stage._1}, ${stage._2}) -> " + countValue)
      TCMP(countValue, countAndPeaks.peaks)
    }

    stageTCMP.computeIfPresent((stageId, stageAttemptId), decrementCount)

    // 从Task峰值映射中移除已完成任务
    taskMetricPeaks.remove(taskId)
  }

  /**
   * 获取指定Task的所有指标峰值，用于填充Task结果返回给Driver
   * 
   * @param taskId 目标任务ID
   * @return 指标峰值数组，索引对应指标类型，若任务不存在则返回全零数组
   */
  def getTaskMetricPeaks(taskId: Long): Array[Long] = {
    val currentPeaks = taskMetricPeaks.get(taskId)
    val metricPeaks = new Array[Long](ExecutorMetricType.numMetrics)
    if (currentPeaks != null) {
      ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
        metricPeaks(i) = currentPeaks.get(i)
      }
    }
    metricPeaks
  }


  /**
   * 心跳汇报时调用，获取自上次心跳以来所有Stage的指标峰值，重置内部峰值以便下次统计
   * 同时清理没有运行任务的Stage条目，避免内存泄漏
   * 
   * @return 每个Stage的指标峰值映射，用于随心跳发送给Driver
   */
  def getExecutorUpdates(): HashMap[StageKey, ExecutorMetrics] = {
    val executorUpdates = new HashMap[StageKey, ExecutorMetrics]

    // 取出当前峰值保存到结果，然后重置峰值数组为全零
    def getUpdateAndResetPeaks(k: StageKey, v: TCMP): TCMP = {
      executorUpdates.put(k, new ExecutorMetrics(v.peaks))
      TCMP(v.count, new AtomicLongArray(ExecutorMetricType.numMetrics))
    }

    stageTCMP.replaceAll(getUpdateAndResetPeaks)

    // 如果Stage运行任务计数为0，移除该Stage条目
    def removeIfInactive(k: StageKey, v: TCMP): TCMP = {
      if (v.count == 0) {
        logDebug(s"removing (${k._1}, ${k._2}) from stageTCMP")
        null
      } else {
        v
      }
    }

    // 遍历所有需要汇报的Stage，清理已完成所有任务的Stage
    executorUpdates.foreach { case (k, _) =>
      stageTCMP.computeIfPresent(k, removeIfInactive)
    }

    executorUpdates
  }

  /** 停止轮询线程，最多等待10秒完成终止 */
  def stop(): Unit = {
    poller.foreach { exec =>
      exec.shutdown()
      exec.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
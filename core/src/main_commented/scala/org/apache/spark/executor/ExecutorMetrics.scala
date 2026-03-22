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

import java.util.concurrent.atomic.AtomicLongArray

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.memory.MemoryManager
import org.apache.spark.metrics.ExecutorMetricType

/**
 * 文件描述：Executor 运行指标统计类，负责收集和存储 Executor/Driver 的资源使用 metrics
 * 核心功能：提供多种构造方式、峰值更新能力，供心跳采集和监控展示使用
 */

/**
 * :: DeveloperApi ::
 * Metrics tracked for executors and the driver.
 *
 * Executor-level metrics are sent from each executor to the driver as part of the Heartbeat.
 *
 * Executor 级别指标类，用于收集和传输 Executor/Driver 进程的资源使用情况。
 *
 * 【采集的指标类型】
 * 本类收集的指标由 ExecutorMetricType 定义，主要包括：
 * - JVM 堆内存使用：heap used、heap committed
 * - JVM 非堆内存使用：non-heap used、non-heap committed
 * - 内存池细分：Eden、Old Gen、Survivor 等各代内存
 * - 直接内存和 Memory-mapped 缓冲区
 * - Spark 执行内存和存储内存使用量
 * - 进程级指标（如 ProcessTreeMetrics）
 *
 * 【数据流转】
 * 1. Executor 定期（通过心跳）采集当前指标值
 * 2. 指标随心跳消息发送到 Driver
 * 3. Driver 端聚合各 Executor 的指标用于监控和 UI 展示
 *
 * 【设计特点】
 * - 使用数组存储指标值，通过 ExecutorMetricType.metricToOffset 映射名称到索引
 * - 支持峰值追踪：compareAndUpdatePeakValues 方法可更新历史峰值
 * - 首元素初始化为 -1 用于标识指标是否已被设置
 */
@DeveloperApi
class ExecutorMetrics private[spark] extends Serializable {
  // 指标值数组，索引由 ExecutorMetricType.metricToOffset 定义
  private val metrics = new Array[Long](ExecutorMetricType.numMetrics)
  // 首元素初始化为 -1，标识指标尚未被设置
  // 设置指标时会覆盖此值，因此可通过 metrics(0) > -1 判断是否已初始化
  metrics(0) = -1

  /** Returns the value for the specified metric. */
  /** 根据指标名称获取对应的值 */
  def getMetricValue(metricName: String): Long = {
    metrics(ExecutorMetricType.metricToOffset(metricName))
  }

  /** Returns true if the values for the metrics have been set, false otherwise. */
  /** 判断指标值是否已被设置 */
  def isSet(): Boolean = metrics(0) > -1

  // 从 Long 数组构造，用于从采集结果创建实例
  private[spark] def this(metrics: Array[Long]) = {
    this()
    Array.copy(metrics, 0, this.metrics, 0, Math.min(metrics.length, this.metrics.length))
  }

  // 从 AtomicLongArray 构造，支持并发场景下的安全读取
  private[spark] def this(metrics: AtomicLongArray) = {
    this()
    ExecutorMetricType.metricToOffset.foreach { case (_, i) =>
      this.metrics(i) = metrics.get(i)
    }
  }

  /**
   * Constructor: create the ExecutorMetrics using a given map.
   *
   * 从 Map 构造，便于从序列化数据或配置中创建实例
   *
   * @param executorMetrics map of executor metric name to value
   */
  private[spark] def this(executorMetrics: Map[String, Long]) = {
    this()
    ExecutorMetricType.metricToOffset.foreach { case (name, idx) =>
      metrics(idx) = executorMetrics.getOrElse(name, 0L)
    }
  }

  /**
   * Compare the specified executor metrics values with the current executor metric values,
   * and update the value for any metrics where the new value for the metric is larger.
   *
   * 比较并更新峰值指标。
   * 遍历所有指标，如果传入的值大于当前值则更新。
   * 用于追踪 Executor 生命周期内的资源使用峰值。
   *
   * @param executorMetrics the executor metrics to compare
   * @return if there is a new peak value for any metric
   */
  private[spark] def compareAndUpdatePeakValues(executorMetrics: ExecutorMetrics): Boolean = {
    var updated = false
    (0 until ExecutorMetricType.numMetrics).foreach { idx =>
      if (executorMetrics.metrics(idx) > metrics(idx)) {
        updated = true
        metrics(idx) = executorMetrics.metrics(idx)
      }
    }
    updated
  }
}

/**
 * ExecutorMetrics 伴生对象，提供静态方法用于采集当前指标
 */
private[spark] object ExecutorMetrics {

  /**
   * Get the current executor metrics. These are returned as an array, with the index
   * determined by ExecutorMetricType.metricToOffset.
   *
   * 采集当前 Executor 的所有指标值。
   * 遍历所有已注册的指标类型（ExecutorMetricType），调用各自的 getMetricValues 方法
   * 获取当前值，并按顺序填充到结果数组中。
   *
   * @param memoryManager the memory manager for execution and storage memory
   * @return the values of the metrics
   */
  def getCurrentMetrics(memoryManager: MemoryManager): Array[Long] = {
    val currentMetrics = new Array[Long](ExecutorMetricType.numMetrics)
    var offset = 0
    // 按顺序遍历所有指标类型，每种类型可能产生多个指标值
    ExecutorMetricType.metricGetters.foreach { metricType =>
      val metricValues = metricType.getMetricValues(memoryManager)
      Array.copy(metricValues, 0, currentMetrics, offset, metricValues.length)
      offset += metricValues.length
    }
    currentMetrics
  }
}
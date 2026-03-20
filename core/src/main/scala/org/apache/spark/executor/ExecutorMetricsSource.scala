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

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.{ExecutorMetricType, MetricsSystem}
import org.apache.spark.metrics.source.Source

/**
 * Expose executor metrics from [[ExecutorMetricsType]] using the Dropwizard metrics system.
 *
 * Metrics related to the memory system can be expensive to gather, therefore
 * we implement some optimizations:
 * (1) Metrics values are cached, updated at each heartbeat (default period is 10 seconds).
 * An alternative faster polling mechanism is used, only if activated, by setting
 * spark.executor.metrics.pollingInterval=<interval in ms>.
 * (2) Procfs metrics are gathered all in one-go and only conditionally:
 * if the /proc filesystem exists
 * and spark.executor.processTreeMetrics.enabled=true.
 *
 * Executor 指标数据源，通过 Dropwizard Metrics 系统暴露 ExecutorMetricType 定义的指标。
 *
 * 【性能优化】
 * 由于内存相关指标的采集开销较大，本类实现了以下优化：
 * 1. 指标缓存：指标值被缓存在 metricsSnapshot 中，由 ExecutorMetricsPoller 定期更新
 *    - 默认通过心跳更新（间隔约 10 秒）
 *    - 可配置 spark.executor.metrics.pollingInterval 启用更高频的独立轮询
 * 2. Procfs 批量采集：/proc 文件系统指标一次性批量读取
 *    - 仅在 Linux 系统且 spark.executor.processTreeMetrics.enabled=true 时启用
 *
 * 【与其他组件的关系】
 * - ExecutorMetricsPoller：负责定期调用 updateMetricsSnapshot() 更新缓存
 * - MetricsSystem：本类注册到 MetricsSystem 后，Gauge 指标被暴露给外部监控系统
 */
private[spark] class ExecutorMetricsSource extends Source {

  override val metricRegistry = new MetricRegistry()
  override val sourceName = "ExecutorMetrics"

  // 指标快照缓存，由 ExecutorMetricsPoller 定期更新
  // 使用 @volatile 保证多线程可见性
  @volatile var metricsSnapshot: Array[Long] = Array.fill(ExecutorMetricType.numMetrics)(0L)

  // 由 ExecutorMetricsPoller 调用以更新指标快照
  def updateMetricsSnapshot(metricsUpdates: Array[Long]): Unit = {
    metricsSnapshot = metricsUpdates
  }

  // Gauge 实现：从快照数组中读取对应索引的值
  private class ExecutorMetricGauge(idx: Int) extends Gauge[Long] {
    def getValue: Long = metricsSnapshot(idx)
  }

  /**
   * 注册所有指标到 MetricsSystem。
   * 为每种指标类型创建一个 Gauge，Gauge 读取时从 metricsSnapshot 获取最新值。
   */
  def register(metricsSystem: MetricsSystem): Unit = {
    // 为每个指标索引创建 Gauge
    val gauges: IndexedSeq[ExecutorMetricGauge] = (0 until ExecutorMetricType.numMetrics).map {
      idx => new ExecutorMetricGauge(idx)
    }

    // 按名称注册所有 Gauge
    ExecutorMetricType.metricToOffset.foreach {
      case (name, idx) =>
        metricRegistry.register(MetricRegistry.name(name), gauges(idx))
    }

    metricsSystem.registerSource(this)
  }
}

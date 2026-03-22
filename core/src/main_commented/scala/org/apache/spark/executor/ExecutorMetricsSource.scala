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

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.{ExecutorMetricType, MetricsSystem}
import org.apache.spark.metrics.source.Source

/**
 * 文件：org.apache.spark.executor.ExecutorMetricsSource
 * 所属模块：Spark Core
 * 核心职责：基于 Dropwizard 指标系统暴露 Executor 运行指标，提供给监控系统采集
 * 
 * 说明：暴露[[ExecutorMetricType]]中定义的Executor指标到 Dropwizard 指标系统。
 *
 * 由于内存系统相关指标采集开销较高，实现了以下优化：
 * (1) 指标值缓存，每次心跳更新一次（默认间隔10秒）。
 * 若配置了spark.executor.metrics.pollingInterval参数，则会启用更快的独立轮询机制。
 * (2) /proc文件系统指标一次性批量采集，仅在满足条件时启用：
 * /proc文件系统存在且spark.executor.processTreeMetrics.enabled=true。
 */
private[spark] class ExecutorMetricsSource extends Source {

  override val metricRegistry = new MetricRegistry()
  override val sourceName = "ExecutorMetrics"

  // 指标快照缓存，由轮询线程定期更新，volatile保证多线程可见性
  @volatile var metricsSnapshot: Array[Long] = Array.fill(ExecutorMetricType.numMetrics)(0L)

  /**
   * 更新指标快照缓存，由Executor指标轮询器调用
   * @param metricsUpdates 最新的指标值数组
   */
  def updateMetricsSnapshot(metricsUpdates: Array[Long]): Unit = {
    metricsSnapshot = metricsUpdates
  }

  /**
   * 自定义Gauge实现，从缓存快照中读取对应位置的指标值
   * @param idx 指标在快照数组中的索引
   */
  private class ExecutorMetricGauge(idx: Int) extends Gauge[Long] {
    def getValue: Long = metricsSnapshot(idx)
  }

  /**
   * 将所有Executor指标注册到Metrics系统，完成暴露
   * @param metricsSystem Spark指标系统实例
   */
  def register(metricsSystem: MetricsSystem): Unit = {
    // 为每个指标创建对应的Gauge实例
    val gauges: IndexedSeq[ExecutorMetricGauge] = (0 until ExecutorMetricType.numMetrics).map {
      idx => new ExecutorMetricGauge(idx)
    }

    // 按指标名称注册所有Gauge到度量注册表
    ExecutorMetricType.metricToOffset.foreach {
      case (name, idx) =>
        metricRegistry.register(MetricRegistry.name(name), gauges(idx))
    }

    // 将当前数据源注册到Metrics系统
    metricsSystem.registerSource(this)
  }
}
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

import com.codahale.metrics.{Gauge, MetricRegistry, Timer}

import org.apache.spark.metrics.source.Source

/**
 * DAGScheduler的度量指标源，通过Codahale Metrics库暴露以下指标：
 * - stage.failedStages：失败的Stage数量
 * - stage.runningStages：正在运行的Stage数量
 * - stage.waitingStages：等待中的Stage数量
 * - job.allJobs：总作业数
 * - job.activeJobs：活跃作业数
 * - messageProcessingTime：事件循环中消息处理耗时
 */
private[scheduler] class DAGSchedulerSource(val dagScheduler: DAGScheduler)
    extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "DAGScheduler"

  // 注册失败Stage数量指标
  metricRegistry.register(MetricRegistry.name("stage", "failedStages"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.failedStages.size
  })

  // 注册运行中Stage数量指标
  metricRegistry.register(MetricRegistry.name("stage", "runningStages"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.runningStages.size
  })

  // 注册等待中Stage数量指标
  metricRegistry.register(MetricRegistry.name("stage", "waitingStages"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.waitingStages.size
  })

  // 注册总作业数指标
  metricRegistry.register(MetricRegistry.name("job", "allJobs"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.numTotalJobs
  })

  // 注册活跃作业数指标
  metricRegistry.register(MetricRegistry.name("job", "activeJobs"), new Gauge[Int] {
    override def getValue: Int = dagScheduler.activeJobs.size
  })

  /** 跟踪DAGScheduler事件循环中消息处理时间的计时器 */
  val messageProcessingTimer: Timer =
    metricRegistry.timer(MetricRegistry.name("messageProcessingTime"))
}

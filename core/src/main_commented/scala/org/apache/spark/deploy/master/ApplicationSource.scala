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

package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
 * Spark Master 应用指标来源，为单个提交的Spark应用导出运行状态指标，供监控系统采集
 * 负责将应用的核心运行状态暴露为DropwizardMetrics可采集的指标
 */
private[master] class ApplicationSource(val application: ApplicationInfo) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "%s.%s.%s".format("application", application.desc.name,
    System.currentTimeMillis())

  // 注册应用状态指标，返回应用当前运行状态
  metricRegistry.register(MetricRegistry.name("status"), new Gauge[String] {
    override def getValue: String = application.state.toString
  })

  // 注册应用运行时长指标，返回应用已运行时间（单位毫秒）
  metricRegistry.register(MetricRegistry.name("runtime_ms"), new Gauge[Long] {
    override def getValue: Long = application.duration
  })

  // 注册分配核数指标，返回当前已分配给应用的CPU核数
  metricRegistry.register(MetricRegistry.name("cores"), new Gauge[Int] {
    override def getValue: Int = application.coresGranted
  })

}
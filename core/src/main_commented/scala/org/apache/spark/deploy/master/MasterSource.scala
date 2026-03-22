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
 * Spark Standalone集群Master指标源，用于向监控系统暴露Master状态相关指标
 * 核心职责：注册Master当前集群状态相关的度量指标，供Metrics系统采集展示
 */
private[spark] class MasterSource(val master: Master) extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "master"

  // 注册集群总Worker数量指标
  metricRegistry.register(MetricRegistry.name("workers"), new Gauge[Int] {
    override def getValue: Int = master.workers.size
  })

  // 注册集群存活Worker数量指标
  metricRegistry.register(MetricRegistry.name("aliveWorkers"), new Gauge[Int]{
    override def getValue: Int = master.workers.count(_.state == WorkerState.ALIVE)
  })

  // 注册集群总应用数量指标
  metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
    override def getValue: Int = master.apps.size
  })

  // 注册集群等待调度应用数量指标
  metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
    override def getValue: Int = master.apps.count(_.state == ApplicationState.WAITING)
  })
}
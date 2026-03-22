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

package org.apache.spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
 * Worker节点度量指标源，向Spark监控系统暴露Worker节点运行状态指标
 * 核心职责：注册Worker节点关键资源和运行状态的度量指标，供监控系统采集
 */
private[worker] class WorkerSource(val worker: Worker) extends Source {
  override val sourceName = "worker"
  override val metricRegistry = new MetricRegistry()

  // 注册当前Worker运行的Executor数量指标
  metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
    override def getValue: Int = worker.executors.size
  })

  // 注册当前Worker已使用CPU核心数指标
  metricRegistry.register(MetricRegistry.name("coresUsed"), new Gauge[Int] {
    override def getValue: Int = worker.coresUsed
  })

  // 注册当前Worker已使用内存大小指标(单位:MB)
  metricRegistry.register(MetricRegistry.name("memUsed_MB"), new Gauge[Int] {
    override def getValue: Int = worker.memoryUsed
  })

  // 注册当前Worker剩余可用CPU核心数指标
  metricRegistry.register(MetricRegistry.name("coresFree"), new Gauge[Int] {
    override def getValue: Int = worker.coresFree
  })

  // 注册当前Worker剩余可用内存大小指标(单位:MB)
  metricRegistry.register(MetricRegistry.name("memFree_MB"), new Gauge[Int] {
    override def getValue: Int = worker.memoryFree
  })
}
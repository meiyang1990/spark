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

package org.apache.spark.deploy

import javax.annotation.concurrent.ThreadSafe

import com.codahale.metrics.{MetricRegistry, MetricSet}

import org.apache.spark.metrics.source.Source

/**
 * 文件说明: 为外部Shuffle服务提供指标监控数据源，用于将Shuffle服务运行指标暴露给Spark指标系统
 * 
 * 类说明: 外部Shuffle服务的指标数据源实现，负责注册和管理Shuffle服务的可观测指标
 */
@ThreadSafe
private class ExternalShuffleServiceSource extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "shuffleService"

  /**
   * 注册一组Shuffle服务指标到指标注册表中
   * @param metricSet 需要注册的指标集合
   */
  def registerMetricSet(metricSet: MetricSet): Unit = {
    metricRegistry.registerAll(metricSet)
  }
}
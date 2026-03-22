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

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

/**
 * Spark监控指标数据源抽象接口
 * 所有Spark内部指标来源都需要实现该接口，提供指标名称和注册到MetricRegistry的入口
 * 供Spark监控系统统一收集和暴露不同组件的运行指标
 */
private[spark] trait Source {
  /** 数据源名称，用于指标分类标识 */
  def sourceName: String
  /** 指标注册器，所有该来源的指标都注册在此Registry中 */
  def metricRegistry: MetricRegistry
}
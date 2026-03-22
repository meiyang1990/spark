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

package org.apache.spark.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricRegistry, Slf4jReporter}

import org.apache.spark.metrics.MetricsSystem

/**
 * 将Spark指标数据输出到Slf4j日志框架的指标接收器
 * 实现了Sink接口，按照配置周期定期将收集的指标写入日志
 * @param property 配置属性，包含拉取周期和时间单位配置
 * @param registry 指标注册表，包含所有需要上报的指标
 */
private[spark] class Slf4jSink(
    val property: Properties, val registry: MetricRegistry) extends Sink {
  // Slf4j默认报告周期：10单位
  val SLF4J_DEFAULT_PERIOD = 10
  // 默认时间单位：秒
  val SLF4J_DEFAULT_UNIT = "SECONDS"

  // 配置键：报告周期
  val SLF4J_KEY_PERIOD = "period"
  // 配置键：时间单位
  val SLF4J_KEY_UNIT = "unit"

  // 读取报告周期配置，使用默认值如果未配置
  val pollPeriod = Option(property.getProperty(SLF4J_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => SLF4J_DEFAULT_PERIOD
  }

  // 读取时间单位配置，使用默认值如果未配置
  val pollUnit: TimeUnit = Option(property.getProperty(SLF4J_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(SLF4J_DEFAULT_UNIT)
  }

  // 检查最小拉取周期，防止过于频繁的指标上报
  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  // 构建Slf4j指标报告器，设置时间单位转换规则
  val reporter: Slf4jReporter = Slf4jReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .build()

  /**
   * 启动指标报告，按配置周期定时上报指标到Slf4j日志
   */
  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  /**
   * 停止指标报告，清理资源
   */
  override def stop(): Unit = {
    reporter.stop()
  }

  /**
   * 手动触发一次指标上报
   */
  override def report(): Unit = {
    reporter.report()
  }
}
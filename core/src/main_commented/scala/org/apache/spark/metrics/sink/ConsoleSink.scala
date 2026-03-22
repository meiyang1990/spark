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

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}

import org.apache.spark.metrics.MetricsSystem

/**
 * 将指标输出到控制台的指标接收器实现
 * 用于开发调试场景，定期将Spark收集的各项监控指标打印到控制台输出
 * 实现了Sink接口，遵循可插拔指标接收器设计
 */
private[spark] class ConsoleSink(
    val property: Properties, val registry: MetricRegistry) extends Sink {
  // 默认报告周期：10秒
  val CONSOLE_DEFAULT_PERIOD = 10
  // 默认时间单位：秒
  val CONSOLE_DEFAULT_UNIT = "SECONDS"

  // 配置键：报告周期
  val CONSOLE_KEY_PERIOD = "period"
  // 配置键：时间单位
  val CONSOLE_KEY_UNIT = "unit"

  // 读取配置的报告周期，未配置则使用默认值
  val pollPeriod = Option(property.getProperty(CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CONSOLE_DEFAULT_PERIOD
  }

  // 读取配置的时间单位，未配置则使用默认值
  val pollUnit: TimeUnit = Option(property.getProperty(CONSOLE_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(CONSOLE_DEFAULT_UNIT)
  }

  // 检查报告周期是否满足最小间隔要求，避免过于频繁的指标输出
  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  // 初始化Dropwizard Metrics控制台报告器
  // 设置持续时间单位为毫秒，比率单位为秒
  val reporter: ConsoleReporter = ConsoleReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build()

  /**
   * 启动控制台指标报告，按照配置周期定期输出指标到控制台
   */
  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  /**
   * 停止控制台指标报告，关闭报告器
   */
  override def stop(): Unit = {
    reporter.stop()
  }

  /**
   * 手动触发一次指标输出到控制台
   */
  override def report(): Unit = {
    reporter.report()
  }
}
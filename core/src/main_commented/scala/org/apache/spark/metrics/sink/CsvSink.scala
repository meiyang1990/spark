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

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{CsvReporter, MetricRegistry}

import org.apache.spark.metrics.MetricsSystem

/**
 * CSV格式指标输出Sink，将Spark指标以CSV格式定期输出到指定目录文件
 * 实现了Sink接口，作为Spark指标系统的输出插件之一
 * @param property 配置属性，包含输出周期、时间单位、输出目录等配置
 * @param registry 指标注册表，包含所有需要输出的指标
 */
private[spark] class CsvSink(
    val property: Properties, val registry: MetricRegistry) extends Sink {
  // 配置项键：输出周期
  val CSV_KEY_PERIOD = "period"
  // 配置项键：时间单位
  val CSV_KEY_UNIT = "unit"
  // 配置项键：输出目录
  val CSV_KEY_DIR = "directory"

  // 默认输出周期：10单位
  val CSV_DEFAULT_PERIOD = 10
  // 默认时间单位：秒
  val CSV_DEFAULT_UNIT = "SECONDS"
  // 默认输出目录：/tmp/
  val CSV_DEFAULT_DIR = "/tmp/"

  // 读取并解析输出周期，使用默认值当配置不存在
  val pollPeriod = Option(property.getProperty(CSV_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CSV_DEFAULT_PERIOD
  }

  // 读取并解析时间单位，使用默认值当配置不存在
  val pollUnit: TimeUnit = Option(property.getProperty(CSV_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(CSV_DEFAULT_UNIT)
  }

  // 检查轮询周期不小于系统要求的最小间隔，防止过于频繁的指标输出
  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  // 读取输出目录，使用默认值当配置不存在
  val pollDir = Option(property.getProperty(CSV_KEY_DIR)) match {
    case Some(s) => s
    case None => CSV_DEFAULT_DIR
  }

  // 构建CsvReporter实例，配置区域格式、时间单位和输出目录
  val reporter: CsvReporter = CsvReporter.forRegistry(registry)
      .formatFor(Locale.US)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(pollDir))

  /**
   * 启动CSV指标输出，开始按指定周期定期输出指标
   */
  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  /**
   * 停止CSV指标输出，关闭Reporter
   */
  override def stop(): Unit = {
    reporter.stop()
  }

  /**
   * 手动触发一次指标输出
   */
  override def report(): Unit = {
    reporter.report()
  }
}
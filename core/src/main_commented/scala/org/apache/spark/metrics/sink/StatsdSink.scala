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

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PREFIX
import org.apache.spark.metrics.MetricsSystem

/**
 * Statsd指标接收器配置常量定义，提供配置项键名和默认值
 */
private[spark] object StatsdSink {
  val STATSD_KEY_HOST = "host"
  val STATSD_KEY_PORT = "port"
  val STATSD_KEY_PERIOD = "period"
  val STATSD_KEY_UNIT = "unit"
  val STATSD_KEY_PREFIX = "prefix"
  val STATSD_KEY_REGEX = "regex"

  val STATSD_DEFAULT_HOST = "127.0.0.1"
  val STATSD_DEFAULT_PORT = "8125"
  val STATSD_DEFAULT_PERIOD = "10"
  val STATSD_DEFAULT_UNIT = "SECONDS"
  val STATSD_DEFAULT_PREFIX = ""
}

/**
 * 将Spark指标输出到Statsd服务的指标接收器
 * 负责从配置中读取参数，初始化StatsdReporter，并按照周期上报指标
 * @param property 配置属性对象，包含Statsd连接和上报参数
 * @param registry 指标注册表，包含需要上报的所有指标
 */
private[spark] class StatsdSink(
    val property: Properties, val registry: MetricRegistry) extends Sink with Logging {
  import StatsdSink._

  // 读取Statsd服务器地址，使用默认值127.0.0.1
  val host = property.getProperty(STATSD_KEY_HOST, STATSD_DEFAULT_HOST)
  // 读取Statsd服务器端口，转换为整数，使用默认值8125
  val port = property.getProperty(STATSD_KEY_PORT, STATSD_DEFAULT_PORT).toInt

  // 读取上报周期，转换为整数，使用默认值10
  val pollPeriod = property.getProperty(STATSD_KEY_PERIOD, STATSD_DEFAULT_PERIOD).toInt
  // 读取时间单位，转换为大写后解析为TimeUnit枚举，默认使用秒
  val pollUnit =
    TimeUnit.valueOf(
      property.getProperty(STATSD_KEY_UNIT, STATSD_DEFAULT_UNIT).toUpperCase(Locale.ROOT))

  // 读取指标前缀，用于所有上报指标的命名前缀，默认为空
  val prefix = property.getProperty(STATSD_KEY_PREFIX, STATSD_DEFAULT_PREFIX)

  // 根据配置的正则表达式创建指标过滤器，只上报匹配名称的指标
  val filter = Option(property.getProperty(STATSD_KEY_REGEX)) match {
    case Some(pattern) => new MetricFilter() {
      override def matches(name: String, metric: Metric): Boolean = {
        pattern.r.findFirstMatchIn(name).isDefined
      }
    }
    // 未配置正则表达式时，上报所有指标
    case None => MetricFilter.ALL
  }

  // 检查上报周期是否满足最小间隔要求，避免过于频繁上报
  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  // 创建StatsdReporter实例，负责实际的指标上报逻辑
  val reporter = new StatsdReporter(registry, host, port, prefix, filter)

  /**
   * 启动Statsd指标接收器，开始周期性上报指标
   */
  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
    logInfo(log"StatsdSink started with prefix: '${MDC(PREFIX, prefix)}'")
  }

  /**
   * 停止Statsd指标接收器，终止周期性上报并清理资源
   */
  override def stop(): Unit = {
    reporter.stop()
    logInfo("StatsdSink stopped.")
  }

  /**
   * 手动触发一次指标上报
   */
  override def report(): Unit = reporter.report()
}
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

/**
 * 文件路径: core/src/main/scala/org/apache/spark/metrics/sink/GraphiteSink.scala
 * 所属模块: Spark核心模块(metrics子系统)
 * 核心职责: 实现将Spark监控指标输出到Graphite监控系统的Sinker，支持TCP和UDP两种传输协议
 */
package org.apache.spark.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter, GraphiteUDP}

import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.metrics.MetricsSystem

/**
 * Graphite指标输出Sinker，实现将Spark监控指标推送到外部Graphite监控系统
 * 核心职责: 读取配置初始化Graphite连接，按照指定周期定期上报Spark指标到Graphite
 * @param property 配置参数，包含主机、端口、上报周期、协议等配置项
 * @param registry 指标注册表，保存所有需要上报的监控指标
 */
private[spark] class GraphiteSink(
    val property: Properties, val registry: MetricRegistry) extends Sink {
  val GRAPHITE_DEFAULT_PERIOD = 10
  val GRAPHITE_DEFAULT_UNIT = "SECONDS"
  val GRAPHITE_DEFAULT_PREFIX = ""

  val GRAPHITE_KEY_HOST = "host"
  val GRAPHITE_KEY_PORT = "port"
  val GRAPHITE_KEY_PERIOD = "period"
  val GRAPHITE_KEY_UNIT = "unit"
  val GRAPHITE_KEY_PREFIX = "prefix"
  val GRAPHITE_KEY_PROTOCOL = "protocol"
  val GRAPHITE_KEY_REGEX = "regex"

  /** 将配置属性转换为Option，空配置返回None */
  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  // 检查主机配置是否存在，不存在则抛出异常
  if (propertyToOption(GRAPHITE_KEY_HOST).isEmpty) {
    throw SparkCoreErrors.graphiteSinkPropertyMissingError("host")
  }

  // 检查端口配置是否存在，不存在则抛出异常
  if (propertyToOption(GRAPHITE_KEY_PORT).isEmpty) {
    throw SparkCoreErrors.graphiteSinkPropertyMissingError("port")
  }

  // 读取配置得到Graphite服务器主机地址
  val host = propertyToOption(GRAPHITE_KEY_HOST).get
  // 读取配置并转换得到Graphite服务器端口
  val port = propertyToOption(GRAPHITE_KEY_PORT).get.toInt

  // 读取上报周期配置，无配置则使用默认值10秒
  val pollPeriod = propertyToOption(GRAPHITE_KEY_PERIOD) match {
    case Some(s) => s.toInt
    case None => GRAPHITE_DEFAULT_PERIOD
  }

  // 读取时间单位配置，无配置则默认使用秒
  val pollUnit: TimeUnit = propertyToOption(GRAPHITE_KEY_UNIT) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(GRAPHITE_DEFAULT_UNIT)
  }

  // 读取指标前缀配置，无配置则使用空前缀
  val prefix = propertyToOption(GRAPHITE_KEY_PREFIX).getOrElse(GRAPHITE_DEFAULT_PREFIX)

  // 检查上报周期是否满足最小间隔要求，避免过于频繁上报影响性能
  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  // 根据配置协议初始化Graphite客户端，默认使用TCP协议
  val graphite = propertyToOption(GRAPHITE_KEY_PROTOCOL).map(_.toLowerCase(Locale.ROOT)) match {
    case Some("udp") => new GraphiteUDP(host, port)
    case Some("tcp") | None => new Graphite(host, port)
    case Some(p) => throw SparkCoreErrors.graphiteSinkInvalidProtocolError(p)
  }

  // 根据正则表达式配置创建指标过滤器，只上报匹配正则的指标，无配置则上报所有指标
  val filter = propertyToOption(GRAPHITE_KEY_REGEX) match {
    case Some(pattern) => new MetricFilter() {
      override def matches(name: String, metric: Metric): Boolean = {
        pattern.r.findFirstMatchIn(name).isDefined
      }
    }
    case None => MetricFilter.ALL
  }

  // 构建Graphite指标上报器，配置单位转换、前缀和过滤器
  val reporter: GraphiteReporter = GraphiteReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .prefixedWith(prefix)
      .filter(filter)
      .build(graphite)

  /**
   * 启动指标上报任务，按照配置周期定期上报指标到Graphite
   */
  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  /**
   * 停止指标上报任务，关闭Graphite连接
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
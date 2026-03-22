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

import java.util.Properties

import com.codahale.metrics.MetricRegistry
import jakarta.servlet.http.HttpServletRequest
import org.eclipse.jetty.ee10.servlet.ServletContextHandler

import org.apache.spark.SparkConf
import org.apache.spark.annotation.{DeveloperApi, Since, Unstable}
import org.apache.spark.ui.JettyUtils._

/**
 * 文件概述: Prometheus格式指标输出Servlet接收器，实现Spark指标对Prometheus监控系统的 exposition 支持
 * 核心职责: 提供HTTP端点供Prometheus拉取Spark节点指标，输出符合Prometheus格式的监控数据，保持与原有Spark指标格式兼容性
 */
/**
 * :: DeveloperApi ::
 * This exposes the metrics of the given registry with Prometheus format.
 *
 * The output is consistent with /metrics/json result in terms of item ordering
 * and with the previous result of Spark JMX Sink + Prometheus JMX Converter combination
 * in terms of key string format.
 *
 * This is used by Spark MetricsSystem internally and Spark K8s operator.
 */
@Unstable
@DeveloperApi
private[spark] class PrometheusServlet(
    val property: Properties, val registry: MetricRegistry) extends Sink {

  // 配置属性中Servlet路径的键名
  val SERVLET_KEY_PATH = "path"

  // 从配置中读取Servlet挂载路径
  val servletPath = property.getProperty(SERVLET_KEY_PATH)

  /**
   * 创建Prometheus指标端点的Jetty Servlet处理器
   * @param conf Spark配置对象
   * @return 包含Servlet上下文处理器的数组
   */
  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
    Array[ServletContextHandler](
      createServletHandler(servletPath,
        new ServletParams(request => getMetricsSnapshot(request), "text/plain"), conf)
    )
  }

  /**
   * 处理HTTP请求，返回Prometheus格式的指标快照
   * @param request HTTP请求对象
   * @return Prometheus格式指标字符串
   */
  def getMetricsSnapshot(request: HttpServletRequest): String = getMetricsSnapshot()

  /**
   * 生成Prometheus格式的全量指标快照
   * @return 格式化后的Prometheus指标字符串
   * @since 4.0.0
   */
  @Since("4.0.0")
  def getMetricsSnapshot(): String = {
    import scala.jdk.CollectionConverters._

    // Gauges类型指标的Prometheus标签
    val gaugesLabel = """{type="gauges"}"""
    // Counters类型指标的Prometheus标签
    val countersLabel = """{type="counters"}"""
    // Meters类型指标复用Counter标签
    val metersLabel = countersLabel
    // Histograms类型指标的Prometheus标签
    val histogramslabels = """{type="histograms"}"""
    // Timers类型指标的Prometheus标签
    val timersLabels = """{type="timers"}"""

    // 拼接结果字符串的StringBuilder
    val sb = new StringBuilder()
    // 遍历所有Gauge指标，跳过字符串类型值，输出Number和Value两个指标
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        sb.append(s"${normalizeKey(k)}Number$gaugesLabel ${v.getValue}\n")
        sb.append(s"${normalizeKey(k)}Value$gaugesLabel ${v.getValue}\n")
      }
    }
    // 遍历所有Counter指标，输出计数指标
    registry.getCounters.asScala.foreach { case (k, v) =>
      sb.append(s"${normalizeKey(k)}Count$countersLabel ${v.getCount}\n")
    }
    // 遍历所有Histogram指标，输出计数和各分位数统计值
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val prefix = normalizeKey(k)
      sb.append(s"${prefix}Count$histogramslabels ${h.getCount}\n")
      sb.append(s"${prefix}Max$histogramslabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$histogramslabels ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min$histogramslabels ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$histogramslabels ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$histogramslabels ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$histogramslabels ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$histogramslabels ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$histogramslabels ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$histogramslabels ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$histogramslabels ${snapshot.getStdDev}\n")
    }
    // 遍历所有Meter指标，输出计数和各时间窗口速率
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val meter = kv.getValue
      sb.append(s"${prefix}Count$metersLabel ${meter.getCount}\n")
      sb.append(s"${prefix}MeanRate$metersLabel ${meter.getMeanRate}\n")
      sb.append(s"${prefix}OneMinuteRate$metersLabel ${meter.getOneMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$metersLabel ${meter.getFiveMinuteRate}\n")
      sb.append(s"${prefix}FifteenMinuteRate$metersLabel ${meter.getFifteenMinuteRate}\n")
    }
    // 遍历所有Timer指标，输出计数、分位数统计和速率信息
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      sb.append(s"${prefix}Count$timersLabels ${timer.getCount}\n")
      sb.append(s"${prefix}Max$timersLabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$timersLabels ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min$timersLabels ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$timersLabels ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$timersLabels ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$timersLabels ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$timersLabels ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$timersLabels ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$timersLabels ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$timersLabels ${snapshot.getStdDev}\n")
      sb.append(s"${prefix}FifteenMinuteRate$timersLabels ${timer.getFifteenMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$timersLabels ${timer.getFiveMinuteRate}\n")
      sb.append(s"${prefix}OneMinuteRate$timersLabels ${timer.getOneMinuteRate}\n")
      sb.append(s"${prefix}MeanRate$timersLabels ${timer.getMeanRate}\n")
    }
    sb.toString()
  }

  /**
   * 规范化指标键名，将非法字符替换为下划线，适配Prometheus命名规则
   * @param key 原始指标键名
   * @return 规范化后的指标前缀
   */
  private def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  /**
   * Sink接口启动方法，Servlet接收器无需周期性报告，实现为空
   */
  override def start(): Unit = { }

  /**
   * Sink接口停止方法，Servlet接收器无需额外停止逻辑，实现为空
   */
  override def stop(): Unit = { }

  /**
   * Sink接口报告方法，Servlet接收器通过HTTP拉取而非主动报告，实现为空
   */
  override def report(): Unit = { }
}
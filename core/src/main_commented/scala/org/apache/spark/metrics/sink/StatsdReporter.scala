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

import java.io.IOException
import java.net.{DatagramPacket, DatagramSocket, InetSocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.SortedMap
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import com.codahale.metrics._
import org.apache.hadoop.net.NetUtils

import org.apache.spark.internal.Logging

/**
 * StatsD指标类型常量定义
 * @see <a href="https://github.com/etsy/statsd/blob/master/docs/metric_types.md">
 *        StatsD metric types</a>
 */
private[spark] object StatsdMetricType {
  val COUNTER = "c"
  val GAUGE = "g"
  val TIMER = "ms"
  val Set = "s"
}

/**
 * 将Spark指标数据上报到StatsD的报告器实现
 * 基于UDP协议将各类监控指标以StatsD协议格式发送到指定StatsD服务器，用于监控Spark集群运行状态
 * @param registry 指标注册表，从中获取需要上报的指标
 * @param host StatsD服务器主机地址，默认127.0.0.1
 * @param port StatsD服务器端口，默认8125
 * @param prefix 指标名称前缀，所有上报的指标都会加上该前缀
 * @param filter 指标过滤器，用于过滤需要上报的指标
 * @param rateUnit 速率单位，默认秒
 * @param durationUnit 时长单位，默认毫秒
 */
private[spark] class StatsdReporter(
    registry: MetricRegistry,
    host: String = "127.0.0.1",
    port: Int = 8125,
    prefix: String = "",
    filter: MetricFilter = MetricFilter.ALL,
    rateUnit: TimeUnit = TimeUnit.SECONDS,
    durationUnit: TimeUnit = TimeUnit.MILLISECONDS)
  extends ScheduledReporter(registry, "statsd-reporter", filter, rateUnit, durationUnit)
  with Logging {

  import StatsdMetricType._

  // StatsD服务器地址
  private val address = new InetSocketAddress(host, port)
  // 空白字符正则，用于清洗指标名称中的空白字符
  private val whitespace = "[\\s]+".r

  /**
   * 执行一次指标上报，将所有类型指标批量发送到StatsD服务器
   * @param gauges 所有Gauge类型指标集合
   * @param counters 所有Counter类型指标集合
   * @param histograms 所有Histogram类型指标集合
   * @param meters 所有Meter类型指标集合
   * @param timers 所有Timer类型指标集合
   */
  override def report(
      gauges: SortedMap[String, Gauge[_]],
      counters: SortedMap[String, Counter],
      histograms: SortedMap[String, Histogram],
      meters: SortedMap[String, Meter],
      timers: SortedMap[String, Timer]): Unit =
    // 尝试创建UDP套接字连接StatsD
    Try(new DatagramSocket) match {
      // 创建失败处理：IO异常记录警告日志
      case Failure(ioe: IOException) => logWarning("StatsD datagram socket construction failed",
        NetUtils.wrapException(host, port, NetUtils.getHostname(), 0, ioe))
      // 创建失败处理：其他异常记录警告日志
      case Failure(e) => logWarning("StatsD datagram socket construction failed", e)
      // 创建成功，开始发送指标
      case Success(s) =>
        implicit val socket = s
        // 获取本地套接字地址信息，用于异常日志
        val localAddress = Try(socket.getLocalAddress).map(_.getHostAddress).getOrElse(null)
        val localPort = socket.getLocalPort
        // 依次上报各类指标
        Try {
          gauges.entrySet.asScala.foreach(e => reportGauge(e.getKey, e.getValue))
          counters.entrySet.asScala.foreach(e => reportCounter(e.getKey, e.getValue))
          histograms.entrySet.asScala.foreach(e => reportHistogram(e.getKey, e.getValue))
          meters.entrySet.asScala.foreach(e => reportMetered(e.getKey, e.getValue))
          timers.entrySet.asScala.foreach(e => reportTimer(e.getKey, e.getValue))
        } recover {
          // 发送异常处理：记录调试日志
          case ioe: IOException =>
            logDebug(s"Unable to send packets to StatsD", NetUtils.wrapException(
              address.getHostString, address.getPort, localAddress, localPort, ioe))
          case e: Throwable => logDebug(s"Unable to send packets to StatsD at '$host:$port'", e)
        }
        // 关闭套接字，处理异常
        Try(socket.close()) recover {
          case ioe: IOException =>
            logDebug("Error when close socket to StatsD", NetUtils.wrapException(
              address.getHostString, address.getPort, localAddress, localPort, ioe))
          case e: Throwable => logDebug("Error when close socket to StatsD", e)
        }
    }

  /**
   * 上报Gauge类型指标到StatsD
   */
  private def reportGauge(name: String, gauge: Gauge[_])(implicit socket: DatagramSocket): Unit =
    formatAny(gauge.getValue).foreach(v => send(fullName(name), v, GAUGE))

  /**
   * 上报Counter类型指标到StatsD
   */
  private def reportCounter(name: String, counter: Counter)(implicit socket: DatagramSocket): Unit =
    send(fullName(name), format(counter.getCount), COUNTER)

  /**
   * 上报Histogram类型指标到StatsD，发送分位数、最值、均值、标准差等统计值
   */
  private def reportHistogram(name: String, histogram: Histogram)
      (implicit socket: DatagramSocket): Unit = {
    val snapshot = histogram.getSnapshot
    send(fullName(name, "count"), format(histogram.getCount), GAUGE)
    send(fullName(name, "max"), format(snapshot.getMax), TIMER)
    send(fullName(name, "mean"), format(snapshot.getMean), TIMER)
    send(fullName(name, "min"), format(snapshot.getMin), TIMER)
    send(fullName(name, "stddev"), format(snapshot.getStdDev), TIMER)
    send(fullName(name, "p50"), format(snapshot.getMedian), TIMER)
    send(fullName(name, "p75"), format(snapshot.get75thPercentile), TIMER)
    send(fullName(name, "p95"), format(snapshot.get95thPercentile), TIMER)
    send(fullName(name, "p98"), format(snapshot.get98thPercentile), TIMER)
    send(fullName(name, "p99"), format(snapshot.get99thPercentile), TIMER)
    send(fullName(name, "p999"), format(snapshot.get999thPercentile), TIMER)
  }

  /**
   * 上报Meter类型指标到StatsD，发送计数和各时间窗口速率
   */
  private def reportMetered(name: String, meter: Metered)(implicit socket: DatagramSocket): Unit = {
    send(fullName(name, "count"), format(meter.getCount), GAUGE)
    send(fullName(name, "m1_rate"), format(convertRate(meter.getOneMinuteRate)), TIMER)
    send(fullName(name, "m5_rate"), format(convertRate(meter.getFiveMinuteRate)), TIMER)
    send(fullName(name, "m15_rate"), format(convertRate(meter.getFifteenMinuteRate)), TIMER)
    send(fullName(name, "mean_rate"), format(convertRate(meter.getMeanRate)), TIMER)
  }

  /**
   * 上报Timer类型指标到StatsD，同时发送耗时统计和速率统计
   */
  private def reportTimer(name: String, timer: Timer)(implicit socket: DatagramSocket): Unit = {
    val snapshot = timer.getSnapshot
    send(fullName(name, "max"), format(convertDuration(snapshot.getMax.toDouble)), TIMER)
    send(fullName(name, "mean"), format(convertDuration(snapshot.getMean)), TIMER)
    send(fullName(name, "min"), format(convertDuration(snapshot.getMin.toDouble)), TIMER)
    send(fullName(name, "stddev"), format(convertDuration(snapshot.getStdDev)), TIMER)
    send(fullName(name, "p50"), format(convertDuration(snapshot.getMedian)), TIMER)
    send(fullName(name, "p75"), format(convertDuration(snapshot.get75thPercentile)), TIMER)
    send(fullName(name, "p95"), format(convertDuration(snapshot.get95thPercentile)), TIMER)
    send(fullName(name, "p98"), format(convertDuration(snapshot.get98thPercentile)), TIMER)
    send(fullName(name, "p99"), format(convertDuration(snapshot.get99thPercentile)), TIMER)
    send(fullName(name, "p999"), format(convertDuration(snapshot.get999thPercentile)), TIMER)

    reportMetered(name, timer)
  }

  /**
   * 将指标按StatsD协议格式组装后通过UDP发送
   */
  private def send(name: String, value: String, metricType: String)
      (implicit socket: DatagramSocket): Unit = {
    // 清洗指标名称后编码为字节数组
    val bytes = sanitize(s"$name:$value|$metricType").getBytes(UTF_8)
    // 构建UDP数据包并发送
    val packet = new DatagramPacket(bytes, bytes.length, address)
    socket.send(packet)
  }

  /**
   * 拼接完整的指标名称，包含前缀和各层级名称
   */
  private def fullName(names: String*): String = MetricRegistry.name(prefix, names : _*)

  /**
   * 清洗指标名称，将空白字符替换为连字符，避免协议解析错误
   */
  private def sanitize(s: String): String = whitespace.replaceAllIn(s, "-")

  /**
   * 格式化指标值为字符串
   */
  private def format(v: Any): String = formatAny(v).getOrElse("")

  /**
   * 根据数值类型格式化指标值，浮点数保留两位小数
   */
  private def formatAny(v: Any): Option[String] =
    v match {
      case f: Float => Some("%2.2f".format(f))
      case d: Double => Some("%2.2f".format(d))
      case b: BigDecimal => Some("%2.2f".format(b))
      case n: Number => Some(v.toString)
      case _ => None
    }
}
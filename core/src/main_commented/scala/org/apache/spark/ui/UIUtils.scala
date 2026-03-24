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

package org.apache.spark.ui

import java.{util => ju}
import java.lang.{Long => JLong}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale, TimeZone}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.xml._
import scala.xml.transform.{RewriteRule, RuleTransformer}

import jakarta.servlet.http.HttpServletRequest
import jakarta.ws.rs.core.{MediaType, MultivaluedMap, Response}
import org.eclipse.jetty.server.Request
import org.glassfish.jersey.internal.util.collection.MultivaluedStringMap

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.ui.scope.RDDOperationGraph

/**
 * Spark Web UI工具类，提供生成Spark UI页面所需的各种工具函数，包括格式化、HTML生成、路径处理等公共能力
 */
private[spark] object UIUtils extends Logging {
  val TABLE_CLASS_NOT_STRIPED = "table table-bordered table-hover table-sm"
  val TABLE_CLASS_STRIPED = TABLE_CLASS_NOT_STRIPED + " table-striped"
  val TABLE_CLASS_STRIPED_SORTABLE = TABLE_CLASS_STRIPED + " sortable"

  // 日期时间格式化器，使用系统默认时区
  private val dateTimeFormatter = DateTimeFormatter
    .ofPattern("yyyy/MM/dd HH:mm:ss", Locale.US)
    .withZone(ZoneId.systemDefault())

  /** 格式化Date对象为UI显示的日期时间字符串 */
  def formatDate(date: Date): String = dateTimeFormatter.format(date.toInstant)

  /** 格式化时间戳为UI显示的日期时间字符串 */
  def formatDate(timestamp: Long): String =
    dateTimeFormatter.format(Instant.ofEpochMilli(timestamp))

  /** 格式化毫秒时长为简洁的人类可读时长字符串 */
  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 100) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 1) {
      return "%.1f s".format(seconds)
    }
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    "%.1f h".format(hours)
  }

  /** Generate a verbose human-readable string representing a duration such as "5 second 35 ms" */
  /** 生成详细的多单位人类可读时长字符串，例如"5 second 35 ms" */
  def formatDurationVerbose(ms: Long): String = {
    try {
      val second = 1000L
      val minute = 60 * second
      val hour = 60 * minute
      val day = 24 * hour
      val week = 7 * day
      val year = 365 * day

      def toString(num: Long, unit: String): String = {
        if (num == 0) {
          ""
        } else if (num == 1) {
          s"$num $unit"
        } else {
          s"$num ${unit}s"
        }
      }
      // 根据时长大小决定显示多少级单位
      val millisecondsString = if (ms >= second && ms % second == 0) "" else s"${ms % second} ms"
      val secondString = toString((ms % minute) / second, "second")
      val minuteString = toString((ms % hour) / minute, "minute")
      val hourString = toString((ms % day) / hour, "hour")
      val dayString = toString((ms % week) / day, "day")
      val weekString = toString((ms % year) / week, "week")
      val yearString = toString(ms / year, "year")

      Seq(
        second -> millisecondsString,
        minute -> s"$secondString $millisecondsString",
        hour -> s"$minuteString $secondString",
        day -> s"$hourString $minuteString $secondString",
        week -> s"$dayString $hourString $minuteString",
        year -> s"$weekString $dayString $hourString"
      ).foreach { case (durationLimit, durationString) =>
        if (ms < durationLimit) {
          // 小于当前单位上限，返回对应层级的字符串
          return durationString
        }
      }
      // 超过一年，返回包含年份的字符串
      s"$yearString $weekString $dayString"
    } catch {
      case e: Exception =>
        logError("Error converting time to string", e)
        // 异常返回空字符串
        ""
    }
  }

  // 流处理批次时间格式化器（无毫秒）
  private val batchTimeFormat = DateTimeFormatter
    .ofPattern("yyyy/MM/dd HH:mm:ss", Locale.US)
    .withZone(ZoneId.systemDefault())

  // 流处理批次时间格式化器（带毫秒）
  private val batchTimeFormatWithMilliseconds = DateTimeFormatter
    .ofPattern("yyyy/MM/dd HH:mm:ss.SSS", Locale.US)
    .withZone(ZoneId.systemDefault())

  /**
   * 根据批次间隔格式化批次时间，间隔小于1秒时显示毫秒
   *
   * @param batchTime 待格式化的批次时间戳
   * @param batchInterval 批次间隔毫秒数
   * @param showYYYYMMSS 是否显示年月日部分，false仅返回时分秒
   * @param timezone 时区，仅用于测试
   * @return 格式化后的批次时间字符串
   */
  def formatBatchTime(
      batchTime: Long,
      batchInterval: Long,
      showYYYYMMSS: Boolean = true,
      timezone: TimeZone = null): String = {
    // 批次间隔大于等于1秒不显示毫秒
    val format = if (batchInterval < 1000) batchTimeFormatWithMilliseconds else batchTimeFormat
    val formatWithZone = if (timezone == null) format else format.withZone(timezone.toZoneId)
    val formattedBatchTime = formatWithZone.format(Instant.ofEpochMilli(batchTime))
    if (showYYYYMMSS) {
      formattedBatchTime
    } else {
      formattedBatchTime.substring(formattedBatchTime.indexOf(' ') + 1)
    }
  }

  /** Generate a human-readable string representing a number (e.g. 100 K) */
  /** 将数字格式化为带单位的简洁人类可读字符串，例如100 K */
  def formatNumber(records: Double): String = {
    val trillion = 1e12
    val billion = 1e9
    val million = 1e6
    val thousand = 1e3

    val (value, unit) = {
      if (records >= 2*trillion) {
        (records / trillion, " T")
      } else if (records >= 2*billion) {
        (records / billion, " B")
      } else if (records >= 2*million) {
        (records / million, " M")
      } else if (records >= 2*thousand) {
        (records / thousand, " K")
      } else {
        (records, "")
      }
    }
    if (unit.isEmpty) {
      "%d".formatLocal(Locale.US, value.toInt)
    } else {
      "%.1f%s".formatLocal(Locale.US, value, unit)
    }
  }

  // Yarn has to go through a proxy so the base uri is provided and has to be on all links
  /**
   * 获取UI根路径，处理代理场景下的基础路径
   * @param knoxBasePathGetter 获取请求头的函数
   * @return 代理基础路径
   */
  def uiRoot(knoxBasePathGetter: String => String): String = {
    // Knox使用X-Forwarded-Context传递基础路径
    val knoxBasePath = Option(knoxBasePathGetter("X-Forwarded-Context"))
    // 优先使用配置的proxyBase，其次使用环境变量，最后使用Knox传递的路径
    sys.props.get("spark.ui.proxyBase")
      .orElse(sys.env.get("APPLICATION_WEB_PROXY_BASE"))
      .orElse(knoxBasePath)
      .getOrElse("")
  }

  /** 从HttpServletRequest获取UI根路径 */
  def uiRoot(request: HttpServletRequest): String = {
    uiRoot(request.getHeader _)
  }

  /** 从Jetty Request获取UI根路径 */
  def uiRoot(request: Request): String = {
    uiRoot(request.getHeaders.get: String => String)
  }

  /** 拼接基础路径到资源路径，生成完整的UI资源路径 */
  def prependBaseUri(
      request: HttpServletRequest,
      basePath: String = "",
      resource: String = ""): String = {
    uiRoot(request) + basePath + resource
  }

  /** 生成所有Spark UI页面公共的HTML头节点，包含所有公共CSS和JS */
  def commonHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/bootstrap.min.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/vis-timeline-graph2d.min.css")} type="text/css"/>
    <link rel="stylesheet" href={prependBaseUri(request, "/static/webui.css")} type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/timeline-view.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/sorttable.js")} ></script>
    <script src={prependBaseUri(request, "/static/jquery.min.js")}></script>
    <script src={prependBaseUri(request, "/static/vis-timeline-graph2d.min.js")}></script>
    <script src={prependBaseUri(request, "/static/bootstrap.bundle.min.js")}></script>
    <script src={prependBaseUri(request, "/static/initialize-tooltips.js")}></script>
    <script src={prependBaseUri(request, "/static/table.js")}></script>
    <script src={prependBaseUri(request, "/static/timeline-view.js")}></script>
    <script src={prependBaseUri(request, "/static/log-view.js")}></script>
    <script src={prependBaseUri(request, "/static/webui.js")}></script>
    <script src={prependBaseUri(request, "/static/scroll-button.js")} type="module"></script>
    <script nonce={CspNonce.get}>setUIRoot('{UIUtils.uiRoot(request)}')</script>
  }

  /** 生成DAG可视化所需的头节点，引入DAG可视化依赖的CSS和JS */
  def vizHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/spark-dag-viz.css")} type="text/css" />
    <script src={prependBaseUri(request, "/static/d3.min.js")}></script>
    <script src={prependBaseUri(request, "/static/dagre-d3.min.js")}></script>
    <script src={prependBaseUri(request, "/static/graphlib-dot.min.js")}></script>
    <script src={prependBaseUri(request, "/static/spark-dag-viz.js")}></script>
  }

  /** 生成DataTables表格插件所需的头节点，引入表格插件依赖的CSS和JS */
  def dataTablesHeaderNodes(request: HttpServletRequest): Seq[Node] = {
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/dataTables.bootstrap5.min.css")}
          type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/jquery.dataTables.min.css")}
          type="text/css"/>
    <link rel="stylesheet"
          href={prependBaseUri(request, "/static/webui-dataTables.css")} type="text/css"/>
    <script src={prependBaseUri(request, "/static/jquery.dataTables.min.js")}></script>
    <script src={prependBaseUri(request, "/static/jquery.cookies.2.2.0.min.js")}></script>
    <script src={prependBaseUri(request, "/static/dataTables.bootstrap5.min.js")}></script>
  }

  /** Returns a spark page with correctly formatted headers */
  /**
   * 生成标准Spark页面结构，包含完整的导航栏、内容区和页脚
   *
   * @param request HTTP请求
   * @param title 页面标题
   * @param content 页面内容节点
   * @param activeTab 当前激活的标签页
   * @param helpText 帮助提示文本
   * @param showVisualization 是否加载DAG可视化依赖
   * @param useDataTables 是否加载DataTables依赖
   * @return 完整的HTML页面节点
   */
  def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      helpText: Option[String] = None,
      showVisualization: Boolean = false,
      useDataTables: Boolean = false): Seq[Node] = {

    val appName = activeTab.appName
    // 应用名称过长时截断显示
    val shortAppName = if (appName.length < 36) appName else appName.take(32) + "..."
    // 生成顶部导航标签栏
    val header = activeTab.headerTabs.map { tab =>
      <li class={if (tab == activeTab) "nav-item active" else "nav-item"}>
        <a class="nav-link"
           href={prependBaseUri(request, activeTab.basePath, "/" + tab.prefix + "/")}>{tab.name}</a>
      </li>
    }
    // 生成帮助按钮（带提示）
    val helpButton: Seq[Node] = helpText.map(tooltip(_, "top")).getOrElse(Seq.empty)

    <html data-bs-theme="light">
      <head>
        {commonHeaderNodes(request)}
        <script nonce={CspNonce.get}>{Unparsed(
          "document.documentElement.setAttribute('data-bs-theme'," +
          "localStorage.getItem('spark-theme')||" +
          "(matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light'))")}</script>
        <script nonce={CspNonce.get}>setAppBasePath('{activeTab.basePath}')</script>
        {if (showVisualization) vizHeaderNodes(request) else Seq.empty}
        {if (useDataTables) dataTablesHeaderNodes(request) else Seq.empty}
        <link rel="shortcut icon"
              href={prependBaseUri(request, "/static/spark-logo.svg")}></link>
        <title>{appName} - {title}</title>
      </head>
      <body class="d-flex flex-column min-vh-100">
        <!-- 加载中遮罩层 -->
        <div id="loading-overlay"
             class={"position-fixed top-0 start-0 w-100 h-100" +
               " d-flex justify-content-center align-items-center d-none"}>
          <div class="text-center">
            <div class="spinner-border text-primary" role="status">
              <span class="visually-hidden">Loading...</span>
            </div>
            <h3 class="mt-2">Loading...</h3>
          </div>
        </div>
        <!-- 顶部导航栏 -->
        <nav class="navbar navbar-expand-md navbar-light bg-light mb-4">
          <div class="navbar-header">
            <div class="navbar-brand">
              <a href={prependBaseUri(request, "/")}>
                <img class="spark-logo" src={prependBaseUri(request, "/static/spark-logo.svg")}
                     alt="Spark Logo" height="36" />
              </a>
            </div>
          </div>
          <
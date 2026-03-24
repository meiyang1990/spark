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

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{LOG_TYPE, PATH}
import org.apache.spark.internal.config.DRIVER_LOG_LOCAL_DIR
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.DriverLogger.DRIVER_LOG_FILE
import org.apache.spark.util.logging.RollingFileAppender

/**
 * 驱动程序日志查看页面，用于在Spark UI中展示Driver进程的运行日志
 * 实现逻辑参考了Worker节点的日志页面实现
 */
private[ui] class DriverLogPage(
    parent: DriverLogTab,
    conf: SparkConf)
  extends WebUIPage("") with Logging {
  require(conf.get(DRIVER_LOG_LOCAL_DIR).nonEmpty, s"Please specify ${DRIVER_LOG_LOCAL_DIR.key}.")

  // 支持查看的日志类型集合
  private val supportedLogTypes = Set(DRIVER_LOG_FILE, "stderr", "stdout")
  // 默认单次加载的日志字节数
  private val defaultBytes = 100 * 1024
  // Driver日志所在的本地目录
  private val logDir = conf.get(DRIVER_LOG_LOCAL_DIR).get

  /**
   * 渲染Driver日志页面HTML内容
   * @param request HTTP请求对象
   * @return 页面XML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 获取请求参数中的日志类型，默认为主驱动日志
    val logType = Option(request.getParameter("logType")).getOrElse(DRIVER_LOG_FILE)
    // 获取起始偏移量参数
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    // 获取本次加载字节数，默认使用预设值
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)
    // 获取指定范围的日志内容
    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    // 当前加载段长度
    val curLogLength = endByte - startByte
    // 日志范围信息展示
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    // 加载更多按钮
    val moreButton =
      <button type="button" class="log-more-btn btn btn-secondary">
        Load More
      </button>

    // 加载最新日志按钮
    val newButton =
      <button type="button" class="log-new-btn btn btn-secondary">
        Load New
      </button>

    // 日志已到底部提示框
    val alert =
      <div class="no-new-alert alert alert-info d-none">
        End of Log
      </div>

    // 日志请求参数
    val logParams = "/?logType=%s".format(logType)
    // 页面初始化JavaScript代码
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    // 组装页面内容
    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        Logs at {logDir}
        {range}
        <div class="log-content overflow-auto p-1" style="height:80vh;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script nonce={CspNonce.get}>{Unparsed(jsOnload)}</script>
      </div>

    // 使用通用页面框架包装返回
    UIUtils.headerSparkPage(request, "Logs", content, parent)
  }

  /**
   * 处理AJAX增量日志请求，返回纯文本日志内容
   * @param request HTTP请求对象
   * @return 纯文本日志内容
   */
  def renderLog(request: HttpServletRequest): String = {
    val logType = Option(request.getParameter("logType")).getOrElse(DRIVER_LOG_FILE)
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  /**
   * 根据偏移量和长度从滚动日志文件中读取指定范围的日志内容
   * @param logDirectory 日志目录路径
   * @param logType 日志类型
   * @param offsetOption 起始字节偏移量，None表示从末尾读取
   * @param byteLength 需要读取的字节数
   * @return (日志内容, 起始偏移, 结束偏移, 日志总长度)
   */
  private def getLog(
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int
    ): (String, Long, Long, Long) = {

    // 检查日志类型是否合法
    if (!supportedLogTypes.contains(logType)) {
      return ("Error: Log type must be one of " + supportedLogTypes.mkString(", "), 0, 0, 0)
    }

    try {
      // 获取按时间排序的所有滚动日志文件
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, logType)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      // 计算每个文件长度和总长度
      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, conf))
      val totalLength = fileLengths.sum
      // 若未指定偏移量，默认读取末尾部分
      val offset = offsetOption.getOrElse(totalLength - byteLength)
      // 边界校正起始偏移量
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      // 计算结束偏移量，不超过总长度
      val endIndex = math.min(startIndex + byteLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      // 跨文件读取指定范围的日志内容
      val logText = Utils.offsetBytes(files, fileLengths, startIndex, endIndex)
      logDebug(s"Got log of length ${logText.length} bytes")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(log"Error getting ${MDC(LOG_TYPE, logType)} logs from directory " +
          log"${MDC(PATH, logDirectory)}", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}

/**
 * Driver日志Tab页，集成到Spark UI导航栏中
 * 负责注册Driver日志页面到Spark UI框架
 */
private[ui] class DriverLogTab(parent: SparkUI) extends SparkUITab(parent, "logs") {
  private val page = new DriverLogPage(this, parent.conf)
  attachPage(page)

  def getPage: DriverLogPage = page
}
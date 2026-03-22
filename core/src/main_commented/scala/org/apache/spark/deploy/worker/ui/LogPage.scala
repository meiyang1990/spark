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

package org.apache.spark.deploy.worker.ui

import java.io.File

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{LOG_TYPE, PATH}
import org.apache.spark.ui.{CspNonce, UIUtils, WebUIPage}
import org.apache.spark.util.Utils
import org.apache.spark.util.logging.RollingFileAppender

/**
 * Worker WebUI 日志页面处理器，负责处理Executor/Driver/Worker自身日志的展示请求
 * 提供分页加载日志能力，支持滚动日志文件的拼接读取
 */
private[ui] class LogPage(parent: WorkerWebUI) extends WebUIPage("logPage") with Logging {
  private val worker = parent.worker
  private val workDir = new File(parent.workDir.toURI.normalize().getPath)
  private val supportedLogTypes = Set("stderr", "stdout", "out")
  private val defaultBytes = 100 * 1024

  /**
   * 处理纯文本日志请求，返回指定范围的日志内容
   * @param request HTTP请求，包含日志定位参数
   * @return 指定范围的日志文本，带字节位置头信息
   */
  def renderLog(request: HttpServletRequest): String = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val self = Option(request.getParameter("self"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

    // 根据请求参数确定日志文件所在目录
    val logDir = (appId, executorId, driverId, self) match {
      case (Some(a), Some(e), None, None) =>
        s"${workDir.getPath}/$a/$e/"
      case (None, None, Some(d), None) =>
        s"${workDir.getPath}/$d/"
      case (None, None, None, Some(_)) =>
        s"${sys.env.getOrElse("SPARK_LOG_DIR", workDir.getPath)}/"
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  /**
   * 渲染HTML格式的日志页面，包含分页加载UI控件和日志展示
   * @param request HTTP请求，包含日志定位参数
   * @return 完整HTML页面的节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val self = Option(request.getParameter("self"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(defaultBytes)

    // 根据请求参数解析得到日志目录、URL参数、页面标题
    val (logDir, params, pageName) = (appId, executorId, driverId, self) match {
      case (Some(a), Some(e), None, None) =>
        (s"${workDir.getPath}/$a/$e/", s"appId=$a&executorId=$e", s"$a/$e")
      case (None, None, Some(d), None) =>
        (s"${workDir.getPath}/$d/", s"driverId=$d", d)
      case (None, None, None, Some(_)) =>
        (s"${sys.env.getOrElse("SPARK_LOG_DIR", workDir.getPath)}/", "self", "worker")
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    // 返回Master页面链接
    val linkToMaster = <p><a href={worker.activeMasterWebUiUrl}>Back to Master</a></p>
    val curLogLength = endByte - startByte
    // 当前展示字节范围信息
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    // 加载更多历史日志按钮
    val moreButton =
      <button type="button" class="log-more-btn btn btn-secondary">
        Load More
      </button>

    // 加载最新日志按钮
    val newButton =
      <button type="button" class="log-new-btn btn btn-secondary">
        Load New
      </button>

    // 日志已到末尾提示框
    val alert =
      <div class="no-new-alert alert alert-info d-none">
        End of Log
      </div>

    val logParams = "?%s&logType=%s".format(params, logType)
    // 页面初始化JS脚本，绑定日志加载逻辑
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    // 组装页面内容
    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        {linkToMaster}
        {range}
        <div class="log-content overflow-auto p-1" style="height:80vh;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script nonce={CspNonce.get}>{Unparsed(jsOnload)}</script>
      </div>

    UIUtils.basicSparkPage(request, content, logType + " log page for " + pageName)
  }

  /** Get the part of the log files given the offset and desired length of bytes */
  /**
   * 根据指定偏移和长度，从滚动日志文件中读取对应范围的日志内容
   * @param logDirectory 日志文件所在目录
   * @param logType 日志类型（stdout/stderr/out）
   * @param offsetOption 起始字节偏移，None则自动读取末尾
   * @param byteLength 需要读取的字节数
   * @return (日志内容, 起始字节, 结束字节, 日志总长度)
   */
  private def getLog(
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int
    ): (String, Long, Long, Long) = {

    // 校验日志类型合法性
    if (!supportedLogTypes.contains(logType)) {
      return ("Error: Log type must be one of " + supportedLogTypes.mkString(", "), 0, 0, 0)
    }

    // 校验日志目录路径合法性，防止路径遍历攻击
    // Verify that the normalized path of the log directory is in the working directory
    val normalizedUri = new File(logDirectory).toURI.normalize()
    val normalizedLogDir = new File(normalizedUri.getPath)
    if (!Utils.isInDirectory(workDir, normalizedLogDir)) {
      return ("Error: invalid log directory " + logDirectory, 0, 0, 0)
    }

    try {
      // 匹配日志文件名，out类型需要自动查找目录下的.out文件
      // Find a log file name
      val fileName = if (logType.equals("out")) {
        normalizedLogDir.listFiles.map(_.getName).filter(_.endsWith(".out"))
          .headOption.getOrElse(logType)
      } else {
        logType
      }
      // 获取排序后的滚动日志文件列表（按时间从旧到新）
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, fileName)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      // 计算所有日志文件总长度
      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, worker.conf))
      val totalLength = fileLengths.sum
      // 确定起始偏移，不指定则默认读取末尾byteLength长度
      val offset = offsetOption.getOrElse(totalLength - byteLength)
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      // 计算结束偏移，不超过总长度
      val endIndex = math.min(startIndex + byteLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      // 跨文件拼接读取指定范围的日志内容
      val logText = Utils.offsetBytes(files, fileLengths, startIndex, endIndex)
      logDebug(s"Got log of length ${logText.length} bytes")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(log"Error getting ${MDC(LOG_TYPE, logType)} logs from " +
          log"directory ${MDC(PATH, logDirectory)}", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}
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

package org.apache.spark.deploy.master.ui

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.deploy.Utils.{getLog, DEFAULT_BYTES}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{CspNonce, UIUtils, WebUIPage}

/**
 * Spark Master WebUI 日志查看页面
 * 负责生成并渲染日志内容页面，支持分页分段加载Master节点日志
 * @param parent 所属的MasterWebUI实例，用于获取Master配置和上下文
 */
private[ui] class LogPage(parent: MasterWebUI) extends WebUIPage("logPage") with Logging {
  /**
   * 渲染日志查看页面，处理HTTP请求生成页面内容
   * @param request HTTP请求对象，包含日志参数
   * @return 生成的页面XML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 从环境变量获取Spark日志目录，默认使用logs/
    val logDir = sys.env.getOrElse("SPARK_LOG_DIR", "logs/")
    // 获取请求参数指定的日志类型
    val logType = request.getParameter("logType")
    // 获取请求指定的起始偏移量
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    // 获取请求指定的读取字节长度，默认使用DEFAULT_BYTES
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(DEFAULT_BYTES)
    // 读取日志内容，获取日志范围信息
    val (logText, startByte, endByte, logLength) =
      getLog(parent.master.conf, logDir, logType, offset, byteLength)
    // 当前加载的日志字节数
    val curLogLength = endByte - startByte
    // 日志范围信息展示
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    // 加载更多日志按钮
    val moreButton =
      <button type="button" class="log-more-btn btn btn-secondary">
        Load More
      </button>

    // 加载最新日志按钮
    val newButton =
      <button type="button" class="log-new-btn btn btn-secondary">
        Load New
      </button>

    // 日志到底部提示框，默认隐藏
    val alert =
      <div class="no-new-alert alert alert-info d-none">
        End of Log
      </div>

    // 日志请求参数拼接
    val logParams = "?self&logType=%s".format(logType)
    // 页面初始化JS代码，调用前端日志分页加载逻辑
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    // 组装页面内容
    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        <p><a href="/">Back to Master</a></p>
        {range}
        <div class="log-content overflow-auto p-1" style="height:80vh;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script nonce={CspNonce.get}>{Unparsed(jsOnload)}</script>
      </div>

    // 调用基础页面模板生成完整Spark UI页面
    UIUtils.basicSparkPage(request, content, logType + " log page for master")
  }
}
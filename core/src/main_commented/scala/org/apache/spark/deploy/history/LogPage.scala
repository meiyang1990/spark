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

package org.apache.spark.deploy.history

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.deploy.Utils.{getLog, DEFAULT_BYTES}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{CspNonce, UIUtils, WebUIPage}

/**
 * 历史服务器日志查看页面处理器
 * 负责生成历史服务器中应用/驱动器日志查看的Web页面，支持分页分段加载日志内容
 */
private[history] class LogPage(conf: SparkConf) extends WebUIPage("logPage") with Logging {

  /**
   * 渲染日志查看页面，生成对应的HTML内容
   * @param request HTTP请求对象，包含请求参数
   * @return 渲染完成的页面XML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 获取Spark日志目录环境变量，默认使用logs/
    val logDir = sys.env.getOrElse("SPARK_LOG_DIR", "logs/")
    // 获取要查看的日志类型参数（driver/executor）
    val logType = request.getParameter("logType")
    // 获取起始偏移量参数
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    // 获取每次加载的字节长度，默认使用预定义值
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(DEFAULT_BYTES)
    // 读取日志内容，返回日志文本、起始字节、结束字节、日志总长度
    val (logText, startByte, endByte, logLength) =
      getLog(conf, logDir, logType, offset, byteLength)
    // 计算本次加载的日志长度
    val curLogLength = endByte - startByte
    // 构建范围显示区域
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    // 构建加载更多按钮
    val moreButton =
      <button type="button" class="log-more-btn btn btn-secondary">
        Load More
      </button>

    // 构建加载新日志按钮
    val newButton =
      <button type="button" class="log-new-btn btn btn-secondary">
        Load New
      </button>

    // 构建日志末尾提示框，默认隐藏
    val alert =
      <div class="no-new-alert alert alert-info d-none">
        End of Log
      </div>

    // 构建日志请求参数，用于前端异步加载
    val logParams = "?self&logType=%s".format(logType)
    // 页面初始化脚本，绑定日志分页加载逻辑
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    // 组装页面所有内容
    val content =
      <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
      <div>
        <p><a href="/">Back to Main page</a></p>
        {range}
        <div class="log-content overflow-auto p-1" style="height:80vh;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script nonce={CspNonce.get}>{Unparsed(jsOnload)}</script>
      </div>

    // 使用基础Spark页面模板包装生成完整页面并返回
    UIUtils.basicSparkPage(request, content, logType + " log page for history server")
  }
}
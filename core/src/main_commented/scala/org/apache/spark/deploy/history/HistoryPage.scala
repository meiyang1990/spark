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

import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.ui.{CspNonce, UIUtils, WebUIPage}
import org.apache.spark.ui.UIUtils.formatImportJavaScript

/**
 * 历史服务器首页渲染类
 * 负责生成Spark历史服务器首页的HTML内容，展示已完成和未完成的应用列表
 * @param parent 所属的HistoryServer实例，提供应用信息和配置
 */
private[history] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  /**
   * 渲染历史服务器首页
   * @param request HTTP请求对象
   * @return 生成的首页HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 获取是否显示未完成应用的参数
    val requestedIncomplete = Option(request.getParameter("showIncomplete"))
      .getOrElse("false").toBoolean

    val displayApplications = shouldDisplayApplications(requestedIncomplete)
    val eventLogsUnderProcessCount = parent.getEventLogsUnderProcess()
    val lastUpdatedTime = parent.getLastUpdatedTime()
    val providerConfig = parent.getProviderConfig()

    // 生成概览信息区域，展示配置和状态
    val summary =
      <div class="container-fluid">
        <ul class="list-unstyled">
          {providerConfig.map { case (k, v) =>
            // 多事件日志目录特殊处理，折叠展示
            if (k == "Event log directory" && v.contains(",")) {
              val dirs = v.split(",").map(_.trim)
              <li>
                <strong>{k}:</strong> {dirs.length} directories
                <a class="ms-1" data-bs-toggle="collapse" href="#logDirList" role="button"
                  aria-expanded="false" aria-controls="logDirList">
                  (show)
                </a>
                <ul class="collapse mt-1" id="logDirList">
                    {dirs.map(d => <li>{d}</li>)}
                </ul>
              </li>
            } else {
              <li><strong>{k}:</strong> {v}</li>
            }
          }}
        </ul>
        {
          // 有正在处理的事件日志时显示提示
          if (eventLogsUnderProcessCount > 0) {
          <p>There are {eventLogsUnderProcessCount} event log(s) currently being
            processed which may result in additional applications getting listed on this page.
            Refresh the page to view updates. </p>
          } else Seq.empty

        }
        {
          // 显示最后更新时间
          if (lastUpdatedTime > 0) {
            <p>Last updated: <span id="last-updated">{lastUpdatedTime}</span></p>
          } else Seq.empty
        }
        {
          // 显示客户端本地时区占位
          <p>Client local time zone: <span id="time-zone"></span></p>
        }
      </div>

    // 生成应用列表区域
    val appList =
      <div class="container-fluid">
        {
          // 初始化应用列表限制的JavaScript代码
          val js =
            s"""
               |${formatImportJavaScript(request, "/static/historypage.js", "setAppLimit")}
               |
               |setAppLimit(${parent.maxApplications});
               |""".stripMargin

          if (displayApplications) {
            // 需要展示应用，引入相关JS并预留应用列表占位
            <script src={UIUtils.prependBaseUri(
              request, "/static/dataTables.rowsGroup.js")}></script> ++
            <script type="module" src={UIUtils.prependBaseUri(
              request, "/static/historypage.js")} ></script> ++
            <script type="module" nonce={CspNonce.get}>{Unparsed(js)}</script> ++
              <div id="history-summary"></div>
          } else if (requestedIncomplete) {
            // 查询未完成应用但无结果
            <h4>No incomplete applications found!</h4>
          } else if (eventLogsUnderProcessCount > 0) {
            // 查询已完成应用但无结果，有正在处理的日志
            <h4>No completed applications found!</h4>
          } else {
            // 查询已完成应用但无结果，展示空列表提示
            <h4>No completed applications found!</h4> ++ parent.emptyListingHtml()
          }
        }
      </div>

    // 生成分页/切换链接区域
    val pageLink =
      <div class="container-fluid">
        <a href={makePageLink(request, !requestedIncomplete)}>
          {
            // 根据当前状态生成切换文本
            if (requestedIncomplete) {
              "Back to completed applications"
            } else {
              "Show incomplete applications"
            }
          }
        </a>
        <p><a href={UIUtils.prependBaseUri(request, "/logPage/?self&logType=out")}>
          Show server log</a></p>
      </div>
    // 拼接完整页面内容，引入公共JS
    val content =
      <script type="module" src={UIUtils.prependBaseUri(
        request, "/static/historypage-common.js")}></script> ++
      <script type="module" src={UIUtils.prependBaseUri(
        request, "/static/utils.js")}></script> ++
      summary ++ appList ++ pageLink
    // 生成基础Spark页面框架并返回
    UIUtils.basicSparkPage(request, content, parent.title, true)
  }

  /**
   * 判断是否需要展示应用列表
   * @param requestedIncomplete 是否请求展示未完成应用
   * @return 是否有符合条件的应用可以展示
   */
  def shouldDisplayApplications(requestedIncomplete: Boolean): Boolean = {
    parent.getApplicationInfoList(1)(isApplicationCompleted(_) != requestedIncomplete).nonEmpty
  }

  /**
   * 生成切换展示状态的链接
   * @param request HTTP请求对象
   * @param showIncomplete 是否展示未完成应用
   * @return 生成的链接地址
   */
  private def makePageLink(request: HttpServletRequest, showIncomplete: Boolean): String = {
    UIUtils.prependBaseUri(request, "/?" + "showIncomplete=" + showIncomplete)
  }

  /**
   * 判断应用是否已完成
   * @param appInfo 应用信息对象
   * @return 应用是否已完成
   */
  private def isApplicationCompleted(appInfo: ApplicationInfo): Boolean = {
    appInfo.attempts.nonEmpty && appInfo.attempts.head.completed
  }
}
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

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.ExecutorState
import org.apache.spark.deploy.StandaloneResourceUtils.{formatResourceRequirements, formatResourcesAddresses}
import org.apache.spark.deploy.master.ExecutorDesc
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/**
 * 应用详情页面，用于在Spark Standalone模式Master WebUI中展示指定应用的详细信息和Executor状态
 * @param parent 所属的MasterWebUI父对象
 */
private[ui] class ApplicationPage(parent: MasterWebUI) extends WebUIPage("app") {

  private val master = parent.masterEndpointRef

  /**
   * 渲染应用详情页面，展示指定应用的基本信息和所有Executor运行状态
   * @param request HTTP请求对象，包含要查询的应用ID参数
   * @return 渲染后的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = request.getParameter("appId")
    // 向Master请求当前集群状态信息
    val state = master.askSync[MasterStateResponse](RequestMasterState)
    // 分别从活跃应用和已完成应用中查找目标应用
    val app = state.activeApps.find(_.id == appId)
      .getOrElse(state.completedApps.find(_.id == appId).orNull)
    // 应用不存在时返回404错误页面
    if (app == null) {
      val msg = <div class="row">No running application with ID {appId}</div>
      return UIUtils.basicSparkPage(request, msg, "Not Found")
    }

    // 定义Executor表格表头
    val executorHeaders = Seq("ExecutorID", "Worker", "Cores", "Memory", "Resource Profile Id",
      "Resources", "State", "Logs")
    // 获取所有Executor，包括当前活跃和已移除的
    val allExecutors = (app.executors.values ++ app.removedExecutors).toSet.toSeq
    // 过滤得到保留展示的Executor：未完成或正常退出的Executor
    val executors = allExecutors.filter { exec =>
      !ExecutorState.isFinished(exec.state) || exec.state == ExecutorState.EXITED
    }
    // 过滤得到已经完全移除的Executor
    val removedExecutors = allExecutors.diff(executors)
    // 生成活跃Executor表格HTML
    val executorsTable = UIUtils.listingTable(executorHeaders, executorRow, executors)
    // 生成已移除Executor表格HTML
    val removedExecutorsTable = UIUtils.listingTable(executorHeaders, executorRow, removedExecutors)

    val content =
      <div class="row">
        <div class="col-12">
          <ul class="list-unstyled">
            <li><strong>ID:</strong> {app.id}</li>
            <li><strong>Name:</strong> {app.desc.name}</li>
            <li><strong>User:</strong> {app.desc.user}</li>
            <li><strong>Cores:</strong>
            {
              // 最大核数不限制时展示无限和已分配核数，否则展示最大、已分配和剩余核数
              if (app.desc.maxCores.isEmpty) {
                "Unlimited (%s granted)".format(app.coresGranted)
              } else {
                "%s (%s granted, %s left)".format(
                  app.desc.maxCores.get, app.coresGranted, app.coresLeft)
              }
            }
            </li>
            <li>
              {UIUtils.tooltipSpan(
                <xml:group><strong>Executor Limit: </strong>
                {
                  // 展示Executor数量限制和已分配数量
                  if (app.getExecutorLimit == Int.MaxValue) "Unlimited" else app.getExecutorLimit
                }
                ({app.executors.size} granted)</xml:group>, ToolTips.APPLICATION_EXECUTOR_LIMIT)}
            </li>
            <li>
              <strong>Executor Memory - Default Resource Profile:</strong>
              {Utils.megabytesToString(app.desc.memoryPerExecutorMB)}
            </li>
            <li>
              <strong>Executor Resources - Default Resource Profile:</strong>
              {formatResourceRequirements(app.desc.resourceReqsPerExecutor)}
            </li>
            <li><strong>Submit Date:</strong> {UIUtils.formatDate(app.submitDate)}</li>
            <li><strong>Duration:</strong> {UIUtils.formatDuration(app.duration)}</li>
            <li><strong>State:</strong> {app.state}</li>
            {
              // 应用未完成时展示应用UI链接，应用完成后如果配置了历史服务器则展示历史UI链接
              if (!app.isFinished) {
                if (app.desc.appUiUrl.isBlank()) {
                  <li><strong>Application UI:</strong> Disabled</li>
                } else {
                  <li><strong>
                      <a href={UIUtils.makeHref(parent.master.reverseProxy,
                        app.id, app.desc.appUiUrl)}>Application Detail UI</a>
                  </strong></li>
                }
              } else if (parent.master.historyServerUrl.nonEmpty) {
                <li><strong>
                    <a href={s"${parent.master.historyServerUrl.get}/history/${app.id}"}>
                      Application History UI</a>
                </strong></li>
              }
            }
          </ul>
        </div>
      </div>

      <div class="row"> <!-- Executors -->
        <div class="col-12">
          <span class="collapse-table" data-bs-toggle="collapse"
              data-bs-target="#aggregated-executors"
              aria-expanded="true" aria-controls="aggregated-executors"
              data-collapse-name="collapse-aggregated-executors">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Executor Summary ({allExecutors.length})</a>
            </h4>
          </span>
          <div class="collapsible-table collapse show" id="aggregated-executors">
            {executorsTable}
          </div>
          {
            // 存在已移除Executor时展示已移除Executor可折叠表格
            if (removedExecutors.nonEmpty) {
              <span class="collapse-table" data-bs-toggle="collapse"
                  data-bs-target="#aggregated-removedExecutors"
                  aria-expanded="true"
                  aria-controls="aggregated-removedExecutors"
                  data-collapse-name="collapse-aggregated-removedExecutors">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Removed Executors ({removedExecutors.length})</a>
                </h4>
              </span> ++
              <div class="collapsible-table collapse show"
                  id="aggregated-removedExecutors">
                {removedExecutorsTable}
              </div>
            }
          }
        </div>
      </div>;
    // 返回完整的页面结构
    UIUtils.basicSparkPage(request, content, "Application: " + app.desc.name)
  }

  /**
   * 生成单个Executor表格行的HTML内容
   * @param executor Executor描述对象
   * @return 表格行HTML节点序列
   */
  private def executorRow(executor: ExecutorDesc): Seq[Node] = {
    // 生成Worker页面跳转链接，处理反向代理前缀
    val workerUrlRef = UIUtils.makeHref(parent.master.reverseProxy,
      executor.worker.id, executor.worker.webUiAddress)
    <tr>
      <td>{executor.id}</td>
      <td>
        <a href={workerUrlRef}>{executor.worker.id}</a>
      </td>
      <td>{executor.cores}</td>
      <td>{executor.memory}</td>
      <td>{executor.rpId}</td>
      <td>{formatResourcesAddresses(executor.resources)}</td>
      <td>{executor.state}</td>
      <td>
        <a href={s"$workerUrlRef/logPage/?appId=${executor.application.id}&executorId=${executor.
          id}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage/?appId=${executor.application.id}&executorId=${executor.
          id}&logType=stderr"}>stderr</a>
      </td>
    </tr>
  }
}
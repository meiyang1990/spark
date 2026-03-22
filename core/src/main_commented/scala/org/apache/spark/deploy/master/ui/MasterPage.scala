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
import org.json4s.JValue

import org.apache.spark.deploy.DeployMessages.{KillDriverResponse, MasterStateResponse, RequestKillDriver, RequestMasterState}
import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.StandaloneResourceUtils._
import org.apache.spark.deploy.master._
import org.apache.spark.internal.config.UI.MASTER_UI_TITLE
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/**
 * Spark Standalone Master WebUI 主页面，用于展示集群整体状态、Worker、应用和驱动程序信息
 * @param parent 父级MasterWebUI实例
 */
private[ui] class MasterPage(parent: MasterWebUI) extends WebUIPage("") {
  private val master = parent.masterEndpointRef
  private val title = parent.master.conf.get(MASTER_UI_TITLE)
  // 匹配JSON请求路径的正则表达式，用于提取请求字段
  private val jsonFieldPattern = "/json/([a-zA-Z]+).*".r

  /**
   * 同步向Master请求获取当前集群完整状态信息
   * @return Master状态响应对象，包含所有Worker、应用、驱动信息
   */
  def getMasterState: MasterStateResponse = {
    master.askSync[MasterStateResponse](RequestMasterState)
  }

  /**
   * 处理JSON格式请求，返回对应集群状态数据
   * @param request HTTP请求对象
   * @return 序列化后的JSON格式集群状态数据
   */
  override def renderJson(request: HttpServletRequest): JValue = {
    jsonFieldPattern.findFirstMatchIn(request.getRequestURI()) match {
      case None => JsonProtocol.writeMasterState(getMasterState)
      case Some(m) if m.group(1) == "clusterutilization" =>
        JsonProtocol.writeClusterUtilization(getMasterState)
      case Some(m) => JsonProtocol.writeMasterState(getMasterState, Some(m.group(1)))
    }
  }

  /**
   * 处理终止应用的HTTP请求
   * @param request HTTP请求对象，包含应用ID参数
   */
  def handleAppKillRequest(request: HttpServletRequest): Unit = {
    handleKillRequest(request, id => {
      parent.master.idToApp.get(id).foreach { app =>
        parent.master.removeApplication(app, ApplicationState.KILLED)
      }
    })
  }

  /**
   * 处理终止驱动程序的HTTP请求
   * @param request HTTP请求对象，包含驱动ID参数
   */
  def handleDriverKillRequest(request: HttpServletRequest): Unit = {
    handleKillRequest(request, id => {
      master.ask[KillDriverResponse](RequestKillDriver(id))
    })
  }

  /**
   * 通用终止请求处理逻辑，校验权限和参数后执行终止操作
   * @param request HTTP请求对象
   * @param action 具体终止操作回调，参数为要终止的对象ID
   */
  private def handleKillRequest(request: HttpServletRequest, action: String => Unit): Unit = {
    // 校验是否允许终止操作以及用户权限
    if (parent.killEnabled &&
        parent.master.securityMgr.checkModifyPermissions(request.getRemoteUser)) {
      // 获取终止标记参数
      val killFlag = Option(request.getParameter("terminate")).getOrElse("false").toBoolean
      // 获取要终止的对象ID
      val id = Option(request.getParameter("id"))
      // 参数校验通过后执行终止操作
      if (id.isDefined && killFlag) {
        action(id.get)
      }

      // 等待操作完成后再返回页面
      Thread.sleep(100)
    }
  }

  /**
   * 格式化Worker节点已用和空闲资源详情为展示文本
   * @param worker Worker信息对象
   * @return 格式化后的资源详情字符串
   */
  private def formatWorkerResourcesDetails(worker: WorkerInfo): String = {
    val usedInfo = worker.resourcesInfoUsed
    val freeInfo = worker.resourcesInfoFree
    formatResourcesDetails(usedInfo, freeInfo)
  }

  /**
   * 统计整个集群所有存活Worker的总资源和已用资源，格式化为展示文本
   * @param aliveWorkers 存活Worker数组
   * @return 格式化后的集群资源使用情况字符串
   */
  private def formatMasterResourcesInUse(aliveWorkers: Array[WorkerInfo]): String = {
    // 统计各资源类型总数量
    val totalInfo = aliveWorkers.map(_.resourcesInfo)
      .flatMap(_.iterator)
      .groupBy(_._1) // 按资源名称分组
      .map { case (rName, rInfoArr) =>
      rName -> rInfoArr.map(_._2.addresses.length).sum
    }
    // 统计各资源类型已使用数量
    val usedInfo = aliveWorkers.map(_.resourcesInfoUsed)
      .flatMap(_.iterator)
      .groupBy(_._1) // 按资源名称分组
      .map { case (rName, rInfoArr) =>
      rName -> rInfoArr.map(_._2.addresses.length).sum
    }
    formatResourcesUsed(totalInfo, usedInfo)
  }

  /** Index view listing applications and executors */
  /**
   * 渲染Master主页面HTML内容
   * @param request HTTP请求对象
   * @return 渲染后的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val state = getMasterState

    // 判断是否需要展示自定义资源列
    val showResourceColumn = state.workers.filter(_.resourcesInfoUsed.nonEmpty).nonEmpty
    val workerHeaders = if (showResourceColumn) {
      Seq("Worker Id", "Address", "State", "Cores", "Memory", "Resources")
    } else {
      Seq("Worker Id", "Address", "State", "Cores", "Memory")
    }
    // 按ID排序Worker
    val workers = state.workers.sortBy(_.id)
    // 筛选存活Worker
    val aliveWorkers = state.workers.filter(_.state == WorkerState.ALIVE)
    // 生成Worker列表表格
    val workerTable = UIUtils.listingTable(workerHeaders, workerRow(showResourceColumn), workers)

    val appHeaders = Seq("Application ID", "Name", "Cores", "Memory per Executor",
      "Resources Per Executor", "Submitted Time", "User", "State", "Duration")
    // 按启动时间倒序排序活跃应用
    val activeApps = state.activeApps.sortBy(_.startTime).reverse
    // 生成活跃应用列表表格
    val activeAppsTable = UIUtils.listingTable(appHeaders, appRow, activeApps)
    // 按结束时间倒序排序已完成应用
    val completedApps = state.completedApps.sortBy(_.endTime).reverse
    // 生成已完成应用列表表格
    val completedAppsTable = UIUtils.listingTable(appHeaders, appRow, completedApps)

    val activeDriverHeaders = Seq("Submission ID", "Submitted Time", "Worker", "State", "Cores",
      "Memory", "Resources", "Main Class", "Duration")
    // 按启动时间倒序排序活跃驱动
    val activeDrivers = state.activeDrivers.sortBy(_.startTime).reverse
    // 生成活跃驱动列表表格
    val activeDriversTable =
      UIUtils.listingTable(activeDriverHeaders, activeDriverRow, activeDrivers)

    val completedDriverHeaders = Seq("Submission ID", "Submitted Time", "Worker", "State", "Cores",
      "Memory", "Resources", "Main Class")
    // 按启动时间倒序排序已完成驱动
    val completedDrivers = state.completedDrivers.sortBy(_.startTime).reverse
    // 生成已完成驱动列表表格
    val completedDriversTable =
      UIUtils.listingTable(completedDriverHeaders, completedDriverRow, completedDrivers)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.
    // 判断集群中是否存在驱动程序，决定是否显示驱动相关区域
    def hasDrivers: Boolean = activeDrivers.length > 0 || completedDrivers.length > 0

    val content =
        <div class="row">
          <div class="col-12">
            <ul class="list-unstyled">
              <li><strong>URL:</strong> {state.uri}</li>
              {
                // 显示REST API地址
                state.restUri.map { uri =>
                  <li>
                    <strong>REST URL:</strong> {uri}
                    <span class="rest-uri"> (cluster mode)</span>
                  </li>
                }.getOrElse { Seq.empty }
              }
              <li><strong>Workers:</strong> {aliveWorkers.length} Alive,
                {workers.count(_.state == WorkerState.DEAD)} Dead,
                {workers.count(_.state == WorkerState.DECOMMISSIONED)} Decommissioned,
                {workers.count(_.state == WorkerState.UNKNOWN)} Unknown
              </li>
              <li><strong>Cores in use:</strong> {aliveWorkers.map(_.cores).sum} Total,
                {aliveWorkers.map(_.coresUsed).sum} Used</li>
              <li><strong>Memory in use:</strong>
                {Utils.megabytesToString(aliveWorkers.map(_.memory).sum)} Total,
                {Utils.megabytesToString(aliveWorkers.map(_.memoryUsed).sum)} Used</li>
              <li><strong>Resources in use:</strong>
                {formatMasterResourcesInUse(aliveWorkers)}</li>
              <li><strong>Applications:</strong>
                {state.activeApps.length} <a href="#running-app">Running</a>,
                {state.completedApps.length} <a href="#completed-app">Completed</a> </li>
              <li><strong>Drivers:</strong>
                {state.activeDrivers.length} Running
                ({state.activeDrivers.count(_.state == DriverState.SUBMITTED)} Waiting),
                {state.completedDrivers.length} Completed
                ({state.completedDrivers.count(_.state == DriverState.KILLED)} Killed,
                {state.completedDrivers.count(_.state == DriverState.FAILED)} Failed,
                {state.completedDrivers.count(_.state == DriverState.ERROR)} Error,
                {state.completedDrivers.count(_.state == DriverState.RELAUNCHING)} Relaunching)
              </li>
              <li><strong>Status:</strong> {state.status}
                (<a href={"/environment/"}>Environment</a>,
                <a href={"/logPage/?self&logType=out"}>Log</a>)
              </li>
            </ul>
          </div>
        </div>

        <div class="row">
          <div class="col-12">
            <span class="collapse-table" data-bs-toggle="collapse"
                data-bs-target="#aggregated-workers"
                aria-expanded="true" aria-controls="aggregated-workers"
                data-collapse-name="collapse-aggregated-workers">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Workers ({workers.length})</a>
              </h4>
            </span>
            <div class="collapsible-table collapse show" id="aggregated-workers">
              {workerTable}
            </div>
          </div>
        </div>

        <div class="row">
          <div class="col-12">
            <span id="running-app" class="collapse-table"
                data-bs-toggle="collapse"
                data-bs-target="#aggregated-activeApps"
                aria-expanded="true" aria-controls="aggregated-activeApps"
                data-collapse-name="collapse-aggregated-activeApps">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Running Applications ({activeApps.length})</a>
              </h4>
            </span>
            <div class="collapsible-table collapse show" id="aggregated-activeApps">
              {activeAppsTable}
            </div>
          </div>
        </div>

        <div>
          {if (hasDrivers) {
             <div class="row">
               <div class="col-12">
                 <span class="collapse-table" data-bs-toggle="collapse"
                     data-bs-target="#aggregated-activeDrivers"
                     aria-expanded="true"
                     aria-controls="aggregated-activeDrivers"
                     data-collapse-name="collapse-aggregated-activeDrivers">
                   <h4>
                     <span class="collapse-table-arrow arrow-open"></span>
                     <a>Running Drivers ({activeDrivers.length})</a>
                   </h4>
                 </span>
                 <div class="collapsible-table collapse show"
                     id="aggregated-activeDrivers">
                   {activeDriversTable}
                 </div>
               </div>
             </div>
           }
          }
        </div>

        <div class="row">
          <div class="col-12">
            <span id="completed-app" class="collapse-table"
                data-bs-toggle="collapse"
                data-bs-target="#aggregated-completedApps"
                aria-expanded="true" aria-controls="aggregated-completedApps"
                data-collapse-name="collapse-aggregated-completedApps">
              <h4>
                <span class="collapse-table-arrow arrow-open"></span>
                <a>Completed Applications ({completedApps.length})</a>
              </h4>
            </span>
            <div class="collapsible-table collapse show"
                id="aggregated-completedApps">
              {completedAppsTable}
            </div>
          </div>
        </div>

        <div>
          {
            if (hasDrivers) {
              <div class="row">
                <div class="col-12">
                  <span class="collapse-table" data-bs-toggle="collapse"
                      data-bs-target="#aggregated-completedDrivers"
                      aria-expanded="true"
                      aria-controls="aggregated-completedDrivers"
                      data-collapse-name="collapse-aggregated-completedDrivers">
                    <h4>
                      <span class="collapse-table-arrow arrow-open"></span>
                      <a>Completed Drivers ({completedDrivers.length})</a>
                    </h4>
                  </span>
                  <div class="collapsible-table collapse show"
                      id="aggregated-completedDrivers">
                    {completedDriversTable}
                  </div>
                </div>
              </div>
            }
          }
        </div>;

    UIUtils.basicSparkPage(request, content, title.getOrElse("Spark Master at " + state.uri))
  }

  /**
   * 生成Worker列表表格中单行的HTML节点
   * @param showResourceColumn 是否显示资源列
   * @return Worker信息转HTML节点的函数
   */
  private def workerRow(showResourceColumn: Boolean): WorkerInfo => Seq[Node] = worker => {
    <tr>
      <td>
        {
          // 存活Worker生成指向其WebUI的链接
          if (worker.isAlive()) {
            <a href={UIUtils.makeHref(parent.master.reverseProxy, worker.id, worker.webUiAddress)}>
              {worker.id}
            </a>
          } else {
            worker.id
          }
        }
      </td>
      <td>{worker.host}:{worker.port}</td>
      <td>{worker.state}</td>
      <td>{worker.cores} ({worker.coresUsed} Used)</td>
      <td sorttable_customkey={"%s.%s".format(worker.memory, worker.memoryUsed)}>
        {Utils.megabytesToString(worker.memory)}
        ({Utils.megabytesToString(worker.memoryUsed)} Used)
      </td>
      {if (showResourceColumn) {
        <td>{formatWorkerResourcesDetails(worker)}</td>
      }}
    </tr>
  }

  /**
   * 生成应用列表表格中单行的HTML节点
   * @param app 应用信息对象
   * @return 应用行HTML节点序列
   */
  private def appRow(app: ApplicationInfo): Seq[Node] = {
    // 允许终止且应用正在运行/等待时，生成终止按钮
    val killLink = if (parent.killEnabled &&
      (app.state == ApplicationState.RUNNING || app.state == ApplicationState.WAITING)) {
      <form action="app/kill/" method="POST" class="d-inline">
        <input type="hidden" name="id" value={app.id}/>
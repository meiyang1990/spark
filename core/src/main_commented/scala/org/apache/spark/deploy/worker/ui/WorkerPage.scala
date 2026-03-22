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

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest
import org.json4s.JValue

import org.apache.spark.deploy.{ExecutorState, JsonProtocol}
import org.apache.spark.deploy.DeployMessages.{RequestWorkerState, WorkerStateResponse}
import org.apache.spark.deploy.StandaloneResourceUtils.{formatResourcesAddresses, formatResourcesDetails}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

/**
 * Worker节点WebUI页面，负责展示Worker节点的状态信息，包括已分配资源、运行和已完成的Executor与Driver
 * 属于Standalone模式Worker端WebUI的核心页面类
 * @param parent 所属的WorkerWebUI实例
 */
private[ui] class WorkerPage(parent: WorkerWebUI) extends WebUIPage("") {
  private val workerEndpoint = parent.worker.self

  /**
   * 将Worker状态信息序列化为JSON格式返回，供前端AJAX请求获取数据
   * @param request HTTP请求对象
   * @return 序列化后的Worker状态JSON
   */
  override def renderJson(request: HttpServletRequest): JValue = {
    val workerState = workerEndpoint.askSync[WorkerStateResponse](RequestWorkerState)
    JsonProtocol.writeWorkerState(workerState)
  }

  /**
   * 格式化Worker节点自定义资源的使用详情，用于页面展示
   * @param workerState Worker状态响应对象
   * @return 格式化后的资源详情字符串
   */
  private def formatWorkerResourcesDetails(workerState: WorkerStateResponse): String = {
    val totalInfo = workerState.resources
    val usedInfo = workerState.resourcesUsed
    // 计算每种资源的剩余可用地址列表
    val freeInfo = totalInfo.map { case (rName, rInfo) =>
      val freeAddresses = if (usedInfo.contains(rName)) {
        rInfo.addresses.diff(usedInfo(rName).addresses)
      } else {
        rInfo.addresses
      }
      rName -> new ResourceInformation(rName, freeAddresses)
    }
    formatResourcesDetails(usedInfo, freeInfo)
  }

  /**
   * 渲染Worker页面HTML内容
   * @param request HTTP请求对象
   * @return 生成的页面XML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 从Worker端点请求最新状态信息
    val workerState = workerEndpoint.askSync[WorkerStateResponse](RequestWorkerState)

    // 构建Executor表格表头
    val executorHeaders = Seq("ExecutorID", "State", "Cores", "Memory", "Resources",
      "Job Details", "Logs")
    val runningExecutors = workerState.executors
    // 生成运行中Executor的表格内容
    val runningExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, runningExecutors)
    val finishedExecutors = workerState.finishedExecutors
    // 生成已完成Executor的表格内容
    val finishedExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, finishedExecutors)

    // 构建Driver表格表头
    val driverHeaders = Seq("DriverID", "Main Class", "State", "Cores", "Memory", "Resources",
      "Logs", "Notes")
    // 按ID降序排序运行中Driver
    val runningDrivers = workerState.drivers.sortBy(_.driverId).reverse
    val runningDriverTable = UIUtils.listingTable[DriverRunner](driverHeaders,
      driverRow(workerState.workerId, _), runningDrivers)
    // 按ID降序排序已完成Driver
    val finishedDrivers = workerState.finishedDrivers.sortBy(_.driverId).reverse
    val finishedDriverTable = UIUtils.listingTable[DriverRunner](driverHeaders,
      driverRow(workerState.workerId, _), finishedDrivers)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.

    // 生成Worker页面的反向代理URL
    val workerUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, workerState.workerId,
      parent.webUrl)
    // 拼接页面所有内容节点
    val content =
      <div class="row"> <!-- Worker Details -->
        <div class="col-12">
          <ul class="list-unstyled">
            <li><strong>ID:</strong>
              <a href={s"$workerUrlRef/logPage/?self&logType=out"}>{workerState.workerId}</a>
            </li>
            <li><strong>
              Master URL:</strong> {workerState.masterUrl}
            </li>
            <li><strong>Cores:</strong> {workerState.cores} ({workerState.coresUsed} Used)</li>
            <li><strong>Memory:</strong> {Utils.megabytesToString(workerState.memory)}
              ({Utils.megabytesToString(workerState.memoryUsed)} Used)</li>
            <li><strong>Resources:</strong>
              {formatWorkerResourcesDetails(workerState)}</li>
          </ul>
          <p><a href={workerState.masterWebUiUrl}>Back to Master</a></p>
        </div>
      </div>
      <div class="row"> <!-- Executors and Drivers -->
        <div class="col-12">
          <span class="collapse-table" data-bs-toggle="collapse"
              data-bs-target="#aggregated-runningExecutors"
              aria-expanded="true" aria-controls="aggregated-runningExecutors"
              data-collapse-name="collapse-aggregated-runningExecutors">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Running Executors ({runningExecutors.size})</a>
            </h4>
          </span>
          <div class="collapsible-table collapse show" id="aggregated-runningExecutors">
            {runningExecutorTable}
          </div>
          {
            // 仅当存在运行中Driver时展示对应区域
            if (runningDrivers.nonEmpty) {
              <span class="collapse-table" data-bs-toggle="collapse"
                  data-bs-target="#aggregated-runningDrivers"
                  aria-expanded="true" aria-controls="aggregated-runningDrivers"
                  data-collapse-name="collapse-aggregated-runningDrivers">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Running Drivers ({runningDrivers.size})</a>
                </h4>
              </span> ++
              <div class="collapsible-table collapse show"
                  id="aggregated-runningDrivers">
                {runningDriverTable}
              </div>
            }
          }
          {
            // 仅当存在已完成Executor时展示对应区域
            if (finishedExecutors.nonEmpty) {
              <span class="collapse-table" data-bs-toggle="collapse"
                  data-bs-target="#aggregated-finishedExecutors"
                  aria-expanded="true"
                  aria-controls="aggregated-finishedExecutors"
                  data-collapse-name="collapse-aggregated-finishedExecutors">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Finished Executors ({finishedExecutors.size})</a>
                </h4>
              </span> ++
              <div class="collapsible-table collapse show"
                  id="aggregated-finishedExecutors">
                {finishedExecutorTable}
              </div>
            }
          }
          {
            // 仅当存在已完成Driver时展示对应区域
            if (finishedDrivers.nonEmpty) {
              <span class="collapse-table" data-bs-toggle="collapse"
                  data-bs-target="#aggregated-finishedDrivers"
                  aria-expanded="true" aria-controls="aggregated-finishedDrivers"
                  data-collapse-name="collapse-aggregated-finishedDrivers">
                <h4>
                  <span class="collapse-table-arrow arrow-open"></span>
                  <a>Finished Drivers ({finishedDrivers.size})</a>
                </h4>
              </span> ++
              <div class="collapsible-table collapse show"
                  id="aggregated-finishedDrivers">
                {finishedDriverTable}
              </div>
            }
          }
        </div>
      </div>;
    // 生成完整的Spark风格页面并返回
    UIUtils.basicSparkPage(request, content, "Spark Worker at %s:%s".format(
      workerState.host, workerState.port))
  }

  /**
   * 生成单个Executor的表格行HTML节点
   * @param executor Executor Runner实例，包含Executor所有状态信息
   * @return 表格行XML节点序列
   */
  def executorRow(executor: ExecutorRunner): Seq[Node] = {
    // 生成Worker端反向代理URL
    val workerUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, executor.workerId,
      parent.webUrl)
    // 生成应用UI反向代理URL
    val appUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, executor.appId,
      executor.appDesc.appUiUrl)

    <tr>
      <td>{executor.execId}</td>
      <td>{executor.state}</td>
      <td>{executor.cores}</td>
      <td sorttable_customkey={executor.memory.toString}>
        {Utils.megabytesToString(executor.memory)}
      </td>
      <td>{formatResourcesAddresses(executor.resources)}</td>
      <td>
        <ul class="list-unstyled">
          <li><strong>ID:</strong> {executor.appId}</li>
          <li><strong>Name:</strong>
          {
            // 运行中且有应用UI地址时生成可点击链接
            if ({executor.state == ExecutorState.RUNNING} && executor.appDesc.appUiUrl.nonEmpty) {
              <a href={appUrlRef}> {executor.appDesc.name}</a>
            } else {
              {executor.appDesc.name}
            }
          }
          </li>
          <li><strong>User:</strong> {executor.appDesc.user}</li>
        </ul>
      </td>
      <td>
        <a href={s"$workerUrlRef/logPage/?appId=${executor
          .appId}&executorId=${executor.execId}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage/?appId=${executor
          .appId}&executorId=${executor.execId}&logType=stderr"}>stderr</a>
      </td>
    </tr>

  }

  /**
   * 生成单个Driver的表格行HTML节点
   * @param workerId 当前Worker的ID
   * @param driver Driver Runner实例，包含Driver所有状态信息
   * @return 表格行XML节点序列
   */
  def driverRow(workerId: String, driver: DriverRunner): Seq[Node] = {
    // 生成Worker端反向代理URL
    val workerUrlRef = UIUtils.makeHref(parent.worker.reverseProxy, workerId, parent.webUrl)
    <tr>
      <td>{driver.driverId}</td>
      <td>{driver.driverDesc.command.arguments(2)}</td>
      <td>{driver.finalState.getOrElse(DriverState.RUNNING)}</td>
      <td sorttable_customkey={driver.driverDesc.cores.toString}>
        {driver.driverDesc.cores.toString}
      </td>
      <td sorttable_customkey={driver.driverDesc.mem.toString}>
        {Utils.megabytesToString(driver.driverDesc.mem)}
      </td>
      <td>{formatResourcesAddresses(driver.resources)}</td>
      <td>
        <a href={s"$workerUrlRef/logPage/?driverId=${driver.driverId}&logType=stdout"}>stdout</a>
        <a href={s"$workerUrlRef/logPage/?driverId=${driver.driverId}&logType=stderr"}>stderr</a>
      </td>
      <td>
        {driver.finalException.getOrElse("")}
      </td>
    </tr>
  }
}
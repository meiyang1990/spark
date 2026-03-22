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

import java.net.{InetAddress, NetworkInterface, SocketException}

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.apache.spark.deploy.DeployMessages.{DecommissionWorkersOnHosts, MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.Utils.addRenderLogHandler
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{HOSTS, NUM_REMOVED_WORKERS}
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.UI.MASTER_UI_DECOMMISSION_ALLOW_MODE
import org.apache.spark.internal.config.UI.UI_KILL_ENABLED
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.ArrayImplicits._

/**
 * Standalone模式下Master节点的Web UI服务，提供集群状态查看、应用管理、Worker退服等Web交互能力
 */
private[master]
class MasterWebUI(
    val master: Master,
    requestedPort: Int)
  extends WebUI(master.securityMgr, master.securityMgr.getSSLOptions("standalone"),
    requestedPort, master.conf, name = "MasterUI") with Logging {

  // Master端点引用，用于向Master发送同步请求
  val masterEndpointRef = master.self
  // 是否允许通过UI杀死应用和Driver
  val killEnabled = master.conf.get(UI_KILL_ENABLED)
  // 是否启用Worker退役机制
  val decommissionEnabled = master.conf.get(DECOMMISSION_ENABLED)
  // UI上允许发起退役请求的权限模式
  val decommissionAllowMode = master.conf.get(MASTER_UI_DECOMMISSION_ALLOW_MODE)

  // 初始化Web UI
  initialize()

  /** 初始化Web UI所有页面和处理器 */
  def initialize(): Unit = {
    val masterPage = new MasterPage(this)
    attachPage(new ApplicationPage(this))
    attachPage(new LogPage(this))
    val envPage = new EnvironmentPage(this, master.conf)
    attachPage(envPage)
    // 绑定环境信息页面Servlet处理器
    this.attachHandler(createServletHandler("/environment",
      (request: HttpServletRequest) => envPage.render(request),
      master.conf))
    attachPage(masterPage)
    // 添加静态资源处理器
    addStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)
    // 添加日志渲染处理器
    addRenderLogHandler(this, master.conf)
    // 如果允许杀死操作，绑定应用和Driver杀死请求处理器
    if (killEnabled) {
      attachHandler(createRedirectHandler(
        "/app/kill", "/", masterPage.handleAppKillRequest, httpMethods = Set("POST")))
      attachHandler(createRedirectHandler(
        "/driver/kill", "/", masterPage.handleDriverKillRequest, httpMethods = Set("POST")))
    }
    // 如果启用退役机制，绑定Worker按主机退役请求处理器
    if (decommissionEnabled) {
      attachHandler(createServletHandler("/workers/kill", new HttpServlet {
        override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
          // 获取请求中要退役Worker对应的主机名列表
          val hostnames: Seq[String] = Option(req.getParameterValues("host"))
            .getOrElse(Array[String]()).toImmutableArraySeq
          // 检查请求是否符合权限要求
          if (!isDecommissioningRequestAllowed(req)) {
            resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
          } else {
            // 向Master发送同步请求，退役对应主机上的所有Worker
            val removedWorkers = masterEndpointRef.askSync[Integer](
              DecommissionWorkersOnHosts(hostnames))
            logInfo(log"Decommissioning of hosts ${MDC(HOSTS, hostnames)}" +
              log" decommissioned ${MDC(NUM_REMOVED_WORKERS, removedWorkers)} workers")
            // 根据退役结果返回对应HTTP状态码
            if (removedWorkers > 0) {
              resp.setStatus(HttpServletResponse.SC_OK)
            } else if (removedWorkers == 0) {
              resp.sendError(HttpServletResponse.SC_NOT_FOUND)
            } else {
              // 不应该出现的异常分支
              resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
            }
          }
        }
      }, ""))
    }
  }

  /** 添加代理处理器，用于代理访问Worker和应用的UI */
  def addProxy(): Unit = {
    val handler = createProxyHandler(idToUiAddress)
    attachHandler(handler)
  }

  /** 根据ID查找对应Worker或应用的UI地址，用于代理转发 */
  def idToUiAddress(id: String): Option[String] = {
    val state = masterEndpointRef.askSync[MasterStateResponse](RequestMasterState)
    // 先尝试查找Worker，再尝试查找活跃应用
    val maybeWorkerUiAddress = state.workers.find(_.id == id).map(_.webUiAddress)
    val maybeAppUiAddress = state.activeApps.find(_.id == id).map(_.desc.appUiUrl)

    maybeWorkerUiAddress.orElse(maybeAppUiAddress)
  }

  /** 检查给定IP地址是否属于本机地址 */
  private def isLocal(address: InetAddress): Boolean = {
    if (address.isAnyLocalAddress || address.isLoopbackAddress) {
      return true
    }
    try {
      // 检查该地址是否绑定到本机的网络接口
      NetworkInterface.getByInetAddress(address) != null
    } catch {
      case _: SocketException => false
    }
  }

  /** 检查是否允许当前请求发起Worker退役操作，根据配置的权限模式判断 */
  private def isDecommissioningRequestAllowed(req: HttpServletRequest): Boolean = {
    decommissionAllowMode match {
      case "ALLOW" => true
      case "LOCAL" => isLocal(InetAddress.getByName(req.getRemoteAddr))
      case _ => false
    }
  }

}

/**
 * MasterWebUI的伴生对象，定义静态资源路径常量
 */
private[master] object MasterWebUI {
  private val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
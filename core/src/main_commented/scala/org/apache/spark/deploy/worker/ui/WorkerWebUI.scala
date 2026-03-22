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

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._

/**
 * Spark Standalone 集群工作节点（Worker）的Web监控UI服务器，提供Worker运行状态查看和日志查看功能
 */
private[worker]
class WorkerWebUI(
    val worker: Worker,
    val workDir: File,
    requestedPort: Int)
  extends WebUI(worker.securityMgr, worker.securityMgr.getSSLOptions("standalone"),
    requestedPort, worker.conf, name = "WorkerUI")
  with Logging {

  initialize()

  /**
   * 初始化Worker UI的所有页面和处理器
   */
  def initialize(): Unit = {
    // 创建日志页面实例
    val logPage = new LogPage(this)
    // 绑定日志页面到UI服务
    attachPage(logPage)
    // 绑定Worker状态总览页面到UI服务
    attachPage(new WorkerPage(this))
    // 添加静态资源处理器，加载CSS/JS等静态资源
    addStaticHandler(WorkerWebUI.STATIC_RESOURCE_BASE)
    // 绑定日志查看Servlet处理器，处理日志内容请求
    attachHandler(createServletHandler("/log",
      (request: HttpServletRequest) => logPage.renderLog(request),
      worker.conf))
  }
}

/**
 * WorkerWebUI工具对象，定义UI相关的全局常量
 */
private[worker] object WorkerWebUI {
  // 静态资源目录，复用Spark UI的全局静态资源路径
  val STATIC_RESOURCE_BASE = SparkUI.STATIC_RESOURCE_DIR
}
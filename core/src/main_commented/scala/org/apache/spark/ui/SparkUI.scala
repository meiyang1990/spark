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

import java.util.Date

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.ee10.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CLASS_NAME, WEB_URL}
import org.apache.spark.internal.config.DRIVER_LOG_LOCAL_DIR
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.env.EnvironmentTab
import org.apache.spark.ui.exec.ExecutorsTab
import org.apache.spark.ui.jobs.{JobsTab, StagesTab}
import org.apache.spark.ui.storage.StorageTab

/**
 * Spark应用的顶级Web用户界面，提供作业、阶段、存储、环境等信息的可视化展示
 * 是Spark应用监控和调试的核心入口
 * 
 * @param store 应用状态存储，提供所有监控数据的查询入口
 * @param sc SparkContext上下文实例，运行模式下存在，历史服务器模式下为None
 * @param conf Spark配置对象
 * @param securityManager 安全管理器，处理UI访问权限和SSL配置
 * @param appName 应用名称
 * @param basePath UI访问的基础路径
 * @param startTime 应用启动时间戳
 * @param appSparkVersion 应用使用的Spark版本
 */
private[spark] class SparkUI private (
    val store: AppStatusStore,
    val sc: Option[SparkContext],
    val conf: SparkConf,
    securityManager: SecurityManager,
    var appName: String,
    val basePath: String,
    val startTime: Long,
    val appSparkVersion: String)
  extends WebUI(securityManager, securityManager.getSSLOptions("ui"), SparkUI.getUIPort(conf),
    conf, basePath, "SparkUI")
  with Logging
  with UIRoot {

  // 是否允许通过UI终止作业/阶段，仅运行模式下可配置
  val killEnabled = sc.map(_.conf.get(UI_KILL_ENABLED)).getOrElse(false)

  // 当前应用ID
  var appId: String = _

  // Spark流处理进度监听器，用于展示流作业进度
  private var streamingJobProgressListener: Option[SparkListener] = None

  // UI初始化占位处理器，用于Spark启动过程中展示等待页面
  private val initHandler: ServletContextHandler = {
    val servlet = new HttpServlet() {
      override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        // 设置响应类型和编码
        res.setContentType("text/html;charset=utf-8")
        // 返回启动中提示页面
        res.getWriter.write("Spark is starting up. Please wait a while until it's ready.")
      }
    }
    createServletHandler("/", servlet, basePath)
  }

  // 标记是否已准备好挂载处理器，启动前添加的处理器会延迟挂载
  private var readyToAttachHandlers = false

  /**
   * 将所有已添加但未挂载的处理器挂载到Jetty服务器
   * 在Spark应用完全启动后调用，完成UI初始化
   */
  def attachAllHandlers(): Unit = {
    // 遍历已初始化的服务器实例，移除占位页面，挂载所有已添加的处理器
    serverInfo.foreach { server =>
      server.removeHandler(initHandler)
      handlers.foreach(server.addHandler(_, securityManager))
    }
    // 标记后续添加的处理器可以直接挂载
    readyToAttachHandlers = true
  }

  /** 
   * 将Servlet处理器添加到UI，遵循启动阶段约定：未就绪时暂存，就绪后自动挂载
   *  Note: The handler will not be attached until readyToAttachHandlers is true,
   *  handlers added before that will be attached by attachAllHandlers */
  override def attachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers += handler
    // 如果已经就绪，直接挂载到服务器
    if (readyToAttachHandlers) {
      serverInfo.foreach(_.addHandler(handler, securityManager))
    }
  }

  /** 
   * 初始化UI所有组件和标签页，构建完整界面 
   */
  def initialize(): Unit = {
    // 创建并挂载作业标签页
    val jobsTab = new JobsTab(this, store)
    attachTab(jobsTab)
    // 创建并挂载阶段标签页
    val stagesTab = new StagesTab(this, store)
    attachTab(stagesTab)
    // 挂载存储标签页，展示RDD缓存信息
    attachTab(new StorageTab(this, store))
    // 挂载环境标签页，展示系统属性和依赖信息
    attachTab(new EnvironmentTab(this, store))
    // 如果配置了驱动本地日志目录，添加驱动日志标签页
    if (sc.map(_.conf.get(DRIVER_LOG_LOCAL_DIR).nonEmpty).getOrElse(false)) {
      val driverLogTab = new DriverLogTab(this)
      attachTab(driverLogTab)
      // 挂载日志查看Servlet处理器
      attachHandler(createServletHandler("/log",
        (request: HttpServletRequest) => driverLogTab.getPage.renderLog(request),
        sc.get.conf))
    }
    // 挂载执行器标签页，展示所有执行器资源和指标信息
    attachTab(new ExecutorsTab(this))
    // 添加静态资源处理器，处理CSS、JS等静态文件
    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)
    // 根路径重定向到作业列表页面
    attachHandler(createRedirectHandler("/", "/jobs/", basePath = basePath))
    // 挂载REST API根处理器
    attachHandler(ApiRootResource.getServletHandler(this))
    // 如果开启Prometheus指标，挂载Prometheus指标端点处理器
    if (sc.map(_.conf.get(UI_PROMETHEUS_ENABLED)).getOrElse(false)) {
      attachHandler(PrometheusResource.getServletHandler(this))
    }

    // These should be POST only, but, the YARN AM proxy won't proxy POSTs
    // 挂载作业终止请求处理器，兼容YARN代理支持GET/POST两种方法
    attachHandler(createRedirectHandler(
      "/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
    // 挂载阶段终止请求处理器，兼容YARN代理支持GET/POST两种方法
    attachHandler(createRedirectHandler(
      "/stages/stage/kill", "/stages/", stagesTab.handleKillRequest,
      httpMethods = Set("GET", "POST")))
  }

  initialize()

  /**
   * 获取当前Spark应用提交用户名称，优先从存储中获取，失败后回退到系统属性
   * @return 用户名称，获取失败返回<unknown>
   */
  def getSparkUser: String = {
    try {
      Option(store.applicationInfo().attempts.head.sparkUser)
        .orElse(store.environmentInfo().systemProperties.toMap.get("user.name"))
        .getOrElse("<unknown>")
    } catch {
      case _: NoSuchElementException => "<unknown>"
    }
  }

  def getAppName: String = appName

  def setAppId(id: String): Unit = {
    appId = id
  }

  /**
   * 绑定Jetty服务器到指定端口，优先完成端口绑定再延迟挂载业务处理器
   * 实现先绑定端口，等应用完全启动后再挂载所有页面的启动流程，避免端口抢占超时
   */
  override def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      // 初始化服务器并绑定端口
      val server = initServer()
      // 仅挂载初始化占位页面
      server.addHandler(initHandler, securityManager)
      serverInfo = Some(server)
    } catch {
      case e: Exception =>
        logError(log"Failed to bind ${MDC(CLASS_NAME, className)}", e)
        System.exit(1)
    }
  }

  /**
   * 停止Spark UI服务器，释放端口资源
   */
  override def stop(): Unit = {
    super.stop()
    logInfo(log"Stopped Spark web UI at ${MDC(WEB_URL, webUrl)}")
  }

  override def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    if (appId == this.appId) {
      fn(this)
    } else {
      throw new NoSuchElementException()
    }
  }

  override def checkUIViewPermissions(appId: String, attemptId: Option[String],
      user: String): Boolean = {
    securityManager.checkUIViewPermissions(user)
  }

  /**
   * 获取当前应用信息迭代器，适配历史服务器API接口
   * @return 包含当前应用信息的迭代器
   */
  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    Iterator(new ApplicationInfo(
      id = appId,
      name = appName,
      coresGranted = None,
      maxCores = None,
      coresPerExecutor = None,
      memoryPerExecutorMB = None,
      attempts = Seq(new ApplicationAttemptInfo(
        attemptId = None,
        startTime = new Date(startTime),
        endTime = new Date(-1),
        duration = System.currentTimeMillis() - startTime,
        lastUpdated = new Date(startTime),
        sparkUser = getSparkUser,
        completed = false,
        appSparkVersion = appSparkVersion
      ))
    ))
  }

  override def getApplicationInfoList(max: Int)(
      filter: ApplicationInfo => Boolean): Iterator[ApplicationInfo] = {
    getApplicationInfoList.filter(filter).take(max)
  }

  /**
   * 根据应用ID查询应用信息，仅支持查询当前应用
   * @param appId 待查询的应用ID
   * @return 当前应用信息匹配返回Some，否则返回None
   */
  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    getApplicationInfoList.find(_.id == appId)
  }

  def getStreamingJobProgressListener: Option[SparkListener] = streamingJobProgressListener

  def setStreamingJobProgressListener(sparkListener: SparkListener): Unit = {
    streamingJobProgressListener = Option(sparkListener)
  }

  def clearStreamingJobProgressListener(): Unit = {
    streamingJobProgressListener = None
  }
}

/**
 * Spark UI标签页基类，统一提供从父SparkUI获取公共属性的能力
 * @param parent 父SparkUI实例
 * @param prefix 标签页URL路径前缀
 */
private[spark] abstract class SparkUITab(parent: SparkUI, prefix: String)
  extends WebUITab(parent, prefix) {

  def appName: String = parent.appName

  def appSparkVersion: String = parent.appSparkVersion

  def sparkUser: String = parent.getSparkUser

  def appStartTime: Long = {
    try {
      parent.store.applicationInfo().attempts.head.startTime.getTime
    } catch {
      case _: Exception => System.currentTimeMillis()
    }
  }
}

/**
 * SparkUI单例对象，提供静态常量和创建SparkUI实例的工厂方法
 */
private[spark] object SparkUI {
  // 静态资源目录路径，存放CSS、JS等前端资源
  val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"
  // 默认调度池名称常量
  val DEFAULT_POOL_NAME = "default"

  /**
   * 从配置中获取UI绑定端口
   * @param conf Spark配置对象
   * @return UI端口号
   */
  def getUIPort(conf: SparkConf): Int = {
    conf.get(UI_PORT)
  }

  /**
   * 创建SparkUI实例工厂方法，基于AppStatusStore提供监控数据
   * @param sc SparkContext实例，运行模式为Some，历史服务器模式为None
   * @param store 应用状态存储，提供监控数据查询
   * @param conf Spark配置对象
   * @param securityManager 安全管理器
   * @param appName 应用名称
   * @param basePath UI基础访问路径
   * @param startTime 应用启动时间戳
   * @param appSparkVersion Spark版本，默认使用当前版本
   * @return 初始化完成的SparkUI实例
   */
  def create(
      sc: Option[SparkContext],
      store: AppStatusStore,
      conf: SparkConf,
      securityManager: SecurityManager,
      appName: String,
      basePath: String,
      startTime: Long,
      appSparkVersion: String = org.apache.spark.SPARK_VERSION): SparkUI = {

    new SparkUI(store, sc, conf, securityManager, appName, basePath, startTime, appSparkVersion)
  }

}
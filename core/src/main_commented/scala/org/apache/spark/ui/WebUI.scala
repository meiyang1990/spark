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

import java.util.EnumSet

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import jakarta.servlet.DispatcherType
import jakarta.servlet.http.{HttpServlet, HttpServletRequest}
import org.eclipse.jetty.ee10.servlet.{FilterHolder, FilterMapping, ServletContextHandler, ServletHolder}
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * Spark Web UI的顶级根组件，封装Jetty HTTP服务器，管理所有UI页面和标签的层级结构
 *
 * WebUI采用层级结构组织内容：WebUI包含多个标签页（WebUITab），每个标签页包含多个页面（WebUIPage）
 * 也可以不使用标签页，直接将页面挂载到WebUI根路径下
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,
    val sslOptions: SSLOptions,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "",
    poolSize: Int = 200)
  extends Logging {

  // 存储当前WebUI所有已挂载的标签页
  protected val tabs = ArrayBuffer[WebUITab]()
  // 存储当前WebUI所有已注册的Servlet上下文处理器
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  // 页面到对应Servlet处理器的映射，一个页面对应HTML渲染和JSON渲染两个处理器
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  // Jetty服务器信息，绑定完成后才有值
  protected var serverInfo: Option[ServerInfo] = None
  // 对外公开的主机名，优先从环境变量SPARK_PUBLIC_DNS获取，否则使用驱动主机地址
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
  // 当前WebUI实例的格式化类名，用于日志输出
  protected val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
  def getTabs: Seq[WebUITab] = tabs.toSeq
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq

  /** 获取所有包装后的Servlet上下文处理器，用于添加过滤器等操作 */
  def getDelegatingHandlers: Seq[DelegatingServletContextHandler] = {
    handlers.map(new DelegatingServletContextHandler(_)).toSeq
  }

  /** 将标签页及其关联的所有页面挂载到当前WebUI */
  def attachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** 将标签页及其关联的所有页面从当前WebUI卸载 */
  def detachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  /** 将页面及其关联的所有处理器从当前WebUI卸载 */
  def detachPage(page: WebUIPage): Unit = {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** 将页面挂载到当前WebUI，创建HTML和JSON渲染两个Servlet处理器并注册 */
  def attachPage(page: WebUIPage): Unit = {
    val pagePath = "/" + page.prefix
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
    handlers += renderJsonHandler
  }

  /** 将Servlet上下文处理器注册到当前WebUI，若服务器已启动则直接添加到服务器 */
  def attachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler, securityManager))
  }

  /** 创建指定路径的Servlet处理器并注册到当前WebUI */
  def attachHandler(contextPath: String, httpServlet: HttpServlet, pathSpec: String): Unit = {
    val ctx = new ServletContextHandler()
    ctx.setContextPath(contextPath)
    ctx.addServlet(new ServletHolder(httpServlet), pathSpec)
    attachHandler(ctx)
  }

  /** 将Servlet上下文处理器从当前WebUI和Jetty服务器移除 */
  def detachHandler(handler: ServletContextHandler): Unit = synchronized {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
   * 卸载指定路径下的内容处理器
   *
   * @param path 要卸载的UI路径
   */
  def detachHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /**
   * 添加静态资源处理器，用于提供CSS、JS等静态文件服务
   *
   * @param resourceBase 静态资源根目录路径
   * @param path UI挂载路径
   */
  def addStaticHandler(resourceBase: String, path: String = "/static"): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /** 初始化UI组件的钩子方法，由子类实现具体初始化逻辑 */
  def initialize(): Unit

  /** 初始化Jetty服务器，绑定指定主机和端口，返回服务器信息 */
  def initServer(): ServerInfo = {
    val hostName = Option(conf.getenv("SPARK_LOCAL_IP"))
        .getOrElse(if (Utils.preferIPv6) "[::]" else "0.0.0.0")
    val server = startJettyServer(hostName, port, sslOptions, conf, name, poolSize)
    server
  }

  /** 绑定Jetty服务器，启动WebUI服务 */
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val server = initServer()
      handlers.foreach(server.addHandler(_, securityManager))
      serverInfo = Some(server)
      val hostName = Option(conf.getenv("SPARK_LOCAL_IP"))
          .getOrElse(if (Utils.preferIPv6) "[::]" else "0.0.0.0")
      logInfo(log"Bound ${MDC(CLASS_NAME, className)} to ${MDC(HOST, hostName)}," +
        log" and started at ${MDC(WEB_URL, webUrl)}")
    } catch {
      case e: Exception =>
        logError(log"Failed to bind ${MDC(CLASS_NAME, className)}", e)
        System.exit(1)
    }
  }

  /** @return 是否启用SSL，仅在bind调用后有效 */
  def isSecure: Boolean = serverInfo.map(_.securePort.isDefined).getOrElse(false)

  /** @return WebUI访问协议，仅在bind调用后有效 */
  def scheme: String = if (isSecure) "https://" else "http://"

  /** @return WebUI完整访问URL，仅在bind调用后有效 */
  def webUrl: String = s"${scheme}$publicHostName:${boundPort}"

  /** @return 服务器实际绑定端口，仅在bind调用后有效 */
  def boundPort: Int = serverInfo.map(si => si.securePort.getOrElse(si.boundPort)).getOrElse(-1)

  /** 停止Jetty服务器，关闭WebUI服务，仅在bind调用后有效 */
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.foreach(_.stop())
  }
}


/**
 * WebUI中的标签页，用于分组管理一组相关页面
 * 前缀会拼接到父路径后形成完整访问路径，前缀不能包含斜杠
 */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  // 当前标签页包含的所有页面
  val pages = ArrayBuffer[WebUIPage]()
  // 标签页显示名称，使用前缀首字母大写
  val name = prefix.capitalize

  /** 将页面挂载到当前标签页，自动拼接路径前缀 */
  def attachPage(page: WebUIPage): Unit = {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** 获取父WebUI中所有按显示顺序排序的标签页，用于渲染顶部导航栏 */
  def headerTabs: Seq[WebUITab] = parent.getTabs.sortBy(_.displayOrder)

  def basePath: String = parent.getBasePath

  /** 标签页显示顺序，数值越小越靠前 */
  def displayOrder: Int = Integer.MIN_VALUE
}


/**
 * WebUI层级结构中的叶子节点，代表一个具体页面
 * 页面前缀拼接到父路径（WebUI或WebUITab）后形成完整访问路径，前缀不能包含斜杠
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  /** 渲染页面HTML内容，由子类实现 */
  def render(request: HttpServletRequest): Seq[Node]
  /** 渲染页面JSON响应，默认返回空值 */
  def renderJson(request: HttpServletRequest): JValue = JNothing
}

/**
 * ServletContextHandler的包装类，提供过滤器管理能力
 */
private[spark] class DelegatingServletContextHandler(handler: ServletContextHandler) {

  /** 在过滤器链开头添加新的过滤器映射 */
  def prependFilterMapping(
      filterName: String,
      spec: String,
      types: EnumSet[DispatcherType]): Unit = {
    val mapping = new FilterMapping()
    mapping.setFilterName(filterName)
    mapping.setPathSpec(spec)
    mapping.setDispatcherTypes(types)
    handler.getServletHandler.prependFilterMapping(mapping)
  }

  /** 添加新的过滤器，配置初始化参数 */
  def addFilter(
      filterName: String,
      className: String,
      filterParams: Map[String, String]): Unit = {
    val filterHolder = new FilterHolder()
    filterHolder.setName(filterName)
    filterHolder.setClassName(className)
    filterParams.foreach { case (k, v) => filterHolder.setInitParameter(k, v) }
    handler.getServletHandler.addFilter(filterHolder)
  }

  /** 获取当前上下文已注册的过滤器数量 */
  def filterCount(): Int = {
    handler.getServletHandler.getFilters.length
  }

  /** 获取当前上下文的路径前缀 */
  def getContextPath(): String = {
    handler.getContextPath
  }
}
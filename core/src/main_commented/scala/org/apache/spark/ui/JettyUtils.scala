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

import java.net.{URI, URL, URLDecoder}
import java.util.{EnumSet, List => JList}

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try
import scala.xml.Node

import jakarta.servlet.{DispatcherType, Filter, FilterChain, ServletRequest, ServletResponse}
import jakarta.servlet.http._
import org.eclipse.jetty.client.{Response => CResponse}
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.transport.HttpClientTransportOverHTTP
import org.eclipse.jetty.compression.server.CompressionHandler
import org.eclipse.jetty.ee10.proxy.ProxyServlet
import org.eclipse.jetty.ee10.servlet._
import org.eclipse.jetty.http.{HttpField, HttpFields, HttpHeader}
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler.{ContextHandler, ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.util.{Callback, URIUtil}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{pretty, render}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.UI._
import org.apache.spark.util.Utils

/**
 * Jetty HTTP 服务器工具类，为 Spark Web UI 提供服务器启动、处理器创建、代理等通用能力
 */
private[spark] object JettyUtils extends Logging {

  val SPARK_CONNECTOR_NAME = "Spark"
  val REDIRECT_CONNECTOR_NAME = "HttpsRedirect"

  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.
  /** 基于HTTP请求返回响应的函数基类型，支持隐式转换为Jetty处理器 */
  type Responder[T] = HttpServletRequest => T

  /**
   * Servlet参数封装类，用于将自定义响应函数转换为Jetty Servlet
   * @param responder 处理请求生成响应的函数
   * @param contentType 响应内容类型
   * @param extractFn 提取响应字符串的函数，默认调用toString
   */
  class ServletParams[T <: AnyRef](val responder: Responder[T],
    val contentType: String,
    val extractFn: T => String = (in: Any) => in.toString) {}

  // Conversions from various types of Responder's to appropriate servlet parameters
  /** JSON响应函数转换为Servlet参数 */
  implicit def jsonResponderToServlet(responder: Responder[JValue]): ServletParams[JValue] =
    new ServletParams(responder, "text/json", (in: JValue) => pretty(render(in)))

  /** HTML响应函数转换为Servlet参数 */
  implicit def htmlResponderToServlet(responder: Responder[Seq[Node]]): ServletParams[Seq[Node]] =
    new ServletParams(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

  /** 文本响应函数转换为Servlet参数 */
  implicit def textResponderToServlet(responder: Responder[String]): ServletParams[String] =
    new ServletParams(responder, "text/plain")

  /**
   * 根据参数创建Jetty HttpServlet实例
   * @param servletParams Servlet参数，包含响应函数和格式配置
   * @param conf Spark配置
   * @return 初始化完成的HttpServlet实例
   */
  private def createServlet[T <: AnyRef](
      servletParams: ServletParams[T],
      conf: SparkConf): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        try {
          // 设置响应内容类型和编码
          response.setContentType("%s;charset=utf-8".format(servletParams.contentType))
          response.setStatus(HttpServletResponse.SC_OK)
          // 调用响应函数生成结果
          val result = servletParams.responder(request)
          // 输出格式化后的响应
          response.getWriter.print(servletParams.extractFn(result))
        } catch {
          case e: IllegalArgumentException =>
            // 参数错误返回400
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage)
          case e: Exception =>
            // 记录异常日志并抛出
            logWarning(log"GET ${MDC(LogKeys.URI, request.getRequestURI)} failed: " +
              log"${MDC(ERROR, e)}", e)
            throw e
        }
      }
      // SPARK-5983 ensure TRACE is not supported
      /** 禁用TRACE方法，防止安全漏洞 */
      protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
  }

  /**
   * 创建指定路径前缀的Servlet上下文处理器
   * @param path 路径前缀
   * @param servletParams Servlet参数
   * @param conf Spark配置
   * @param basePath 基础路径前缀
   * @return 初始化完成的Servlet上下文处理器
   */
  def createServletHandler[T <: AnyRef](
      path: String,
      servletParams: ServletParams[T],
      conf: SparkConf,
      basePath: String = ""): ServletContextHandler = {
    createServletHandler(path, createServlet(servletParams, conf), basePath)
  }

  /**
   * 创建指定路径前缀的Servlet上下文处理器
   * @param path 路径前缀
   * @param servlet 已创建的Servlet实例
   * @param basePath 基础路径前缀
   * @return 初始化完成的Servlet上下文处理器
   */
  def createServletHandler(
      path: String,
      servlet: HttpServlet,
      basePath: String): ServletContextHandler = {
    // 拼接完整路径，移除末尾斜杠避免双斜杠
    val prefixedPath = if (basePath == "" && path == "/") {
      path
    } else {
      (basePath + path).stripSuffix("/")
    }
    // 创建上下文并注册Servlet
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(prefixedPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /**
   * 创建重定向处理器，将请求从源路径重定向到目标路径
   * @param srcPath 源路径前缀
   * @param destPath 目标路径
   * @param beforeRedirect 重定向前执行的回调函数
   * @param basePath 基础路径前缀
   * @param httpMethods 允许重定向的HTTP方法集合
   * @return 初始化完成的重定向上下文处理器
   */
  def createRedirectHandler(
      srcPath: String,
      destPath: String,
      beforeRedirect: HttpServletRequest => Unit = x => (),
      basePath: String = "",
      httpMethods: Set[String] = Set("GET")): ServletContextHandler = {
    val prefixedDestPath = basePath + destPath
    val servlet = new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        if (httpMethods.contains("GET")) {
          doRequest(request, response)
        } else {
          response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
      }
      override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        if (httpMethods.contains("POST")) {
          doRequest(request, response)
        } else {
          response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
        }
      }
      /** 执行重定向逻辑 */
      private def doRequest(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        // 执行重定向前回调
        beforeRedirect(request)
        // 拼接新URL，避免路径中间出现双斜杠
        val requestURL = new URI(request.getRequestURL.toString).toURL
        // scalastyle:off URLConstructor
        val newUrl = new URL(requestURL, prefixedDestPath).toString
        // scalastyle:on URLConstructor
        response.sendRedirect(newUrl)
      }
      // SPARK-5983 ensure TRACE is not supported
      /** 禁用TRACE方法，防止安全漏洞 */
      protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
    createServletHandler(srcPath, servlet, basePath)
  }

  /**
   * 创建静态资源处理器，从指定目录提供静态文件服务
   * @param resourceBase 静态资源基础路径
   * @param path 上下文路径前缀
   * @return 初始化完成的静态资源上下文处理器
   */
  def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler
    // 禁用Jetty默认gzip，由上层压缩处理器处理
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
    val staticHandler = new DefaultServlet
    val holder = new ServletHolder(staticHandler)
    // 从类路径查找静态资源根目录
    Option(Utils.getSparkClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("baseResource", res.toString)
      case None =>
        throw new Exception("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.setContextPath(path)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  /**
   * 创建代理处理器，将请求代理到Worker或Application Driver的UI
   * @param idToUiAddress 根据ID获取目标UI地址的函数
   * @return 初始化完成的代理上下文处理器
   */
  def createProxyHandler(idToUiAddress: String => Option[String]): ServletContextHandler = {
    val servlet = new ProxyServlet {
      override def rewriteTarget(request: HttpServletRequest): String = {
        val path = request.getPathInfo
        if (path == null) return null

        // 从路径提取目标ID
        val prefixTrailingSlashIndex = path.indexOf('/', 1)
        val prefix = if (prefixTrailingSlashIndex == -1) {
          path
        } else {
          path.substring(0, prefixTrailingSlashIndex)
        }
        val id = prefix.drop(1)

        // 查询目标地址，构造代理URI并返回
        idToUiAddress(id)
          .map(createProxyURI(prefix, _, path, request.getQueryString))
          .filter(uri => uri != null && validateDestination(uri.getHost, uri.getPort))
          .map(_.toString)
          .orNull
      }

      override def newHttpClient(): HttpClient = {
        // SPARK-21176: Use the Jetty logic to calculate the number of selector threads (#CPUs/2),
        // but limit it to 8 max.
        // 限制选择器线程数最大为8，避免浪费线程资源
        val numSelectors = math.max(1, math.min(8, Runtime.getRuntime().availableProcessors() / 2))
        new HttpClient(new HttpClientTransportOverHTTP(numSelectors))
      }

      /** 重写响应头中的Location，适配代理路径 */
      override def filterServerResponseHeader(
          clientRequest: HttpServletRequest,
          serverResponse: CResponse,
          headerName: String,
          headerValue: String): String = {
        if (headerName.equalsIgnoreCase("location")) {
          val newHeader = createProxyLocationHeader(headerValue, clientRequest,
            serverResponse.getRequest().getURI())
          if (newHeader != null) {
            return newHeader
          }
        }
        super.filterServerResponseHeader(
          clientRequest, serverResponse, headerName, headerValue)
      }
    }

    // 创建代理上下文，匹配/proxy/*所有路径
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath("/proxy")
    contextHandler.addServlet(holder, "/*")
    contextHandler
  }

  /**
   * 启动Jetty服务器，绑定指定主机端口，自动重试空闲端口
   * @param hostName 绑定主机名
   * @param port 起始端口
   * @param sslOptions SSL配置
   * @param conf Spark配置
   * @param serverName 服务器名称，用于线程命名
   * @param poolSize 线程池最大线程数
   * @return 服务器信息，包含Server对象、绑定端口等
   */
  def startJettyServer(
      hostName: String,
      port: Int,
      sslOptions: SSLOptions,
      conf: SparkConf,
      serverName: String = "",
      poolSize: Int = 200): ServerInfo = {

    // 获取Jetty服务器停止超时配置
    val stopTimeout = conf.get(UI_JETTY_STOP_TIMEOUT)
    logInfo(log"Start Jetty ${MDC(HOST, hostName)}:${MDC(PORT, port)}" +
      log" for ${MDC(SERVER_NAME, serverName)}")
    // 创建线程池，设置为守护线程
    val pool = new QueuedThreadPool(poolSize)
    if (serverName.nonEmpty) {
      pool.setName(serverName)
    }
    pool.setDaemon(true)

    // 创建Jetty Server实例
    val server = new Server(pool)

    // 配置错误处理器，控制是否显示堆栈
    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(conf.get(UI_SHOW_ERROR_STACKS))
    server.setErrorHandler(errorHandler)

    val collection = new ContextHandlerCollection
    // 如果配置了代理重定向URI，添加代理重定向处理器
    conf.get(PROXY_REDIRECT_URI) match {
      case Some(proxyUri) =>
        val proxyHandler = new ProxyRedirectHandler(proxyUri)
        proxyHandler.setHandler(collection)
        server.setHandler(proxyHandler)

      case _ =>
        server.setHandler(collection)
    }

    // 创建守护线程调度器用于Jetty连接器
    val serverExecutor = new ScheduledExecutorScheduler(s"$serverName-JettyScheduler", true)

    try {
      server.setStopTimeout(stopTimeout)
      // 先启动服务器，再添加连接器
      server.start()

      // As each acceptor and each selector will use one thread, the number of threads should at
      // least be the number of acceptors and selectors plus 1. (See SPARK-13776)
      // 计算最小所需线程数，每个接收器和选择器各占用一个线程
      var minThreads = 1

      /** 创建新连接器并绑定指定端口 */
      def newConnector(
          connectionFactories: Array[ConnectionFactory],
          port: Int): (ServerConnector, Int) = {
        val connector = new ServerConnector(
          server,
          null,
          serverExecutor,
          null,
          -1,
          -1,
          connectionFactories: _*)
        connector.setPort(port)
        connector.setHost(hostName)
        // Windows下不开启reuseAddress避免端口占用问题
        connector.setReuseAddress(!Utils.isWindows)
         // spark-45248: set the idle timeout to prevent slow DoS
         // 设置空闲超时防止慢DOS攻击
        connector.setIdleTimeout(8000)

        // 限制最大接收器数量为8，避免浪费线程
        connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))

        connector.start()
        // 选择器数量等于接收器数量，更新最小线程数
        minThreads += connector.getAcceptors * 2

        (connector, connector.getLocalPort())
      }
      // 创建HTTP配置，设置请求头大小
      val httpConfig = new HttpConfiguration()
      val requestHeaderSize = conf.get(UI_REQUEST_HEADER_SIZE).toInt
      logDebug(s"Using requestHeaderSize: $requestHeaderSize")
      httpConfig.setRequestHeaderSize(requestHeaderSize)

      // 隐藏服务器版本信息，提高安全性
      logDebug("Using setSendServerVersion: false")
      httpConfig.setSendServerVersion(false)
      logDebug("Using setSendXPoweredBy: false")
      httpConfig.setSendXPoweredBy(false)

      // 如果配置了SSL，先创建HTTPS连接器
      val securePort = sslOptions.createJettySslContextFactoryServer().map { factory =>

        // SPARK-45522: SniHostCheck defaulted to true since Jetty 10,
        // this will affect the standalone deployment.
        // 关闭SNI主机检查，兼容Standalone部署场景
        val src = new SecureRequestCustomizer()
        src.setSniHostCheck(false)
        httpConfig.add
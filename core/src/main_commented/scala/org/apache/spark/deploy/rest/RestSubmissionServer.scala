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

package org.apache.spark.deploy.rest

import java.util.EnumSet
import java.util.concurrent.{Executors, ExecutorService}

import scala.io.Source

import com.fasterxml.jackson.core.JsonProcessingException
import jakarta.servlet.DispatcherType
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.ee10.servlet.{FilterHolder, ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{MASTER_REST_SERVER_FILTERS, MASTER_REST_SERVER_MAX_THREADS, MASTER_REST_SERVER_VIRTUAL_THREADS}
import org.apache.spark.util.Utils

/**
 * 文件级注释：Spark Standalone 集群应用提交 REST 服务端核心实现，提供基于 HTTP 的应用提交、状态查询、杀死应用等REST API，
 * 是 spark-submit --deploy-mode cluster 模式的后端服务，供 RestSubmissionClient 远程调用。
 *
 * A server that responds to requests submitted by the [[RestSubmissionClient]].
 *
 * This server responds with different HTTP codes depending on the situation:
 *   200 OK - Request was processed successfully
 *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
 *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
 *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
 *
 * The server always includes a JSON representation of the relevant [[SubmitRestProtocolResponse]]
 * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
 * instead of the one expected by the client. If the construction of this error response itself
 * fails, the response will consist of an empty body with a response code that indicates internal
 * server error.
 *
 * 【学习型注释】
 * RestSubmissionServer 是 Spark REST API 提交服务的抽象基类。
 * 主要功能：
 * 1. 提供 HTTP REST API，允许通过 HTTP 请求提交、杀死、查询 Spark 应用
 * 2. 支持的端点包括：/create（提交）、/kill（杀死）、/killall（杀死全部）、/status（状态）、/clear（清理）、/readyz（就绪检查）
 * 3. 使用 Jetty 作为嵌入式 HTTP 服务器
 * 4. 所有响应都是 JSON 格式的 SubmitRestProtocolResponse
 * 这是 spark-submit --deploy-mode cluster 时使用的后端服务。
 */
private[spark] abstract class RestSubmissionServer(
    val host: String,
    val requestedPort: Int,
    val masterConf: SparkConf) extends Logging {

  protected val submitRequestServlet: SubmitRequestServlet
  protected val killRequestServlet: KillRequestServlet
  protected val killAllRequestServlet: KillAllRequestServlet
  protected val statusRequestServlet: StatusRequestServlet
  protected val clearRequestServlet: ClearRequestServlet
  protected val readyzRequestServlet: ReadyzRequestServlet

  // Visible for testing
  private[rest] var _server: Option[Server] = None

  // URL 路径前缀到 Servlet 的映射，定义了 REST API 的路由规则
  protected val baseContext = s"/${RestSubmissionServer.PROTOCOL_VERSION}/submissions"
  protected lazy val contextToServlet = Map[String, RestServlet](
    s"$baseContext/create/*" -> submitRequestServlet,     // 提交应用
    s"$baseContext/kill/*" -> killRequestServlet,         // 杀死指定应用
    s"$baseContext/killall/*" -> killAllRequestServlet,   // 杀死所有应用
    s"$baseContext/status/*" -> statusRequestServlet,     // 查询应用状态
    s"$baseContext/clear/*" -> clearRequestServlet,       // 清理已完成的应用
    s"$baseContext/readyz/*" -> readyzRequestServlet,     // 就绪检查（健康检查）
    "/*" -> new ErrorServlet // 默认错误处理器
  )

  /**
   * 启动 REST 服务器，自动处理端口冲突并返回最终绑定的端口号
   * @return 绑定成功的端口号
   */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, masterConf)
    _server = Some(server)
    logInfo(log"Started REST server for submitting applications on ${MDC(HOST, host)}" +
      log" with port ${MDC(PORT, boundPort)}")
    boundPort
  }

  /**
   * 实际执行 Jetty 服务器启动的内部方法，完成所有配置初始化和路由注册
   * @param startPort 尝试启动的端口
   * @return (启动完成的服务器实例, 实际绑定的端口)
   */
  private def doStart(startPort: Int): (Server, Int) = {
    // 创建 Jetty 线程池，使用配置的最大线程数
    val threadPool = new QueuedThreadPool(masterConf.get(MASTER_REST_SERVER_MAX_THREADS))
    threadPool.setName(getClass().getSimpleName())
    // Java 21+ 开启虚拟线程优化，提升高并发场景性能
    if (Utils.isJavaVersionAtLeast21 && masterConf.get(MASTER_REST_SERVER_VIRTUAL_THREADS)) {
      val newVirtualThreadPerTaskExecutor =
        classOf[Executors].getMethod("newVirtualThreadPerTaskExecutor")
      val service = newVirtualThreadPerTaskExecutor.invoke(null).asInstanceOf[ExecutorService]
      threadPool.setVirtualThreadsExecutor(service)
    }
    // 设置为守护线程，不阻止 JVM 退出
    threadPool.setDaemon(true)
    val server = new Server(threadPool)

    // 配置 HTTP 参数，关闭版本信息泄露，提升安全性
    val httpConfig = new HttpConfiguration()
    logDebug("Using setSendServerVersion: false")
    httpConfig.setSendServerVersion(false)
    logDebug("Using setSendXPoweredBy: false")
    httpConfig.setSendXPoweredBy(false)

    // 创建 HTTP 连接连接器，使用守护线程调度器
    val connector = new ServerConnector(
      server,
      null,
      // Call this full constructor to set this, which forces daemon threads:
      new ScheduledExecutorScheduler("RestSubmissionServer-JettyScheduler", true),
      null,
      -1,
      -1,
      new HttpConnectionFactory(httpConfig))
    connector.setHost(host)
    connector.setPort(startPort)
    // Windows 下不开启地址复用，非 Windows 开启
    connector.setReuseAddress(!Utils.isWindows)
    server.addConnector(connector)

    // 创建 Servlet 上下文，注册所有路由映射
    val mainHandler = new ServletContextHandler
    mainHandler.setServer(server)
    mainHandler.setContextPath("/")
    contextToServlet.foreach { case (prefix, servlet) =>
      mainHandler.addServlet(new ServletHolder(servlet), prefix)
    }
    // 注册用户自定义过滤器
    addFilters(mainHandler)
    server.setHandler(mainHandler)
    // 启动 Jetty 服务器
    server.start()
    val boundPort = connector.getLocalPort
    (server, boundPort)
  }

  /**
   * 根据配置注册自定义过滤器到 Servlet 上下文
   * @param handler Servlet 上下文处理器
   */
  private def addFilters(handler: ServletContextHandler): Unit = {
    masterConf.get(MASTER_REST_SERVER_FILTERS).foreach { filter =>
      val params = masterConf.getAllWithPrefix(s"spark.$filter.param.").toMap
      val holder = new FilterHolder()
      holder.setClassName(filter)
      params.foreach { case (k, v) => holder.setInitParameter(k, v) }
      handler.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))
    }
  }

  /**
   * 停止 REST 服务器，释放所有资源
   */
  def stop(): Unit = {
    _server.foreach(_.stop())
  }
}

/**
 * RestSubmissionServer 伴生对象，定义协议常量和HTTP状态码
 */
private[rest] object RestSubmissionServer {
  // 协议版本号，与客户端保持一致
  val PROTOCOL_VERSION = RestSubmissionClient.PROTOCOL_VERSION
  // 未知协议版本自定义HTTP状态码
  val SC_UNKNOWN_PROTOCOL_VERSION = 468
}

/**
 * 所有REST API Servlet的抽象基类，提供通用的JSON处理、错误处理、参数解析和响应序列化能力
 * 子类只需实现具体业务逻辑即可
 *
 * An abstract servlet for handling requests passed to the [[RestSubmissionServer]].
 *
 * 【学习型注释】
 * RestServlet 是所有 REST API 处理器的基类，提供通用的响应序列化、错误处理等功能。
 * 子类只需实现具体的业务逻辑（如提交、杀死、查询状态等）。
 */
private[rest] abstract class RestServlet extends HttpServlet with Logging {

  /**
   * 序列化响应对象为JSON并写入HTTP响应，发送前验证响应结构正确性
   * @param responseMessage 响应对象
   * @param responseServlet HTTP响应对象
   */
  protected def sendResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): Unit = {
    val message = validateResponse(responseMessage, responseServlet)
    responseServlet.setContentType("application/json")
    responseServlet.setCharacterEncoding("utf-8")
    responseServlet.getWriter.write(message.toJson)
  }

  /**
   * 对比客户端请求JSON与服务端消息结构，找出客户端发送的服务端不认识的未知字段，用于协议兼容检查
   * @param requestJson 客户端原始请求JSON字符串
   * @param requestMessage 服务端解析后的消息对象
   * @return 未知字段名数组
   */
  protected def findUnknownFields(
      requestJson: String,
      requestMessage: SubmitRestProtocolMessage): Array[String] = {
    val clientSideJson = parse(requestJson)
    val serverSideJson = parse(requestMessage.toJson)
    val Diff(_, _, unknown) = clientSideJson.diff(serverSideJson)
    unknown match {
      case j: JObject => j.obj.map { case (k, _) => k }.toArray
      case _ => Array.empty[String] // No difference
    }
  }

  /**
   * 将异常栈格式化为可读字符串，用于错误响应
   * @param e 异常对象
   * @return 格式化后的异常堆栈字符串
   */
  protected def formatException(e: Throwable): String = {
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    s"$e\n$stackTraceString"
  }

  /**
   * 构造错误响应对象，填入当前Spark版本和错误信息
   * @param message 错误描述信息
   * @return 错误响应对象
   */
  protected def handleError(message: String): ErrorResponse = {
    val e = new ErrorResponse
    e.serverSparkVersion = sparkVersion
    e.message = message
    e
  }

  /**
   * 从URL路径中解析提交ID，提交ID是路径的第一段
   * 例如 /[submissionId]/other/path，返回submissionId
   * @param path URL路径
   * @return 解析到的非空提交ID，非法路径返回None
   */
  protected def parseSubmissionId(path: String): Option[String] = {
    if (path == null || path.isEmpty) {
      None
    } else {
      path.stripPrefix("/").split("/").headOption.filter(_.nonEmpty)
    }
  }

  /**
   * 验证响应对象结构是否合法，验证失败返回错误响应
   * @param responseMessage 待验证的响应对象
   * @param responseServlet HTTP响应对象，用于设置状态码
   * @return 验证通过返回原响应对象，验证失败返回错误响应对象
   */
  private def validateResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    try {
      responseMessage.validate()
      responseMessage
    } catch {
      case e: Exception =>
        responseServlet.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        handleError("Internal server error: " + formatException(e))
    }
  }
}

/**
 * 处理杀死指定应用请求的Servlet抽象类，处理POST /{version}/submissions/kill/{submissionId} 请求
 * A servlet for handling kill requests passed to the [[RestSubmissionServer]].
 *
 * 处理杀死应用的 REST 请求（POST /kill/{submissionId}）。
 */
private[rest] abstract class KillRequestServlet extends RestServlet {

  /**
   * 处理POST杀死应用请求，解析URL中的提交ID，调用业务处理方法
   * @param request HTTP请求
   * @param response HTTP响应
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleKill).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in kill request.")
    }
    sendResponse(responseMessage, response)
  }

  /**
   * 子类实现具体杀死应用业务逻辑
   * @param submissionId 待杀死的应用提交ID
   * @return 杀死操作响应对象
   */
  protected def handleKill(submissionId: String): KillSubmissionResponse
}

/**
 * 处理杀死所有应用请求的Servlet抽象类，处理POST /{version}/submissions/killall 请求
 * A servlet for handling killAll requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class KillAllRequestServlet extends RestServlet {

  /**
   * 处理POST杀死所有应用请求，调用Master执行杀死操作并返回结果
   * @param request HTTP请求
   * @param response HTTP响应
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val responseMessage = handleKillAll()
    sendResponse(responseMessage, response)
  }

  /**
   * 子类实现具体杀死所有应用业务逻辑
   * @return 杀死所有操作响应对象
   */
  protected def handleKillAll(): KillAllSubmissionResponse
}

/**
 * 处理清理已完成应用请求的Servlet抽象类，处理POST /{version}/submissions/clear 请求
 * A servlet for handling clear requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class ClearRequestServlet extends RestServlet {

  /**
   * 处理POST清理请求，清理Master中已完成的应用信息
   * @param request HTTP请求
   * @param response HTTP响应
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val responseMessage = handleClear()
    sendResponse(responseMessage, response)
  }

  /**
   * 子类实现具体清理已完成应用业务逻辑
   * @return 清理操作响应对象
   */
  protected def handleClear(): ClearResponse
}

/**
 * 处理Master就绪检查请求的Servlet抽象类，处理GET /{version}/submissions/readyz 请求，用于健康检查
 * A servlet for handling readyz requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class ReadyzRequestServlet extends RestServlet {

  /**
   * 处理GET就绪检查请求，返回Master就绪状态
   * @param request HTTP请求
   * @param response HTTP响应
   */
  protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val readyzResponse = handleReadyz()
    val responseMessage = if (readyzResponse.success) {
      response.setStatus(HttpServletResponse.SC_OK)
      readyzResponse
    } else {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE)
      handleError("Master is not ready.")
    }
    sendResponse(responseMessage, response)
  }

  /**
   * 子类实现具体就绪状态检查逻辑
   * @return 就绪检查响应对象
   */
  protected def handleReadyz(): ReadyzResponse
}

/**
 * 处理应用状态查询请求的Servlet抽象类，处理GET /{version}/submissions/status/{submissionId} 请求
 * A servlet for handling status requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class StatusRequestServlet extends RestServlet {

  /**
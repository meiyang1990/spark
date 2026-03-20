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
   * 启动 REST 服务器并返回绑定的端口号。
   */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, masterConf)
    _server = Some(server)
    logInfo(log"Started REST server for submitting applications on ${MDC(HOST, host)}" +
      log" with port ${MDC(PORT, boundPort)}")
    boundPort
  }

  /**
   * 将 Servlet 映射到对应的上下文路径并绑定到服务器。
   * 返回启动的服务器实例和绑定的端口号。
   */
  private def doStart(startPort: Int): (Server, Int) = {
    // 创建 Jetty 线程池，使用守护线程
    val threadPool = new QueuedThreadPool(masterConf.get(MASTER_REST_SERVER_MAX_THREADS))
    threadPool.setName(getClass().getSimpleName())
    // Java 21+ 支持虚拟线程，可以提升并发性能
    if (Utils.isJavaVersionAtLeast21 && masterConf.get(MASTER_REST_SERVER_VIRTUAL_THREADS)) {
      val newVirtualThreadPerTaskExecutor =
        classOf[Executors].getMethod("newVirtualThreadPerTaskExecutor")
      val service = newVirtualThreadPerTaskExecutor.invoke(null).asInstanceOf[ExecutorService]
      threadPool.setVirtualThreadsExecutor(service)
    }
    threadPool.setDaemon(true)
    val server = new Server(threadPool)

    // 配置 HTTP 连接，隐藏服务器版本信息以提升安全性
    val httpConfig = new HttpConfiguration()
    logDebug("Using setSendServerVersion: false")
    httpConfig.setSendServerVersion(false)
    logDebug("Using setSendXPoweredBy: false")
    httpConfig.setSendXPoweredBy(false)

    // 创建 HTTP 连接器
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
    connector.setReuseAddress(!Utils.isWindows)
    server.addConnector(connector)

    // 创建 Servlet 上下文处理器，注册所有路由
    val mainHandler = new ServletContextHandler
    mainHandler.setServer(server)
    mainHandler.setContextPath("/")
    contextToServlet.foreach { case (prefix, servlet) =>
      mainHandler.addServlet(new ServletHolder(servlet), prefix)
    }
    // 添加自定义过滤器（如果配置了的话）
    addFilters(mainHandler)
    server.setHandler(mainHandler)
    server.start()
    val boundPort = connector.getLocalPort
    (server, boundPort)
  }

  /**
   * Add filters, if any, to the given ServletContextHandlers.
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

  def stop(): Unit = {
    _server.foreach(_.stop())
  }
}

private[rest] object RestSubmissionServer {
  val PROTOCOL_VERSION = RestSubmissionClient.PROTOCOL_VERSION
  val SC_UNKNOWN_PROTOCOL_VERSION = 468
}

/**
 * An abstract servlet for handling requests passed to the [[RestSubmissionServer]].
 *
 * 【学习型注释】
 * RestServlet 是所有 REST API 处理器的基类，提供通用的响应序列化、错误处理等功能。
 * 子类只需实现具体的业务逻辑（如提交、杀死、查询状态等）。
 */
private[rest] abstract class RestServlet extends HttpServlet with Logging {

  /**
   * 将响应消息序列化为 JSON 并发送给客户端。
   * 发送前会验证响应的正确性。
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
   * Return any fields in the client request message that the server does not know about.
   *
   * The mechanism for this is to reconstruct the JSON on the server side and compare the
   * diff between this JSON and the one generated on the client side. Any fields that are
   * only in the client JSON are treated as unexpected.
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

  /** Return a human readable String representation of the exception. */
  protected def formatException(e: Throwable): String = {
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    s"$e\n$stackTraceString"
  }

  /** Construct an error message to signal the fact that an exception has been thrown. */
  protected def handleError(message: String): ErrorResponse = {
    val e = new ErrorResponse
    e.serverSparkVersion = sparkVersion
    e.message = message
    e
  }

  /**
   * Parse a submission ID from the relative path, assuming it is the first part of the path.
   * For instance, we expect the path to take the form /[submission ID]/maybe/something/else.
   * The returned submission ID cannot be empty. If the path is unexpected, return None.
   */
  protected def parseSubmissionId(path: String): Option[String] = {
    if (path == null || path.isEmpty) {
      None
    } else {
      path.stripPrefix("/").split("/").headOption.filter(_.nonEmpty)
    }
  }

  /**
   * Validate the response to ensure that it is correctly constructed.
   *
   * If it is, simply return the message as is. Otherwise, return an error response instead
   * to propagate the exception back to the client and set the appropriate error code.
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
 * A servlet for handling kill requests passed to the [[RestSubmissionServer]].
 *
 * 处理杀死应用的 REST 请求（POST /kill/{submissionId}）。
 */
private[rest] abstract class KillRequestServlet extends RestServlet {

  /**
   * 处理 POST 请求，从 URL 中解析 submissionId 并杀死对应的 Driver。
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

  protected def handleKill(submissionId: String): KillSubmissionResponse
}

/**
 * A servlet for handling killAll requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class KillAllRequestServlet extends RestServlet {

  /**
   * Have the Master kill all drivers and return an appropriate response to the client.
   * Otherwise, return error.
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val responseMessage = handleKillAll()
    sendResponse(responseMessage, response)
  }

  protected def handleKillAll(): KillAllSubmissionResponse
}

/**
 * A servlet for handling clear requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class ClearRequestServlet extends RestServlet {

  /**
   * Clear the completed drivers and apps.
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val responseMessage = handleClear()
    sendResponse(responseMessage, response)
  }

  protected def handleClear(): ClearResponse
}

/**
 * A servlet for handling readyz requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class ReadyzRequestServlet extends RestServlet {

  /**
   * Return the status of master is ready or not.
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

  protected def handleReadyz(): ReadyzResponse
}

/**
 * A servlet for handling status requests passed to the [[RestSubmissionServer]].
 */
private[rest] abstract class StatusRequestServlet extends RestServlet {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleStatus).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in status request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleStatus(submissionId: String): SubmissionStatusResponse
}

/**
 * A servlet for handling submit requests passed to the [[RestSubmissionServer]].
 *
 * 处理应用提交的 REST 请求（POST /create）。
 * 这是 spark-submit --deploy-mode cluster 时实际调用的后端接口。
 */
private[rest] abstract class SubmitRequestServlet extends RestServlet {

  /**
   * 处理 POST 请求，解析 JSON 格式的提交请求并将应用提交给 Master。
   * 请求体是 SubmitRestProtocolRequest 的 JSON 序列化形式。
   */
  protected override def doPost(
      requestServlet: HttpServletRequest,
      responseServlet: HttpServletResponse): Unit = {
    val responseMessage =
      try {
        val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
        val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
        // The response should have already been validated on the client.
        // In case this is not true, validate it ourselves to avoid potential NPEs.
        requestMessage.validate()
        handleSubmit(requestMessageJson, requestMessage, responseServlet)
      } catch {
        // The client failed to provide a valid JSON, so this is not our fault
        case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
          responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError("Malformed request: " + formatException(e))
      }
    sendResponse(responseMessage, responseServlet)
  }

  protected def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse
}

/**
 * A default servlet that handles error cases that are not captured by other servlets.
 */
private class ErrorServlet extends RestServlet {
  private val serverVersion = RestSubmissionServer.PROTOCOL_VERSION

  /** Service a faulty request by returning an appropriate error message to the client. */
  protected override def service(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val path = request.getPathInfo
    val parts = path.stripPrefix("/").split("/").filter(_.nonEmpty).toList
    var versionMismatch = false
    var msg =
      parts match {
        case Nil =>
          // http://host:port/
          "Missing protocol version."
        case `serverVersion` :: Nil =>
          // http://host:port/correct-version
          "Missing the /submissions prefix."
        case `serverVersion` :: "submissions" :: tail =>
          // http://host:port/correct-version/submissions/*
          "Missing an action: please specify one of /create, /kill, /killall, /clear, /status, " +
            "or /readyz."
        case unknownVersion :: tail =>
          // http://host:port/unknown-version/*
          versionMismatch = true
          s"Unknown protocol version '$unknownVersion'."
        case _ =>
          "Malformed path."
      }
    msg += s" Please submit requests through http://[host]:[port]/$serverVersion/submissions/..."
    val error = handleError(msg)
    // If there is a version mismatch, include the highest protocol version that
    // this server supports in case the client wants to retry with our version
    if (versionMismatch) {
      error.highestProtocolVersion = serverVersion
      response.setStatus(RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION)
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    sendResponse(error, response)
  }
}

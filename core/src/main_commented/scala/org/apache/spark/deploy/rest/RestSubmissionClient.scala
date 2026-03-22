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

import java.io.{DataOutputStream, FileNotFoundException}
import java.net.{ConnectException, HttpURLConnection, SocketException, URI, URL}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import jakarta.servlet.http.HttpServletResponse

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf, SparkException}
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.util.Utils

/**
 * 文件说明: Spark Standalone 集群 REST 应用提交客户端，实现通过 REST API 向 Master 提交、终止、查询应用任务
 *
 * A client that submits applications to a [[RestSubmissionServer]].
 *
 * In protocol version v1, the REST URL takes the form http://[host:port]/v1/submissions/[action],
 * where [action] can be one of create, kill, or status. Each type of request is represented in
 * an HTTP message sent to the following prefixes:
 *   (1) submit - POST to /submissions/create
 *   (2) kill - POST /submissions/kill/[submissionId]
 *   (3) status - GET /submissions/status/[submissionId]
 *
 * In the case of (1), parameters are posted in the HTTP body in the form of JSON fields.
 * Otherwise, the URL fully specifies the intended action of the client.
 *
 * Since the protocol is expected to be stable across Spark versions, existing fields cannot be
 * added or removed, though new optional fields can be added. In the rare event that forward or
 * backward compatibility is broken, Spark must introduce a new protocol version (e.g. v2).
 *
 * The client and the server must communicate using the same version of the protocol. If there
 * is a mismatch, the server will respond with the highest protocol version it supports. A future
 * implementation of this client can use that information to retry using the version specified
 * by the server.
 */
private[spark] class RestSubmissionClient(master: String) extends Logging {
  import RestSubmissionClient._

  // 解析Master地址，支持高可用场景下多个Master地址
  private val masters: Array[String] = if (master.startsWith("spark://")) {
    Utils.parseStandaloneMasterUrls(master)
  } else {
    Array(master)
  }

  // 记录连接失败的Master地址集合，用于判断是否所有Master都不可用
  private val lostMasters = new mutable.HashSet[String]

  /**
   * 提交应用部署请求到Standalone集群
   * @param request 创建应用提交请求对象
   * @return 服务端返回的响应对象
   * 提交成功后会轮询应用状态并输出给用户，失败则返回错误信息
   */
  def createSubmission(request: CreateSubmissionRequest): SubmitRestProtocolResponse = {
    logInfo(log"Submitting a request to launch an application in ${MDC(MASTER_URL, master)}.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    // 轮询所有Master，直到找到一个可以处理请求的可用Master
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getSubmitUrl(m)
      try {
        response = postJson(url, request.toJson)
        response match {
          case s: CreateSubmissionResponse =>
            if (s.success) {
              reportSubmissionStatus(s)
              handleRestResponse(s)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /**
   * 请求终止指定提交ID的应用
   * @param submissionId 应用提交ID
   * @return 服务端返回的响应对象
   */
  def killSubmission(submissionId: String): SubmitRestProtocolResponse = {
    logInfo(log"Submitting a request to kill submission " +
      log"${MDC(SUBMISSION_ID, submissionId)} in " +
      log"${MDC(MASTER_URL, master)}.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getKillUrl(m, submissionId)
      try {
        response = post(url)
        response match {
          case k: KillSubmissionResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /**
   * 请求终止所有正在运行的应用
   * @return 服务端返回的响应对象
   */
  def killAllSubmissions(): SubmitRestProtocolResponse = {
    logInfo(log"Submitting a request to kill all submissions in ${MDC(MASTER_URL, master)}.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getKillAllUrl(m)
      try {
        response = post(url)
        response match {
          case k: KillAllSubmissionResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /**
   * 请求清理所有已完成的提交和应用
   * @return 服务端返回的响应对象
   */
  def clear(): SubmitRestProtocolResponse = {
    logInfo(log"Submitting a request to clear ${MDC(MASTER_URL, master)}.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getClearUrl(m)
      try {
        response = post(url)
        response match {
          case k: ClearResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /**
   * 检查Master服务就绪状态
   * @return 服务端返回的就绪状态响应
   */
  def readyz(): SubmitRestProtocolResponse = {
    logInfo(log"Submitting a request to check the status of ${MDC(MASTER_URL, master)}.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = new ErrorResponse
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getReadyzUrl(m)
      try {
        response = get(url)
        response match {
          case k: ReadyzResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /**
   * 查询指定提交ID的应用状态
   * @param submissionId 应用提交ID
   * @param quiet 是否静默模式（不输出日志）
   * @return 服务端返回的状态响应对象
   */
  def requestSubmissionStatus(
      submissionId: String,
      quiet: Boolean = false): SubmitRestProtocolResponse = {
      logInfo(log"Submitting a request for the status of submission " +
      log"${MDC(SUBMISSION_ID, submissionId)} in " +
      log"${MDC(MASTER_URL, master)}.")

    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getStatusUrl(m, submissionId)
      try {
        response = get(url)
        response match {
          case s: SubmissionStatusResponse if s.success =>
            if (!quiet) {
              handleRestResponse(s)
            }
            handled = true
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /**
   * 构造应用提交请求对象，填充所有必要参数
   * @param appResource 应用jar包路径
   * @param mainClass 应用入口主类
   * @param appArgs 应用启动参数
   * @param sparkProperties Spark配置属性
   * @param environmentVariables 环境变量
   * @return 构造完成并验证过的提交请求对象
   */
  def constructSubmitRequest(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      sparkProperties: Map[String, String],
      environmentVariables: Map[String, String]): CreateSubmissionRequest = {
    val message = new CreateSubmissionRequest
    message.clientSparkVersion = sparkVersion
    message.appResource = appResource
    message.mainClass = mainClass
    message.appArgs = appArgs
    message.sparkProperties = sparkProperties
    message.environmentVariables = environmentVariables
    message.validate()
    message
  }

  /**
   * 发送GET请求到指定URL
   * @param url 目标请求URL
   * @return 服务端解析验证后的响应对象
   */
  private def get(url: URL): SubmitRestProtocolResponse = {
    logDebug(s"Sending GET request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    readResponse(conn)
  }

  /**
   * 发送POST请求到指定URL（无请求体）
   * @param url 目标请求URL
   * @return 服务端解析验证后的响应对象
   */
  private def post(url: URL): SubmitRestProtocolResponse = {
    logDebug(s"Sending POST request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    readResponse(conn)
  }

  /**
   * 发送带JSON请求体的POST请求到指定URL
   * @param url 目标请求URL
   * @param json 请求体JSON字符串
   * @return 服务端解析验证后的响应对象
   */
  private def postJson(url: URL, json: String): SubmitRestProtocolResponse = {
    logDebug(s"Sending POST request to server at $url:\n$json")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    // 设置请求头类型
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    // 允许写入请求体
    conn.setDoOutput(true)
    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        // 写入JSON请求体
        out.write(json.getBytes(StandardCharsets.UTF_8))
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        throw new SubmitRestConnectionException("Connect Exception when connect to server", e)
    }
    readResponse(conn)
  }

  /**
   * 读取并解析服务端HTTP响应，转换为协议响应对象
   * @param connection HTTP连接对象
   * @return 验证后的协议响应对象
   */
  private[rest] def readResponse(connection: HttpURLConnection): SubmitRestProtocolResponse = {
    // scalastyle:off executioncontextglobal
    import scala.concurrent.ExecutionContext.Implicits.global
    // scalastyle:on executioncontextglobal
    val responseFuture = Future {
      val responseCode = connection.getResponseCode

      // 响应码非200，处理错误情况
      if (responseCode != HttpServletResponse.SC_OK) {
        val errString = Some(Source.fromInputStream(connection.getErrorStream())
          .getLines().mkString("\n"))
        // 服务器内部错误且不是JSON响应，直接抛出异常
        if (responseCode == HttpServletResponse.SC_INTERNAL_SERVER_ERROR &&
          !connection.getContentType().contains("application/json")) {
          throw new SubmitRestProtocolException(s"Server responded with exception:\n${errString}")
        }
        logError(log"Server responded with error:\n${MDC(ERROR, errString)}")
        val error = new ErrorResponse
        // 协议版本不匹配，记录服务端支持的最高版本
        if (responseCode == RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION) {
          error.highestProtocolVersion = RestSubmissionServer.PROTOCOL_VERSION
        }
        error.message = errString.get
        error
      } else {
        val dataStream = connection.getInputStream

        // 响应体为空，抛出异常
        if (dataStream == null) {
          throw new SubmitRestProtocolException("Server returned empty body")
        }
        // 读取响应JSON并解析
        val responseJson = Source.fromInputStream(dataStream).mkString
        logDebug(s"Response from the server:\n$responseJson")
        val response = SubmitRestProtocolMessage.fromJson(responseJson)
        response.validate()
        response match {
          // 如果是错误响应，记录错误日志
          case error: ErrorResponse =>
            logError(log"Server responded with error:\n${MDC(ERROR, error.message)}")
            error
          // 正常响应直接返回
          case response: SubmitRestProtocolResponse => response
          // 非预期响应类型，抛出异常
          case unexpected =>
            throw new SubmitRestProtocolException(
              s"Message received from server was not a response:\n${unexpected.toJson}")
        }
      }
    }

    // scalastyle:off awaitresult
    try { Await.result(responseFuture, 10.seconds) } catch {
      // scalastyle:on awaitresult
      // 连接异常，包装为连接异常抛出
      case unreachable @ (_: FileNotFoundException | _: SocketException) =>
        throw new SubmitRestConnectionException("Unable to connect to server", unreachable)
      // 响应格式错误，包装为协议异常抛出
      case malformed @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
        throw new SubmitRestProtocolException("Malformed response received from server", malformed)
      // 请求超时，包装为连接异常抛出
      case timeout: TimeoutException =>
        throw new SubmitRestConnectionException("No response from server", timeout)
      case NonFatal(t) =>
        throw new SparkException("Exception while waiting for response", t)
    }
  }

  /** 获取创建应用提交的REST接口URL */
  private def getSubmitUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URI(s"$baseUrl/create").toURL
  }

  /** 获取终止单个应用的REST接口URL */
  private def getKillUrl(master: String, submissionId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URI(s"$baseUrl/kill/$submissionId").toURL
  }

  /** 获取终止所有应用的REST接口URL */
  private def getKillAllUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URI(s"$baseUrl/killall").toURL
  }
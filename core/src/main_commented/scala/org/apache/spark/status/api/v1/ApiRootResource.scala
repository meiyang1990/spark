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

/**
 * 文件说明：Spark状态REST API v1版本根资源，提供基于JAX-RS的JSON状态查询入口
 * 所属模块：Spark核心模块，用于暴露应用状态指标的REST接口，供UI和外部监控使用
 */
package org.apache.spark.status.api.v1

import java.util.zip.ZipOutputStream

import jakarta.servlet.ServletContext
import jakarta.servlet.http.HttpServletRequest
import jakarta.ws.rs._
import jakarta.ws.rs.core.{Context, Response}
import org.eclipse.jetty.ee10.servlet.{ServletContextHandler, ServletHolder}
import org.eclipse.jetty.server.handler.ContextHandler
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.SecurityManager
import org.apache.spark.ui.{SparkUI, UIUtils}

/**
 * 根API资源类，作为v1版本REST API的入口点，基于JAX-RS提供JSON格式的Spark应用状态服务
 * 所有返回结果使用api.scala中定义的公共类，保证二进制兼容性，自动通过Jackson转换为JSON
 */
@Path("/v1")
private[v1] class ApiRootResource extends ApiRequestContext {

  @Path("applications")
  def applicationList(): Class[ApplicationListResource] = classOf[ApplicationListResource]

  @Path("applications/{appId}")
  def application(): Class[OneApplicationResource] = classOf[OneApplicationResource]

  @GET
  @Path("version")
  def version(): VersionInfo = new VersionInfo(org.apache.spark.SPARK_VERSION)

}

/**
 * API根资源工具对象，用于创建Jersey Servlet上下文处理器，启动v1版本REST API服务
 */
private[spark] object ApiRootResource {

  /**
   * 创建并配置Jersey Servlet上下文处理器，用于处理所有/api/v1开头的请求
   * @param uiRoot UI根对象，提供应用信息访问接口
   * @return 配置完成的Servlet上下文处理器
   */
  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    // 创建无会话的Servlet上下文
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    // 设置API上下文路径
    jerseyContext.setContextPath("/api")
    // 创建Jersey Servlet容器
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    // 设置Jersey扫描的资源包路径
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    // 将UIRoot存入Servlet上下文供后续请求使用
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    // 映射所有API请求到该Servlet
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}

/**
 * UI根公共接口，被历史服务器和运行中应用UI共同实现，提供标准化的应用信息访问接口
 * 用于REST API统一获取应用状态数据，不需要区分是运行中应用还是历史应用
 */
private[spark] trait UIRoot {
  /**
   * 在指定应用的SparkUI实例上下文中执行代码
   * @param appId 应用ID
   * @param attemptId 尝试ID，None表示默认尝试
   * @param fn 要执行的代码，接收SparkUI为参数
   * @return 代码执行结果
   * @throws java.util.NoSuchElementException 如果指定应用/尝试不存在
   */
  def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T

  def getApplicationInfoList: Iterator[ApplicationInfo]

  def getApplicationInfoList(max: Int)(
      filter: ApplicationInfo => Boolean): Iterator[ApplicationInfo]

  def getApplicationInfo(appId: String): Option[ApplicationInfo]

  /**
   * 将指定应用的事件日志写入Zip输出流，仅历史服务器支持该功能
   * @param appId 应用ID
   * @param attemptId 尝试ID，None表示导出所有尝试的日志
   * @param zipStream 输出Zip流
   */
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit = {
    Response.serverError()
      .entity("Event logs are only available through the history server.")
      .status(Response.Status.SERVICE_UNAVAILABLE)
      .build()
  }
  def securityManager: SecurityManager

  /**
   * 检查用户是否有权限查看指定应用的UI
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @param user 用户名
   * @return 是否有权限
   */
  def checkUIViewPermissions(appId: String, attemptId: Option[String], user: String): Boolean
}

/**
 * 从Servlet上下文获取UIRoot实例的工具类，供API资源处理请求时使用
 */
private[v1] object UIRootFromServletContext {

  private val attribute = getClass.getCanonicalName

  /**
   * 将UIRoot实例存入Servlet上下文属性
   * @param contextHandler Jetty上下文处理器
   * @param uiRoot 要存储的UIRoot实例
   */
  def setUiRoot(contextHandler: ContextHandler, uiRoot: UIRoot): Unit = {
    contextHandler.setAttribute(attribute, uiRoot)
  }

  /**
   * 从Servlet上下文获取存储的UIRoot实例
   * @param context Servlet上下文
   * @return 存储的UIRoot实例
   */
  def getUiRoot(context: ServletContext): UIRoot = {
    context.getAttribute(attribute).asInstanceOf[UIRoot]
  }
}

/**
 * API请求上下文特质，为所有资源提供UIRoot和请求对象访问能力
 */
private[v1] trait ApiRequestContext {
  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  def uiRoot: UIRoot = UIRootFromServletContext.getUiRoot(servletContext)

}

/**
 * 应用特定资源基类，封装应用ID、尝试ID解析、SparkUI获取和权限检查逻辑
 * 所有针对特定应用的API资源都继承该特质，减少重复代码
 */
private[v1] trait BaseAppResource extends ApiRequestContext {

  @PathParam("appId") protected[this] var appId: String = _
  @PathParam("attemptId") protected[this] var attemptId: String = _

  /**
   * 在指定应用的SparkUI上下文中执行代码，内置权限检查和异常处理
   * @param fn 要执行的代码
   * @return 执行结果
   */
  protected def withUI[T](fn: SparkUI => T): T = {
    try {
      uiRoot.withSparkUI(appId, Option(attemptId)) { ui =>
        val user = httpRequest.getRemoteUser()
        // 检查用户查看权限
        if (!ui.securityManager.checkUIViewPermissions(user)) {
          throw new ForbiddenException(raw"""user "$user" is not authorized""")
        }
        fn(ui)
      }
    } catch {
      case _: NoSuchElementException =>
        // 应用不存在，返回404
        val appKey = Option(attemptId).map(appId + "/" + _).getOrElse(appId)
        throw new NotFoundException(s"no such app: $appKey")
    }
  }

  /**
   * 独立检查UI查看权限，用于不需要获取SparkUI的权限检查场景
   */
  protected def checkUIViewPermissions(): Unit = {
    try {
      val user = httpRequest.getRemoteUser()
      if (!uiRoot.checkUIViewPermissions(appId, Option(attemptId), user)) {
        throw new ForbiddenException(raw"""user "$user" is not authorized""")
      }
    } catch {
      case _: NoSuchElementException =>
        val appKey = Option(attemptId).map(appId + "/" + _).getOrElse(appId)
        throw new NotFoundException(s"no such app: $appKey")
    }
  }
}

/**
 * 403禁止访问异常
 */
private[v1] class ForbiddenException(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.FORBIDDEN, msg))

/**
 * 404资源不存在异常
 */
private[v1] class NotFoundException(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.NOT_FOUND, msg))

/**
 * 503服务不可用异常
 */
private[v1] class ServiceUnavailable(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.SERVICE_UNAVAILABLE, msg))

/**
 * 400参数错误异常
 */
private[v1] class BadParameterException(msg: String) extends WebApplicationException(
    UIUtils.buildErrorResponse(Response.Status.BAD_REQUEST, msg)) {
  def this(param: String, exp: String, actual: String) = {
    this(raw"""Bad value for parameter "$param".  Expected a $exp, got "$actual"""")
  }
}
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

package org.apache.spark.deploy.history

import java.util.zip.ZipOutputStream

import scala.util.control.NonFatal
import scala.xml.Node

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.ee10.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.Utils.addRenderLogHandler
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.History
import org.apache.spark.internal.config.UI._
import org.apache.spark.status.api.v1.{ApiRootResource, ApplicationInfo, UIRoot}
import org.apache.spark.ui.{SparkUI, UIUtils, WebUI}
import org.apache.spark.util.{ShutdownHookManager, SystemClock, Utils}

/**
 * 历史服务器 Web 服务，用于渲染展示已完成 Spark 应用的 SparkUI
 *
 * Standalone 模式下已经由 MasterWebUI 提供了该功能，因此 HistoryServer 主要用于
 * 其他部署模式（如 YARN、Kubernetes）下提供历史作业查看能力
 *
 * 日志目录结构：在指定的基础目录下，每个应用的事件日志维护在各自的子目录中，
 * 该结构与 EventLoggingListener 写入事件日志时的结构一致。
 *
 * 核心功能：
 * 1. 从事件日志目录读取已完成应用的历史信息
 * 2. 提供 Web UI 供用户查看历史作业执行详情、DAG、Executor 等信息
 * 3. 使用 ApplicationCache 缓存已加载的 SparkUI，避免重复解析提升性能
 * 4. 支持 REST API 查询应用列表和详细信息
 * 是非 Standalone 模式下查看历史 Spark 应用运行信息的主要入口
 */
class HistoryServer(
    conf: SparkConf,
    provider: ApplicationHistoryProvider,
    securityManager: SecurityManager,
    port: Int)
  extends WebUI(securityManager, securityManager.getSSLOptions("historyServer"),
    port, conf, name = "HistoryServerUI",
    // 通常历史服务器存储大量不同应用和用户的事件日志
    // 对比单应用使用的Spark LiveUI，历史服务器需要更多线程来提升并发能力，
    // 处理来自不同用户和客户端的请求
    poolSize = 1000)
  with Logging with UIRoot with ApplicationCacheOperations {

  val title = conf.get(History.HISTORY_SERVER_UI_TITLE)

  // 缓存保留的最大应用数量
  private val retainedApplications = conf.get(History.RETAINED_APPLICATIONS)

  // 应用列表页面最多展示的应用数量
  private[history] val maxApplications = conf.get(History.HISTORY_UI_MAX_APPS);

  // 应用历史 UI 缓存，复用已加载的 SparkUI 实例，避免重复解析事件日志
  private val appCache = new ApplicationCache(this, retainedApplications, new SystemClock())

  // 缓存指标，用于测试和监控
  val cacheMetrics = appCache.metrics

  // 应用UI加载Servlet，负责根据URL路径解析appId和attemptId，加载对应应用的SparkUI
  private val loaderServlet = new HttpServlet {
    // 处理GET请求，解析 /history/{appId}/{attemptId} 格式URL
    protected override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {

      res.setContentType("text/html;charset=utf-8")

      // 解析URL路径，提取appId和可选的attemptId
      // URL格式: /history/{appId} 或 /history/{appId}/{attemptId}
      val parts = Option(req.getPathInfo()).getOrElse("").split("/")
      // 路径至少需要包含appId，否则重定向到首页
      if (parts.length < 2) {
        res.sendRedirect("/")
      }

      val appId = parts(1)
      var shouldAppendAttemptId = false
      // URL未指定attemptId时，获取该应用最新一次attempt
      val attemptId = if (parts.length >= 3) {
        Some(parts(2))
      } else {
        val lastAttemptId = provider.getApplicationInfo(appId).flatMap(_.attempts.head.attemptId)
        if (lastAttemptId.isDefined) {
          shouldAppendAttemptId = true
          lastAttemptId
        } else {
          None
        }
      }

      // 尝试加载应用UI，先尝试空attemptId（单次attempt），失败后再尝试指定attemptId
      // 兼容单次和多次attempt混合场景，两种方式都尝试保证兼容性
      if (!loadAppUi(appId, None) && (attemptId.isEmpty || !loadAppUi(appId, attemptId))) {
        val msg = <div class="row">Application {appId} not found.</div>
        res.setStatus(HttpServletResponse.SC_NOT_FOUND)
        UIUtils.basicSparkPage(req, msg, "Not Found").foreach { n =>
          res.getWriter().write(n.toString)
        }
        return
      }

      // 这里不需要从缓存获取UI；上面的缓存加载已经完成了UI注册，
      // 只需要将用户重定向到请求的URI，对应UI就会正确处理请求
      // 同时保留原请求中的查询参数
      val redirect = if (shouldAppendAttemptId) {
        req.getRequestURI.stripSuffix("/") + "/" + attemptId.get + "/"
      } else {
        req.getRequestURI.stripSuffix("/") + "/"
      }
      val query = Option(req.getQueryString).map("?" + _).getOrElse("")
      res.sendRedirect(res.encodeRedirectURL(redirect + query))
    }

    // SPARK-5983 禁用TRACE方法避免安全风险
    protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
  }

  override def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    appCache.withSparkUI(appId, attemptId)(fn)
  }

  override def checkUIViewPermissions(appId: String, attemptId: Option[String],
      user: String): Boolean = {
    provider.checkUIViewPermissions(appId, attemptId, user)
  }

  initialize()

  /**
   * 初始化历史服务器。
   *
   * 启动后台线程，定期将UI展示的信息与存储目录中的事件日志进行同步。
   */
  def initialize(): Unit = {
    // 注册应用列表页面，展示所有已完成应用
    attachPage(new HistoryPage(this))
    // 注册日志查看页面
    attachPage(new LogPage(conf))

    // 注册REST API处理器，提供 /api/v1 接口
    attachHandler(ApiRootResource.getServletHandler(this))

    // 注册静态资源处理器，处理CSS、JS等静态文件
    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)
    addRenderLogHandler(this, conf)

    // 创建Servlet上下文，将加载器Servlet映射到 /history/* 路径
    val contextHandler = new ServletContextHandler
    contextHandler.setContextPath(HistoryServer.UI_PATH_PREFIX)
    contextHandler.addServlet(new ServletHolder(loaderServlet), "/*")
    attachHandler(contextHandler)
  }

  /** 绑定HTTP服务器端口，启动Web服务 */
  override def bind(): Unit = {
    super.bind()
  }

  /** 停止服务器并关闭文件系统 */
  override def stop(): Unit = {
    super.stop()
    provider.stop()
  }

  /** 将重建后的UI附加到服务器，必须在bind后调用 */
  override def attachSparkUI(
      appId: String,
      attemptId: Option[String],
      ui: SparkUI,
      completed: Boolean): Unit = {
    assert(serverInfo.isDefined, "HistoryServer must be bound before attaching SparkUIs")
    ui.getHandlers.foreach { handler =>
      serverInfo.get.addHandler(handler, ui.securityManager)
    }
  }

  /** 将重建后的UI从服务器分离，必须在bind后调用 */
  override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
    assert(serverInfo.isDefined, "HistoryServer must be bound before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
    provider.onUIDetached(appId, attemptId, ui)
  }

  /**
   * 获取应用UI实例以及是否已完成
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @return 如果找到，返回SparkUI实例和缓存需要的历史信息
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    provider.getAppUI(appId, attemptId)
  }

  /**
   * 获取可用应用列表，按结束时间降序排列
   *
   * @return 所有已知应用的列表迭代器
   */
  def getApplicationList(): Iterator[ApplicationInfo] = {
    provider.getListing()
  }

  def getEventLogsUnderProcess(): Int = {
    provider.getEventLogsUnderProcess()
  }

  def getLastUpdatedTime(): Long = {
    provider.getLastUpdatedTime()
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    getApplicationList()
  }

  override def getApplicationInfoList(max: Int)(
      filter: ApplicationInfo => Boolean): Iterator[ApplicationInfo] = {
    provider.getListing(max)(filter)
  }

  def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    provider.getApplicationInfo(appId)
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {
    provider.writeEventLogs(appId, attemptId, zipStream)
  }

  /**
   * @return 应用列表为空时展示的HTML内容
   */
  def emptyListingHtml(): Seq[Node] = {
    provider.getEmptyListingHtml()
  }

  /**
   * 获取提供端配置，在列表页面展示
   *
   * @return 包含提供端配置的Map
   */
  def getProviderConfig(): Map[String, String] = provider.getConfig()

  /**
   * 加载应用UI并附加到Web服务器
   * @param appId 应用ID
   * @param attemptId 可选尝试ID
   * @return 应用找到并加载成功返回true
   */
  private def loadAppUi(appId: String, attemptId: Option[String]): Boolean = {
    try {
      appCache.withSparkUI(appId, attemptId) { _ =>
        // 仅触发加载，不需要额外操作
      }
      true
    } catch {
      case NonFatal(e: NoSuchElementException) =>
        false
    }
  }

  /**
   * 用于诊断的字符串描述
   * @return 多行服务器状态描述
   */
  override def toString: String = {
    s"""
      | History Server;
      | provider = $provider
      | cache = $appCache
    """.stripMargin
  }
}

/**
 * 推荐通过 start-history-server.sh 和 stop-history-server.sh 脚本来启动和停止 HistoryServer。
 * 事件日志基础目录以及其他历史服务器配置都需要通过 $SPARK_HISTORY_OPTS 环境变量指定，例如：
 *
 *   export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events"
 *   ./sbin/start-history-server.sh
 *
 * 该方式会将 HistoryServer 作为 Spark 守护进程启动。
 *
 * HistoryServer 伴生对象，包含main方法作为历史服务器启动入口
 */
object HistoryServer extends Logging {
  private lazy val conf = new SparkConf

  val UI_PATH_PREFIX = "/history"

  /**
   * 历史服务器启动入口主方法
   * @param argStrings 命令行参数
   */
  def main(argStrings: Array[String]): Unit = {
    Utils.resetStructuredLogging()
    Utils.initDaemon(log)
    new HistoryServerArguments(conf, argStrings)
    initSecurity()
    val securityManager = createSecurityManager(conf)

    // 通过反射创建应用历史提供端实例，默认实现为FsHistoryProvider
    val providerName = conf.get(History.PROVIDER)
    val provider = Utils.classForName[ApplicationHistoryProvider](providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)

    val port = conf.get(History.HISTORY_SERVER_UI_PORT)

    // 创建并启动HistoryServer
    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()
    // 启动提供端后台线程，定期扫描事件日志目录更新应用列表
    provider.start()

    // 注册JVM关闭钩子，确保服务器优雅停止
    ShutdownHookManager.addShutdownHook { () => server.stop() }

    // 主线程无限等待，直到进程被手动停止
    while (true) { Thread.sleep(Int.MaxValue) }
  }

  /**
   * 创建安全管理器。
   * 关闭认证配置，使得历史服务器能够在开启了安全认证的Spark集群中启动。
   * @param config 安全管理器构造配置
   * @return 构造好的安全管理器，用于构建HistoryServer
   */
  private[history] def createSecurityManager(config: SparkConf): SecurityManager = {
    if (config.getBoolean(SecurityManager.SPARK_AUTH_CONF, false)) {
      logDebug(s"Clearing ${SecurityManager.SPARK_AUTH_CONF}")
      config.set(SecurityManager.SPARK_AUTH_CONF, "false")
    }

    if (config.get(ACLS_ENABLE)) {
      logInfo(log"${MDC(KEY, ACLS_ENABLE.key)} is configured, " +
        log"clearing it and only using ${MDC(KEY2, History.HISTORY_SERVER_UI_ACLS_ENABLE.key)}")
      config.set(ACLS_ENABLE, false)
    }

    new SecurityManager(config)
  }

  /**
   * 初始化Kerberos安全认证。
   * 如果启用Kerberos，使用keytab文件登录，让历史服务器能够访问受保护的HDFS目录，
   * 并支持票据过期后自动重新登录。
   */
  def initSecurity(): Unit = {
    // 如果需要访问开启安全认证(Kerberos)的HDFS，必须从keytab文件登录，
    // 这样才能在Kerberos票据过期后继续访问HDFS，Hadoop RPC会自动从keytab重新登录
    if (conf.get(History.KERBEROS_ENABLED)) {
      val principalName = conf.get(History.KERBEROS_PRINCIPAL)
        .getOrElse(throw new NoSuchElementException(History.KERBEROS_PRINCIPAL.key))
      val keytabFilename = conf.get(History.KERBEROS_KEYTAB)
        .getOrElse(throw new NoSuchElementException(History.KERBEROS_KEYTAB.key))
      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  private[history] def getAttemptURI(appId: String, attemptId: Option[String]): String = {
    val attemptSuffix = attemptId.map { id => s"/$id" }.getOrElse("")
    s"${HistoryServer.UI_PATH_PREFIX}/${appId}${attemptSuffix}"
  }

}
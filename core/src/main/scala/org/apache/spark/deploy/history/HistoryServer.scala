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
 * A web server that renders SparkUIs of completed applications.
 *
 * For the standalone mode, MasterWebUI already achieves this functionality. Thus, the
 * main use case of the HistoryServer is in other deploy modes (e.g. Yarn).
 *
 * The logging directory structure is as follows: Within the given base directory, each
 * application's event logs are maintained in the application's own sub-directory. This
 * is the same structure as maintained in the event log write code path in
 * EventLoggingListener.
 *
 * 【学习型注释】
 * HistoryServer 是 Spark 历史服务器的核心类，用于展示已完成应用的 SparkUI。
 * 主要功能：
 * 1. 从事件日志目录读取已完成应用的历史信息
 * 2. 提供 Web UI 供用户查看历史作业的执行详情、DAG、Executor 等信息
 * 3. 使用 ApplicationCache 缓存已加载的 SparkUI，避免重复解析
 * 4. 支持 REST API 查询应用列表和详细信息
 * 在非 Standalone 模式（如 YARN、K8s）下，HistoryServer 是查看历史作业的主要入口。
 */
class HistoryServer(
    conf: SparkConf,
    provider: ApplicationHistoryProvider,
    securityManager: SecurityManager,
    port: Int)
  extends WebUI(securityManager, securityManager.getSSLOptions("historyServer"),
    port, conf, name = "HistoryServerUI",
    // Usually, a History Server stores plenty of event logs for various applications and users
    // Comparing to Spark LiveUI which is generally for per application usage, it needs more
    // threads to increase concurrency to handle request from different users and clients.
    poolSize = 1000)
  with Logging with UIRoot with ApplicationCacheOperations {

  val title = conf.get(History.HISTORY_SERVER_UI_TITLE)

  // How many applications to retain
  private val retainedApplications = conf.get(History.RETAINED_APPLICATIONS)

  // How many applications the summary ui displays
  private[history] val maxApplications = conf.get(History.HISTORY_UI_MAX_APPS);

  // 应用历史 UI 缓存，用于复用已加载的 SparkUI 实例
  private val appCache = new ApplicationCache(this, retainedApplications, new SystemClock())

  // and its metrics, for testing as well as monitoring
  val cacheMetrics = appCache.metrics

  // 加载器 Servlet，负责根据 URL 路径解析 appId 和 attemptId，加载对应应用的 SparkUI
  private val loaderServlet = new HttpServlet {
    // 处理 GET 请求，解析 /history/{appId}/{attemptId} 形式的 URL
    protected override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {

      res.setContentType("text/html;charset=utf-8")

      // 解析 URL 路径，提取 appId 和可选的 attemptId
      // URL 格式: /history/{appId} 或 /history/{appId}/{attemptId}
      val parts = Option(req.getPathInfo()).getOrElse("").split("/")
      // 路径至少需要包含 appId
      if (parts.length < 2) {
        res.sendRedirect("/")
      }

      val appId = parts(1)
      var shouldAppendAttemptId = false
      // 如果 URL 中没有指定 attemptId，则尝试获取最新的 attempt
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

      // 尝试加载应用 UI，先尝试单次 attempt 模式，失败后再尝试多次 attempt 模式
      // 由于可能存在单次和多次 attempt 混合的情况，需要两种方式都尝试
      if (!loadAppUi(appId, None) && (attemptId.isEmpty || !loadAppUi(appId, attemptId))) {
        val msg = <div class="row">Application {appId} not found.</div>
        res.setStatus(HttpServletResponse.SC_NOT_FOUND)
        UIUtils.basicSparkPage(req, msg, "Not Found").foreach { n =>
          res.getWriter().write(n.toString)
        }
        return
      }

      // Note we don't use the UI retrieved from the cache; the cache loader above will register
      // the app's UI, and all we need to do is redirect the user to the same URI that was
      // requested, and the proper data should be served at that point.
      // Also, make sure that the redirect url contains the query string present in the request.
      val redirect = if (shouldAppendAttemptId) {
        req.getRequestURI.stripSuffix("/") + "/" + attemptId.get + "/"
      } else {
        req.getRequestURI.stripSuffix("/") + "/"
      }
      val query = Option(req.getQueryString).map("?" + _).getOrElse("")
      res.sendRedirect(res.encodeRedirectURL(redirect + query))
    }

    // SPARK-5983 ensure TRACE is not supported
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
   * Initialize the history server.
   *
   * This starts a background thread that periodically synchronizes information displayed on
   * this UI with the event logs in the provided base directory.
   *
   * 初始化历史服务器，注册页面处理器和 REST API 处理器。
   */
  def initialize(): Unit = {
    // 注册历史列表页面，展示所有已完成应用的列表
    attachPage(new HistoryPage(this))
    // 注册日志页面，用于查看事件日志内容
    attachPage(new LogPage(conf))

    // 注册 REST API 处理器，提供 /api/v1 接口
    attachHandler(ApiRootResource.getServletHandler(this))

    // 注册静态资源处理器（CSS、JS 等）
    addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)
    addRenderLogHandler(this, conf)

    // 创建 Servlet 上下文，将加载器 Servlet 映射到 /history/* 路径
    val contextHandler = new ServletContextHandler
    contextHandler.setContextPath(HistoryServer.UI_PATH_PREFIX)
    contextHandler.addServlet(new ServletHolder(loaderServlet), "/*")
    attachHandler(contextHandler)
  }

  /** Bind to the HTTP server behind this web interface. */
  override def bind(): Unit = {
    super.bind()
  }

  /** Stop the server and close the file system. */
  override def stop(): Unit = {
    super.stop()
    provider.stop()
  }

  /** Attach a reconstructed UI to this server. Only valid after bind(). */
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

  /** Detach a reconstructed UI from this server. Only valid after bind(). */
  override def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit = {
    assert(serverInfo.isDefined, "HistoryServer must be bound before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
    provider.onUIDetached(appId, attemptId, ui)
  }

  /**
   * Get the application UI and whether or not it is completed
   * @param appId application ID
   * @param attemptId attempt ID
   * @return If found, the Spark UI and any history information to be used in the cache
   */
  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    provider.getAppUI(appId, attemptId)
  }

  /**
   * Returns a list of available applications, in descending order according to their end time.
   *
   * @return List of all known applications.
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
   * @return html text to display when the application list is empty
   */
  def emptyListingHtml(): Seq[Node] = {
    provider.getEmptyListingHtml()
  }

  /**
   * Returns the provider configuration to show in the listing page.
   *
   * @return A map with the provider's configuration.
   */
  def getProviderConfig(): Map[String, String] = provider.getConfig()

  /**
   * Load an application UI and attach it to the web server.
   * @param appId application ID
   * @param attemptId optional attempt ID
   * @return true if the application was found and loaded.
   */
  private def loadAppUi(appId: String, attemptId: Option[String]): Boolean = {
    try {
      appCache.withSparkUI(appId, attemptId) { _ =>
        // Do nothing, just force the UI to load.
      }
      true
    } catch {
      case NonFatal(e: NoSuchElementException) =>
        false
    }
  }

  /**
   * String value for diagnostics.
   * @return a multi-line description of the server state.
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
 * The recommended way of starting and stopping a HistoryServer is through the scripts
 * start-history-server.sh and stop-history-server.sh. The path to a base log directory,
 * as well as any other relevant history server configuration, should be specified via
 * the $SPARK_HISTORY_OPTS environment variable. For example:
 *
 *   export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events"
 *   ./sbin/start-history-server.sh
 *
 * This launches the HistoryServer as a Spark daemon.
 *
 * 【学习型注释】
 * HistoryServer 伴生对象，包含 main 方法作为历史服务器的启动入口。
 * 推荐通过 start-history-server.sh 脚本启动，通过环境变量 SPARK_HISTORY_OPTS 配置参数。
 */
object HistoryServer extends Logging {
  private lazy val conf = new SparkConf

  val UI_PATH_PREFIX = "/history"

  /**
   * 历史服务器启动入口。
   * 1. 初始化日志和安全配置
   * 2. 创建 ApplicationHistoryProvider（默认为 FsHistoryProvider）
   * 3. 创建并绑定 HistoryServer
   * 4. 启动 provider 的后台扫描线程
   */
  def main(argStrings: Array[String]): Unit = {
    Utils.resetStructuredLogging()
    Utils.initDaemon(log)
    new HistoryServerArguments(conf, argStrings)
    // 初始化 Kerberos 安全认证（如果启用）
    initSecurity()
    val securityManager = createSecurityManager(conf)

    // 通过反射创建 ApplicationHistoryProvider 实例
    val providerName = conf.get(History.PROVIDER)
    val provider = Utils.classForName[ApplicationHistoryProvider](providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)

    val port = conf.get(History.HISTORY_SERVER_UI_PORT)

    // 创建并启动 HistoryServer
    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()
    // 启动 provider 的后台线程，定期扫描事件日志目录
    provider.start()

    // 注册 JVM 关闭钩子，确保优雅停止
    ShutdownHookManager.addShutdownHook { () => server.stop() }

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    // 主线程无限等待，直到进程被手动停止
    while (true) { Thread.sleep(Int.MaxValue) }
  }

  /**
   * Create a security manager.
   * This turns off security in the SecurityManager, so that the History Server can start
   * in a Spark cluster where security is enabled.
   * @param config configuration for the SecurityManager constructor
   * @return the security manager for use in constructing the History Server.
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
   * 初始化 Kerberos 安全认证。
   * 如果启用了 Kerberos，则使用 keytab 文件登录，以便能够访问受保护的 HDFS 目录。
   * 这样可以在 Kerberos ticket 过期后自动重新登录。
   */
  def initSecurity(): Unit = {
    // If we are accessing HDFS and it has security enabled (Kerberos), we have to login
    // from a keytab file so that we can access HDFS beyond the kerberos ticket expiration.
    // As long as it is using Hadoop rpc (hdfs://), a relogin will automatically
    // occur from the keytab.
    if (conf.get(History.KERBEROS_ENABLED)) {
      // 如果启用 Kerberos，必须配置 principal 和 keytab 参数
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

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

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, ExecutionException}

import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.UncheckedExecutionException
import jakarta.servlet.{DispatcherType, Filter, FilterChain, ServletException, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.eclipse.jetty.ee10.servlet.FilterHolder

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.metrics.source.Source
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Clock

/**
 * 文件说明: 历史服务器应用UI缓存管理器，负责已加载应用UI的缓存、加载、淘汰和生命周期管理
 *
 * 缓存保持已加载的应用UI，容量达到上限时自动淘汰最久未使用的应用，配合历史服务器实现多应用UI的高效访问
 */
private[history] class ApplicationCache(
    val operations: ApplicationCacheOperations,
    val retainedApplications: Int,
    val clock: Clock) extends Logging {

  /**
   * 跟踪已加载应用UI，包含缓存中活跃UI和已从缓存移除但尚未完成分离的UI，用于并发加载时的同步控制
   */
  private val loadedApps = new ConcurrentHashMap[CacheKey, CountDownLatch]()

  private val appLoader = new CacheLoader[CacheKey, CacheEntry] {

    /** 缓存未命中或缓存过期时，加载新的应用UI条目 */
    override def load(key: CacheKey): CacheEntry = {
      // 加载新条目之前等待旧UI的分离完成，保证一致性
      val removalLatch = loadedApps.get(key)
      if (removalLatch != null) {
        removalLatch.await()
      }
      val entry = loadApplicationEntry(key.appId, key.attemptId)
      loadedApps.put(key, new CountDownLatch(1))
      entry
    }

  }

  private val removalListener = new RemovalListener[CacheKey, CacheEntry] {

    /** 缓存淘汰时，触发UI分离操作，并同步通知等待加载的线程 */
    override def onRemoval(rm: RemovalNotification[CacheKey, CacheEntry]): Unit = try {
      metrics.evictionCount.inc()
      val key = rm.getKey
      logDebug(s"Evicting entry ${key}")
      operations.detachSparkUI(key.appId, key.attemptId, rm.getValue().loadedUI.ui)
    } finally {
      loadedApps.remove(rm.getKey()).countDown()
    }
  }

  // 基于Guava Cache构建LRU缓存，设置最大容量和淘汰监听器
  private val appCache: LoadingCache[CacheKey, CacheEntry] = {
    CacheBuilder.newBuilder()
        .maximumSize(retainedApplications)
        .removalListener(removalListener)
        .build(appLoader)
  }

  /**
   * 缓存使用指标收集
   */
  val metrics = new CacheMetrics("history.cache")

  /**
   * 根据应用ID和尝试ID获取缓存的UI条目，缓存未命中时自动加载
   * @param appId 应用ID
   * @param attemptId 尝试ID，可选
   * @return 缓存的UI条目
   */
  def get(appId: String, attemptId: Option[String] = None): CacheEntry = {
    try {
      appCache.get(new CacheKey(appId, attemptId))
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException) =>
        throw Option(e.getCause()).getOrElse(e)
    }
  }

  /**
   * 在持有应用UI读锁的情况下执行用户逻辑，防止使用过程中UI被缓存关闭淘汰
   * @param appId 应用ID
   * @param attemptId 尝试ID，可选
   * @param fn 需要执行的用户逻辑，入参为SparkUI
   * @return 用户逻辑执行结果
   */
  def withSparkUI[T](appId: String, attemptId: Option[String])(fn: SparkUI => T): T = {
    var entry = get(appId, attemptId)

    // 如果条目存在，需要重试直到获取到有效的条目才能执行
    entry.loadedUI.lock.readLock().lock()
    try {
      while (!entry.loadedUI.valid) {
        entry.loadedUI.lock.readLock().unlock()
        entry = null
        try {
          invalidate(new CacheKey(appId, attemptId))
          entry = get(appId, attemptId)
          metrics.loadCount.inc()
        } finally {
          if (entry != null) {
            entry.loadedUI.lock.readLock().lock()
          }
        }
      }

      fn(entry.loadedUI.ui)
    } finally {
      if (entry != null) {
        entry.loadedUI.lock.readLock().unlock()
      }
    }
  }

  /**
   * 获取当前缓存中应用UI数量
   * @return 缓存条目数量
   */
  def size(): Long = appCache.size()

  // 计时工具，统计传入代码块的执行时间
  private def time[T](t: Timer)(f: => T): T = {
    val timeCtx = t.time()
    try {
      f
    } finally {
      timeCtx.close()
    }
  }

  /**
   * 加载应用UI条目，从存储获取UI信息并挂载到Web服务器，未完成应用添加更新检查过滤器
   * @param appId 应用ID
   * @param attemptId 尝试ID，可选
   * @return 加载完成的缓存条目
   * @throws NoSuchElementException 应用不存在时抛出
   */
  @throws[NoSuchElementException]
  private def loadApplicationEntry(appId: String, attemptId: Option[String]): CacheEntry = {
    lazy val application = log"${MDC(APP_ID, appId)}/${MDC(APP_ATTEMPT_ID, attemptId.mkString)}"
    logDebug(log"Loading application Entry " + application)
    metrics.loadCount.inc()
    val loadedUI = time(metrics.loadTimer) {
      metrics.lookupCount.inc()
      operations.getAppUI(appId, attemptId) match {
        case Some(loadedUI) =>
          logDebug(log"Loaded application " + application)
          loadedUI
        case None =>
          metrics.lookupFailureCount.inc()
          // Guava缓存日志有限，所以这里输出自己的日志
          logInfo(log"Failed to load application attempt " + application)
          throw new NoSuchElementException(s"no application with application Id '$appId'" +
              attemptId.map { id => s" attemptId '$id'" }.getOrElse(" and no attempt Id"))
      }
    }
    try {
      // 判断应用是否已经运行完成
      val completed = loadedUI.ui.getApplicationInfoList.exists(_.attempts.last.completed)
      if (!completed) {
        // 未完成应用添加更新检查过滤器，处理后续可能的更新
        registerFilter(new CacheKey(appId, attemptId), loadedUI)
      }
      operations.attachSparkUI(appId, attemptId, loadedUI.ui, completed)
      new CacheEntry(loadedUI, completed)
    } catch {
      case e: Exception =>
        logWarning(log"Failed to initialize application UI for ${MDC(APP_ID, application)}", e)
        operations.detachSparkUI(appId, attemptId, loadedUI.ui)
        throw e
    }
  }

  /**
   * 输出缓存状态和指标信息，用于测试和诊断
   * @return 缓存状态字符串
   */
  override def toString: String = {
    val sb = new StringBuilder(s"ApplicationCache(" +
          s" retainedApplications= $retainedApplications)")
    sb.append(s"; time= ${clock.getTimeMillis()}")
    sb.append(s"; entry count= ${appCache.size()}\n")
    sb.append("----\n")
    appCache.asMap().asScala.foreach {
      case(key, entry) => sb.append(s"  $key -> $entry\n")
    }
    sb.append("----\n")
    sb.append(metrics)
    sb.append("----\n")
    sb.toString()
  }

  /**
   * 为未完成应用注册HTTP过滤器，用于处理UI更新检查和重定向
   * @param key 应用缓存键，包含appId和attemptId
   * @param loadedUI 需要挂载过滤器的Spark UI
   */
  private def registerFilter(key: CacheKey, loadedUI: LoadedAppUI): Unit = {
    require(loadedUI != null)
    val enumDispatcher = java.util.EnumSet.of(DispatcherType.ASYNC, DispatcherType.REQUEST)
    val filter = new ApplicationCacheCheckFilter(key, loadedUI, this)
    val holder = new FilterHolder(filter)
    require(loadedUI.ui.getHandlers != null, "null handlers")
    loadedUI.ui.getHandlers.foreach { handler =>
      handler.addFilter(holder, "/*", enumDispatcher)
    }
  }

  /**
   * 使指定缓存键的条目失效，触发淘汰
   * @param key 需要失效的缓存键
   */
  def invalidate(key: CacheKey): Unit = appCache.invalidate(key)

}

/**
 * 缓存条目，包装已加载的应用UI和应用完成状态
 * @param loadedUI 已加载的应用UI
 * @param completed 应用是否已完成，已完成应用不需要检查更新
 */
private[history] final class CacheEntry(
    val loadedUI: LoadedAppUI,
    val completed: Boolean) {

  override def toString: String = {
    s"UI ${loadedUI.ui}, completed=$completed"
  }
}

/**
 * 缓存键，基于应用ID和尝试Id唯一标识一个应用尝试
 * @param appId 应用ID
 * @param attemptId 尝试ID
 */
private[history] final case class CacheKey(appId: String, attemptId: Option[String]) {

  override def toString: String = {
    appId + attemptId.map { id => s"/$id" }.getOrElse("")
  }
}

/**
 * 缓存指标收集类，统计缓存各项操作 metrics，继承Source注册到Spark指标系统
 * @param prefix 指标名称前缀
 */
private[history] class CacheMetrics(prefix: String) extends Source {

  /* 各类指标计数器和计时器 */
  val lookupCount = new Counter()
  val lookupFailureCount = new Counter()
  val evictionCount = new Counter()
  val loadCount = new Counter()
  val loadTimer = new Timer()

  /** 计数器集合，用于注册和输出 */
  private val counters = Seq(
    ("lookup.count", lookupCount),
    ("lookup.failure.count", lookupFailureCount),
    ("eviction.count", evictionCount),
    ("load.count", loadCount))

  override val sourceName = "ApplicationCache"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  override def toString: String = {
    val sb = new StringBuilder()
    counters.foreach { case (name, counter) =>
      sb.append(name).append(" = ").append(counter.getCount).append('\n')
    }
    sb.toString()
  }
}

/**
 * 缓存操作接口，定义加载、挂载、分离应用UI的抽象，由实现类提供具体逻辑
 */
private[history] trait ApplicationCacheOperations {

  /**
   * 获取应用UI和更新探针
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @return 找到则返回LoadedAppUI，否则返回None
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI]

  /**
   * 将加载完成的UI挂载到Web服务器
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @param ui Spark UI实例
   * @param completed 应用是否已完成
   */
  def attachSparkUI(
      appId: String,
      attemptId: Option[String],
      ui: SparkUI,
      completed: Boolean): Unit

  /**
   * 从Web服务器分离卸载UI，释放资源
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @param ui 要卸载的Spark UI
   */
  def detachSparkUI(appId: String, attemptId: Option[String], ui: SparkUI): Unit

}

/**
 * Servlet过滤器，拦截未完成应用的HTTP请求，检查UI是否更新，如果已失效则重定向重新加载
 * 当应用更新后，过滤器会返回302重定向，让客户端重新获取更新后的UI
 */
private[history] class ApplicationCacheCheckFilter(
    key: CacheKey,
    loadedUI: LoadedAppUI,
    cache: ApplicationCache)
  extends Filter with Logging {

  /**
   * 过滤HTTP请求，检查UI有效性，有效则放行，无效则使缓存失效并重定向
   * @param request Servlet请求
   * @param response Servlet响应
   * @param chain 过滤器链
   */
  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {

    // 仅处理HTTP请求
    if (!(request.isInstanceOf[HttpServletRequest])) {
      throw new ServletException("This filter only works for HTTP/HTTPS")
    }
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]
    val requestURI = httpRequest.getRequestURI

    // 检查UI有效性，持有读锁保证并发安全
    loadedUI.lock.readLock().lock()
    if (loadedUI.valid) {
      try {
        chain.doFilter(request, response)
      } finally {
        loadedUI.lock.readLock.unlock()
      }
    } else {
      loadedUI.lock.readLock.unlock()
      // UI已失效，使缓存失效并重定向到当前URL，触发重新加载
      cache.invalidate(key)
      val queryStr = Option(httpRequest.getQueryString).map("?" + _).getOrElse("")
      val redirectUrl = httpResponse.encodeRedirectURL(requestURI + queryStr)
      httpResponse.sendRedirect(redirectUrl)
    }
  }
}
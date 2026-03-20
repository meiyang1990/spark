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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL, URLClassLoader}
import java.nio.ByteBuffer
import java.util.{Locale, Properties}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy

import scala.collection.immutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.{MDC => SLF4JMDC}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.Executor.{IDLE_TASK_THREAD_NAME, TASK_THREAD_NAME_PREFIX}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.{EXECUTOR_USER_CLASS_PATH_FIRST => EXECUTOR_USER_CLASS_PATH_FIRST_CONFIG}
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.metrics.source.JVMCPUSource
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler._
import org.apache.spark.serializer.SerializerHelper
import org.apache.spark.shuffle.{FetchFailedException, ShuffleBlockPusher}
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._

/**
 * 隔离会话状态的全局管理器。
 * 在 Spark Connect 场景下，多个客户端会话需要资源隔离（如独立的类加载器、JAR、文件等），
 * 此对象维护所有会话的权威存储映射。
 */
private[spark] object IsolatedSessionState {
  // 所有隔离会话的权威存储映射。会话创建时加入，清理时移除。
  // Guava 缓存仅用于 LRU 淘汰策略跟踪"活跃"会话，但此 map 才是真正的数据源。
  // 确保同一时刻每个 UUID 只有一个 IsolatedSessionState 实例。
  val sessions = new ConcurrentHashMap[String, IsolatedSessionState]()
}

/**
 * 表示 Executor 端的隔离会话状态，包含会话特定的类加载器、文件、JAR 和归档文件。
 * 此类管理这些资源的生命周期，并防止并发任务执行与缓存淘汰之间的竞争条件。
 *
 * == 架构设计 ==
 *
 * 会话通过两种机制管理：
 *  1. Guava LRU 缓存 (`isolatedSessionCache`)：用于活跃会话查找，带有大小限制
 *  2. 权威映射表 (`IsolatedSessionState.sessions`)：跟踪所有会话直到清理完成
 *
 * Guava 缓存处理 LRU 淘汰，而权威映射确保同一时刻每个 UUID 只有一个
 * IsolatedSessionState 实例，并跟踪已淘汰但仍在使用中的会话。
 *
 * == 状态机 ==
 *
 * 每个会话有两个受同步锁保护的状态变量：
 *  - `refCount`: 当前使用此会话的任务数
 *  - `evicted`: 会话是否已从 Guava 缓存中淘汰
 *
 * 有效的状态转换：
 * {{{
 *   常规工作流（无竞争）：
 *   [创建] --> acquire() --> [活跃: refCount > 0]
 *                                    |
 *                               release() (所有任务完成)
 *                                    |
 *                                    v
 *                           [空闲: refCount = 0]
 *                                    |
 *                           markEvicted() (缓存淘汰)
 *                                    |
 *                                    v
 *                                 [清理]
 *
 *   竞争情况（任务运行中被淘汰）：
 *   [活跃: refCount > 0] --> markEvicted() --> [延迟: evicted = true]
 *                                                   |
 *             +--------------------------------------------+
 *             |                                            |
 *             v                                            v
 *   release() (最后一个任务)                        tryUnEvict()
 *             |                                            |
 *             v                                            v
 *          [清理]                               [活跃] (重回缓存)
 * }}}
 *
 * == 清理条件 ==
 *
 * 当同时满足以下条件时执行清理：`refCount == 0` 且 `evicted == true`。
 * 可能的触发方式：
 *  - 当调用 `markEvicted()` 时没有任务使用该会话，立即清理
 *  - 当最后一个任务调用 `release()` 时会话已被淘汰，延迟清理
 *
 * 清理操作包括关闭类加载器、删除会话文件、从权威映射中移除会话。
 *
 * == 并发保证 ==
 *
 * 核心设计思想：只要会话仍在使用（refCount > 0），它就保留在权威映射中，
 * 我们可以获取其实例。当新任务需要一个已从 LRU 缓存淘汰但仍在使用的会话时：
 *
 *  - 如果清理尚未开始（refCount > 0）：可以通过 `tryUnEvict()` 取消待处理的清理，
 *    将实例放回 LRU 缓存并安全重用。
 *
 *  - 如果清理已经开始（refCount 变为 0）：清理在锁内同步执行，必须在任何新任务
 *    继续之前完成。清理完成后，会话从权威映射中移除，创建新实例。
 *
 * 这种设计确保永远不会出现任务使用正在或已经被清理的会话的竞争条件。
 * `acquire()` 和 `tryUnEvict()` 方法有意分开：`tryUnEvict()` 仅从缓存加载器调用，
 * 以确保会话被放回 LRU 缓存，维护"未淘汰的会话始终在缓存中"的不变量。
 *
 * @param sessionUUID 会话唯一标识符
 * @param urlClassLoader 可变 URL 类加载器
 * @param replClassLoader REPL 类加载器
 * @param currentFiles 当前会话的文件映射（名称 -> 时间戳）
 * @param currentJars 当前会话的 JAR 映射（名称 -> 时间戳）
 * @param currentArchives 当前会话的归档文件映射（名称 -> 时间戳）
 * @param replClassDirUri REPL 类目录 URI（可选）
 */
private[spark] class IsolatedSessionState(
    val sessionUUID: String,
    var urlClassLoader: MutableURLClassLoader,
    var replClassLoader: ClassLoader,
    val currentFiles: HashMap[String, Long],
    val currentJars: HashMap[String, Long],
    val currentArchives: HashMap[String, Long],
    val replClassDirUri: Option[String]) extends Logging {

  // 使用此会话的运行中任务数量，通过 lock 同步访问
  private var refCount: Int = 0

  // 此会话是否已从缓存中淘汰，通过 lock 同步访问
  private var evicted: Boolean = false

  // 用于同步所有状态变更的锁对象
  private val lock = new Object

  /**
   * 增加引用计数，表示有任务正在使用此会话。
   * @return 如果会话成功获取返回 true，如果已被淘汰返回 false
   */
  def acquire(): Boolean = lock.synchronized {
    if (evicted) {
      false
    } else {
      refCount += 1
      true
    }
  }

  /**
   * 尝试取消会话的淘汰状态以便重用。
   * 此方法从缓存加载器调用，用于复用延迟清理的会话。
   * 调用者应在会话加入缓存后单独调用 acquire()。
   * @return 如果成功取消淘汰返回 true，如果已清理或 refCount 为 0 返回 false
   */
  def tryUnEvict(): Boolean = lock.synchronized {
    if (evicted && refCount > 0) {
      evicted = false
      logInfo(log"Session ${MDC(SESSION_ID, sessionUUID)} un-evicted, " +
        log"still in use by ${MDC(LogKeys.COUNT, refCount)} task(s)")
      true
    } else {
      false
    }
  }

  /** 递减引用计数。如果已淘汰且没有更多任务，则执行清理。 */
  def release(): Unit = lock.synchronized {
    refCount -= 1
    if (refCount == 0 && evicted) {
      cleanup()
    }
  }

  /** 将此会话标记为已淘汰。如果 refCount 为 0 则立即清理。 */
  def markEvicted(): Unit = lock.synchronized {
    evicted = true
    if (refCount == 0) {
      cleanup()
    } else {
      logInfo(log"Session ${MDC(SESSION_ID, sessionUUID)} evicted but still in use by " +
        log"${MDC(LogKeys.COUNT, refCount)} task(s), deferring cleanup")
    }
  }

  // 执行会话清理：关闭类加载器、删除文件、从映射中移除
  private def cleanup(): Unit = {
    // 关闭 urlClassLoader 以释放资源
    try {
      urlClassLoader match {
        case cl: URLClassLoader =>
          cl.close()
          logInfo(log"Closed urlClassLoader for session ${MDC(SESSION_ID, sessionUUID)}")
        case _ =>
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to close urlClassLoader for session " +
          log"${MDC(SESSION_ID, sessionUUID)}", e)
    }

    // 删除会话相关的本地文件目录
    val sessionBasedRoot = new File(SparkFiles.getRootDirectory(), sessionUUID)
    if (sessionBasedRoot.isDirectory && sessionBasedRoot.exists()) {
      Utils.deleteRecursively(sessionBasedRoot)
    }

    // 清理完成后从权威映射中移除会话
    IsolatedSessionState.sessions.remove(sessionUUID)
    logInfo(log"Session cleaned up: ${MDC(SESSION_ID, sessionUUID)}")
  }
}

/**
 * Spark Executor，由线程池支持来运行任务。
 *
 * Executor 是 Spark 应用程序在 Worker 节点上的执行进程，是分布式计算的核心组件。
 * 
 * == 主要职责 ==
 *  - 运行 Driver 分配的 Task，每个 Task 处理一个数据分区
 *  - 将 RDD 数据缓存到内存或磁盘，供后续 Stage 复用
 *  - 定期向 Driver 发送心跳和任务执行指标
 *  - 管理 Shuffle 数据的写入和读取
 *
 * == 支持的部署模式 ==
 *  - YARN：Hadoop 资源管理框架
 *  - Kubernetes：容器编排平台
 *  - Standalone：Spark 自带的集群管理器
 *
 * == 与 Driver 通信 ==
 * 使用内部 RPC 接口与 Driver 进行通信，汇报任务状态和指标。
 *
 * @param executorId Executor 唯一标识符
 * @param executorHostname Executor 所在主机名
 * @param env SparkEnv 运行环境
 * @param userClassPath 用户代码的类路径
 * @param isLocal 是否为本地模式
 * @param uncaughtExceptionHandler 未捕获异常处理器
 * @param resources 分配给此 Executor 的资源信息（如 GPU）
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = new SparkUncaughtExceptionHandler,
    resources: immutable.Map[String, ResourceInformation])
  extends Logging {

  logInfo(log"Starting executor ID ${MDC(LogKeys.EXECUTOR_ID, executorId)}" +
    log" on host ${MDC(HOST, executorHostname)}")
  logInfo(log"Running Spark version ${MDC(LogKeys.SPARK_VERSION, org.apache.spark.SPARK_VERSION)}")
  logInfo(log"OS info ${MDC(OS_NAME, Utils.osName)}," +
    log" ${MDC(OS_VERSION, Utils.osVersion)}, " +
    log"${MDC(OS_ARCH, Utils.osArch)}")
  logInfo(log"Java version ${MDC(JAVA_VERSION, Utils.javaVersion)}")

  // 标记 Executor 是否已关闭
  private val executorShutdown = new AtomicBoolean(false)
  // 注册关闭钩子，确保 Executor 正常关闭时执行清理
  val stopHookReference = ShutdownHookManager.addShutdownHook(
    () => stop()
  )

  // 空字节缓冲区常量，用于发送无数据的状态更新
  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private[executor] val conf = env.conf

  // SPARK-48131: 日志 MDC key 的兼容性处理（task_name 格式）
  private[executor] val taskNameMDCKey = if (conf.get(LEGACY_TASK_NAME_MDC_ENABLED)) {
    "mdc.taskName"
  } else {
    TASK_NAME.name.toLowerCase(Locale.ROOT)
  }

  // SPARK-40235: 使用 ReentrantLock 替代 synchronized 关键字更新依赖，
  // 允许任务在等待其他任务下载依赖时被中断并快速退出
  private val updateDependenciesLock = new ReentrantLock()

  // 校验主机名格式：不能包含 IP 或端口
  Utils.checkHost(executorHostname)
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // 确保本地报告的主机名与集群调度器使用的名称一致
  Utils.setCustomHostname(executorHostname)

  // 非本地模式下设置未捕获异常处理器
  // 任何线程因未捕获异常终止都会导致整个 Executor 进程退出，避免出现意外的停滞
  if (!isLocal) {
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // 创建任务执行线程池
  // 使用 UninterruptibleThread 运行任务，避免被 Thread.interrupt() 中断
  // 某些库（如 KAFKA-1894, HADOOP-10622）在被中断时会永久挂起
  private[executor] val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(s"$TASK_THREAD_NAME_PREFIX-%d")
      .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
  // 配置需要监控的文件系统 scheme 列表（用于 IO 指标采集）
  private val schemes = conf.get(EXECUTOR_METRICS_FILESYSTEM_SCHEMES)
    .toLowerCase(Locale.ROOT).split(",").map(_.trim).filter(_.nonEmpty)
  // Executor 指标数据源，暴露线程池、文件系统和任务相关指标
  private val executorSource = new ExecutorSource(threadPool, executorId, schemes)
  // 用于监督任务终止/取消的线程池
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // 正在被终止的任务及其 TaskReaper 的映射
  // 使用 HashMap 而非 ConcurrentHashMap，因为需要在同步块中进行额外操作
  // 目的是避免为同一任务的多次 killTask() 调用创建多个 TaskReaper
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  // Executor 指标源（可选），用于暴露更细粒度的 Executor 级别指标
  val executorMetricsSource =
    if (conf.get(METRICS_EXECUTORMETRICS_SOURCE_ENABLED)) {
      Some(new ExecutorMetricsSource)
    } else {
      None
    }

  // 非本地模式下初始化 BlockManager 和注册指标数据源
  if (!isLocal) {
    env.blockManager.initialize(conf.getAppId)
    env.metricsSystem.registerSource(executorSource)
    env.metricsSystem.registerSource(new JVMCPUSource())
    executorMetricsSource.foreach(_.register(env.metricsSystem))
    env.metricsSystem.registerSource(env.blockManager.shuffleMetricsSource)
  } else {
    // 本地模式下暂存 executorSource，实际注册在 SparkContext 中进行
    // 因为此时 appId 尚不可用
    Executor.executorSourceLocalModeOnly = executorSource
  }

  // 是否优先从用户 JAR 中加载类（而非 Spark JAR）
  private val userClassPathFirst = conf.get(EXECUTOR_USER_CLASS_PATH_FIRST_CONFIG)

  // 是否启用 TaskReaper 监控被终止/中断的任务
  private val taskReaperEnabled = conf.get(TASK_REAPER_ENABLED)

  // 检查致命错误时搜索异常链的最大深度
  private val killOnFatalErrorDepth = conf.get(KILL_ON_FATAL_ERROR_DEPTH)

  // 系统类加载器（父类加载器）
  private val systemLoader = Utils.getContextOrSparkClassLoader

  // 为给定的作业 Artifact 状态创建新的隔离会话
  private def newSessionState(jobArtifactState: JobArtifactState): IsolatedSessionState = {
    val currentFiles = new HashMap[String, Long]
    val currentJars = new HashMap[String, Long]
    val currentArchives = new HashMap[String, Long]
    val urlClassLoader =
      createClassLoader(currentJars, isStubbingEnabledForState(jobArtifactState.uuid),
        isDefaultState(jobArtifactState.uuid))
    val replClassLoader = addReplClassLoaderIfNeeded(
      urlClassLoader, jobArtifactState.replClassDirUri, jobArtifactState.uuid)
    val state = new IsolatedSessionState(
      jobArtifactState.uuid, urlClassLoader, replClassLoader,
      currentFiles,
      currentJars,
      currentArchives,
      jobArtifactState.replClassDirUri
    )
    // 立即存入权威会话映射，确保同一 UUID 同时只有一个会话
    IsolatedSessionState.sessions.put(jobArtifactState.uuid, state)
    state
  }

  // 判断是否为指定会话启用 UDF 存根（Spark Connect 场景）
  private def isStubbingEnabledForState(name: String) = {
    !isDefaultState(name) &&
      conf.get(CONNECT_SCALA_UDF_STUB_PREFIXES).nonEmpty
  }

  // 判断是否为默认会话（非 Spark Connect 会话）
  private def isDefaultState(name: String) = name == "default"

  // 类加载器隔离相关
  // 默认隔离组，不在缓存中，永不淘汰
  val defaultSessionState: IsolatedSessionState = newSessionState(JobArtifactState("default", None))

  // 隔离会话的 LRU 缓存，用于 Spark Connect 多会话场景
  // 缓存大小和过期时间可配置，淘汰时触发会话清理
  val isolatedSessionCache: Cache[String, IsolatedSessionState] = CacheBuilder.newBuilder()
    .maximumSize(conf.get(EXECUTOR_ISOLATED_SESSION_CACHE_SIZE))
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .removalListener(new RemovalListener[String, IsolatedSessionState]() {
      override def onRemoval(
          notification: RemovalNotification[String, IsolatedSessionState]): Unit = {
        val state = notification.getValue
        // 缓存仅用于隔离会话，默认会话不在缓存中
        assert(!isDefaultState(state.sessionUUID))
        // 标记为已淘汰。会话在清理前保留在权威映射中。
        // 如果 refCount > 0，清理延迟到所有任务释放后执行。
        state.markEvicted()
      }
    })
    .build[String, IsolatedSessionState]

  // 为序列化器设置默认类加载器
  env.serializer.setDefaultClassLoader(defaultSessionState.replClassLoader)
  // SPARK-21928: SerializerManager 内部的 Kryo 实例可能在 Netty 线程中用于
  // 获取远程缓存的 RDD 块，需要确保使用正确的类加载器
  env.serializerManager.setDefaultClassLoader(defaultSessionState.replClassLoader)

  // 直接返回结果的最大大小限制
  // 如果任务结果超过此值，将使用 BlockManager 发送结果
  // 保证小于数组字节限制（2GB）
  private val maxDirectResultSize = Math.min(
    conf.get(TASK_MAX_DIRECT_RESULT_SIZE),
    RpcUtils.maxMessageSizeBytes(conf))

  // 任务结果的最大大小限制（超过此值将丢弃结果）
  private val maxResultSize = conf.get(MAX_RESULT_SIZE)

  // 维护正在运行的任务列表（任务ID -> TaskRunner）
  private[executor] val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Kill 标记的 TTL（毫秒）- 10 秒
  private val KILL_MARK_TTL_MS = 10000L

  // Kill 标记映射，包含中断线程标志、终止原因和时间戳
  // 用于处理 killTask() 在 launchTask() 之前被调用的情况
  private[executor] val killMarks = new ConcurrentHashMap[Long, (Boolean, String, Long)]

  // 定期清理过期 Kill 标记的任务
  private val killMarkCleanupTask = new Runnable {
    override def run(): Unit = {
      val oldest = System.currentTimeMillis() - KILL_MARK_TTL_MS
      val iter = killMarks.entrySet().iterator()
      while (iter.hasNext) {
        if (iter.next().getValue._3 < oldest) {
          iter.remove()
        }
      }
    }
  }

  // Kill 标记清理线程调度器
  private val killMarkCleanupService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("executor-kill-mark-cleanup")

  // 启动定期清理任务
  killMarkCleanupService.scheduleAtFixedRate(
    killMarkCleanupTask, KILL_MARK_TTL_MS, KILL_MARK_TTL_MS, TimeUnit.MILLISECONDS)

  /**
   * Executor 向 Driver 发送心跳失败的最大次数，超过此次数将自杀。
   * 默认值为 60。例如，如果最大失败次数为 60，心跳间隔为 10 秒，
   * 则最多尝试发送心跳 600 秒（10 分钟）。
   */
  private val HEARTBEAT_MAX_FAILURES = conf.get(EXECUTOR_HEARTBEAT_MAX_FAILURES)

  /**
   * 是否在心跳消息中丢弃值为零的累加器更新。
   * 包含空累加器（满足 isZero）可能使心跳消息非常大。
   */
  private val HEARTBEAT_DROP_ZEROES = conf.get(EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES)

  /**
   * 心跳发送间隔（毫秒）
   */
  private val HEARTBEAT_INTERVAL_MS = conf.get(EXECUTOR_HEARTBEAT_INTERVAL)

  /**
   * Executor 指标轮询间隔（毫秒）
   */
  private val METRICS_POLLING_INTERVAL_MS = conf.get(EXECUTOR_METRICS_POLLING_INTERVAL)

  // 如果指标轮询间隔大于 0，则使用独立轮询；否则在心跳时轮询
  private val pollOnHeartbeat = if (METRICS_POLLING_INTERVAL_MS > 0) false else true

  // 内存指标轮询器，可用于测试
  private[executor] val metricsPoller = new ExecutorMetricsPoller(
    env.memoryManager,
    METRICS_POLLING_INTERVAL_MS,
    executorMetricsSource)

  // 心跳发送器，定期向 Driver 汇报状态
  private val heartbeater = new Heartbeater(
    () => Executor.this.reportHeartBeat(),
    "executor-heartbeater",
    HEARTBEAT_INTERVAL_MS)

  // 必须在调用 heartbeater.start() 之前初始化
  // Driver 端心跳接收器的 RPC 引用
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  /**
   * 心跳失败计数器。仅在心跳线程中访问。每次成功心跳后重置为 0。
   */
  private var heartbeatFailures = 0

  /**
   * 标记 Executor 是否正在退役。退役期间阻止启动新任务。
   * 访问此变量可能存在竞争条件，但退役只是尽力而为，不是硬性停止。
   */
  private var decommissioned = false

  // 启动心跳线程
  heartbeater.start()

  // 应用启动时间
  private val appStartTime = conf.getLong("spark.app.startTime", 0)

  // 为了让用户能够分发插件及其所需的文件（通过 --jars、--files 和 --archives 提交），
  // 这些 JAR/文件/归档应该被下载并通过 updateDependencies 添加到类加载器。
  // 这应该在下面的插件初始化之前完成，因为 Executor 从类加载器搜索并初始化插件。
  private val Seq(initialUserJars, initialUserFiles, initialUserArchives) =
    Seq("jar", "file", "archive").map { key =>
      conf.getOption(s"spark.app.initial.$key.urls").map { urls =>
        import org.apache.spark.util.ArrayImplicits._
        immutable.Map(urls.split(",").map(url => (url, appStartTime)).toImmutableArraySeq: _*)
      }.getOrElse(immutable.Map.empty)
    }
  // 下载并更新初始依赖到默认会话状态
  updateDependencies(initialUserFiles, initialUserJars, initialUserArchives, defaultSessionState)

  // 插件和 ShuffleManager 需要使用包含 Executor 用户类路径的类加载器加载
  // 插件还需要在心跳启动后初始化，避免阻塞心跳发送（参见 SPARK-32175 和 SPARK-45762）
  private val plugins: Option[PluginContainer] =
    Utils.withContextClassLoader(defaultSessionState.replClassLoader) {
      PluginContainer(env, resources.asJava)
    }

  // 跳过本地模式，因为 ShuffleManager 已经初始化
  if (!isLocal) {
    Utils.withContextClassLoader(defaultSessionState.replClassLoader) {
      env.initializeShuffleManager()
    }
  }

  // 启动指标轮询器
  metricsPoller.start()

  // 返回正在运行的任务数量
  private[executor] def numRunningTasks: Int = runningTasks.size()

  /**
   * 标记 Executor 进入退役状态，阻止启动新任务。
   * 退役是 Spark 优雅关闭 Executor 的机制，允许完成正在运行的任务并迁移数据。
   */
  private[spark] def decommission(): Unit = {
    decommissioned = true
  }

  // 创建 TaskRunner 实例（工厂方法，便于测试）
  private[executor] def createTaskRunner(context: ExecutorBackend,
    taskDescription: TaskDescription) = new TaskRunner(context, taskDescription, plugins)

  /**
   * 启动任务执行。
   * 创建 TaskRunner 并提交到线程池执行。
   * 如果任务在启动前已被标记为 kill，则立即终止任务。
   */
  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    val taskId = taskDescription.taskId
    var taskRunnerOpt: Option[TaskRunner] = None
    try {
      val tr = createTaskRunner(context, taskDescription)
      taskRunnerOpt = Some(tr)
      runningTasks.put(taskId, tr)
      val killMark = killMarks.get(taskId)
      if (killMark != null) {
        tr.kill(killMark._1, killMark._2)
        killMarks.remove(taskId)
      }
      threadPool.execute(tr)
    } catch {
      case t: Throwable =>
        // Clean up if task was added to runningTasks before the failure.
        // If TaskRunner construction failed, taskRunnerOpt will be None and nothing to clean up.
        taskRunnerOpt.foreach { tr =>
          runningTasks.remove(tr.taskId)
        }
        try {
          logError(log"Executor launch task ${MDC(TASK_NAME, taskDescription.name)} failed," +
            log" reason: ${MDC(REASON, t.getMessage)}")
          context.statusUpdate(
            taskDescription.taskId,
            TaskState.FAILED,
            env.closureSerializer.newInstance().serialize(new ExceptionFailure(t, Seq.empty)))
        } catch {
          case NonFatal(e) if env.isStopped =>
            logError(
              log"Executor update launching task " +
                log"${MDC(TASK_NAME, taskDescription.name)} " +
                log"failed status failed, reason: ${MDC(REASON, t.getMessage)}" +
                log", spark env is stopped"
            )
          // No need to exit the executor as the executor is already stopped.
          // Leave it live to clean up the rest tasks and log info (similar to SPARK-19147).
          case t: Throwable =>
            logError(
              log"Executor update launching task " +
                log"${MDC(TASK_NAME, taskDescription.name)} " +
                log"failed status failed, reason: ${MDC(REASON, t.getMessage)}" +
                log", shutting down the executor"
            )
            System.exit(-1)
        }
    }
    if (decommissioned) {
      log.error(s"Launching a task while in decommissioned state.")
    }
  }

  /**
   * 终止指定任务。
   * 通过设置 kill 标记并可选地中断线程来终止任务。
   * 如果启用了 TaskReaper，会创建一个监督线程来监控任务终止过程。
   */
  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    killMarks.put(taskId, (interruptThread, reason, System.currentTimeMillis()))
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
      // Safe to remove kill mark as we got a chance with the TaskRunner.
      killMarks.remove(taskId)
    }
  }

  /**
   * 终止 Executor 上所有正在运行的任务。
   * 此方法由 Executor 后端调用，用于在不关闭 JVM 的情况下终止所有任务。
   * @param interruptThread 是否中断任务线程
   * @param reason 终止原因
   */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  /**
   * 停止 Executor。
   * 执行完整的清理流程，包括停止指标轮询器、心跳发送器、线程池和插件等。
   */
  def stop(): Unit = {
    if (!executorShutdown.getAndSet(true)) {
      ShutdownHookManager.removeShutdownHook(stopHookReference)
      env.metricsSystem.report()
      try {
        if (metricsPoller != null) {
          metricsPoller.stop()
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Unable to stop executor metrics poller", e)
      }
      try {
        if (heartbeater != null) {
          heartbeater.stop()
        }
      } catch {
        case NonFatal(e) =>
          logWarning("Unable to stop heartbeater", e)
      }
      ShuffleBlockPusher.stop()
      if (threadPool != null) {
        threadPool.shutdown()
      }
      if (killMarkCleanupService != null) {
        killMarkCleanupService.shutdown()
      }
      if (defaultSessionState != null && plugins != null) {
        // Notify plugins that executor is shutting down so they can terminate cleanly
        Utils.withContextClassLoader(defaultSessionState.replClassLoader) {
          plugins.foreach(_.shutdown())
        }
      }
      if (!isLocal) {
        env.stop()
      }
    }
  }

  /** 计算此 JVM 进程在垃圾回收上花费的总时间（毫秒） */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  /**
   * TaskRunner - 任务执行的核心实现类。
   * 
   * 每个 Task 由一个 TaskRunner 在独立线程中执行，负责：
   * - 反序列化 Task 对象
   * - 设置执行环境（类加载器、依赖等）
   * - 运行 Task 并收集结果
   * - 汇报执行状态和指标
   */
  class TaskRunner(
      execBackend: ExecutorBackend,
      val taskDescription: TaskDescription,
      private val plugins: Option[PluginContainer])
    extends Runnable {

    // 任务 ID
    val taskId = taskDescription.taskId
    // 任务名称（用于日志和监控）
    val taskName = taskDescription.name
    // 执行线程名称
    val threadName = s"$TASK_THREAD_NAME_PREFIX for $taskName"
    // MDC（Mapped Diagnostic Context）属性，用于结构化日志
    val mdcProperties = taskDescription.properties.asScala
      .filter(_._1.startsWith("mdc.")).toSeq

    /** 如果任务被终止，此字段包含终止原因 */
    @volatile private var reasonIfKilled: Option[String] = None

    // 执行此任务的线程 ID
    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** 任务是否已完成 */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** 任务开始运行时 JVM 进程已花费的 GC 时间 */
    @volatile var startGCTime: Long = _

    /**
     * 待运行的任务对象。在 run() 中通过反序列化 Driver 发来的任务二进制数据设置。
     * 一旦设置，就不会再改变。
     */
    @volatile var task: Task[Any] = _

    /**
     * 终止任务。设置 kill 原因并调用 Task 的 kill 方法。
     */
    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(log"Executor is trying to kill ${MDC(TASK_NAME, taskName)}, " +
        log"interruptThread: ${MDC(INTERRUPT_THREAD, interruptThread)}, " +
        log"reason: ${MDC(REASON, reason)}")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    /**
     * 设置任务完成标志并清除当前线程的中断状态。
     * SPARK-14234: 重置线程中断状态以避免 execBackend.statusUpdate 时的
     * ClosedByInterruptException，该异常会导致 Executor 崩溃。
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // 重置线程中断状态
      Thread.interrupted()
      // 通知所有等待的 TaskReaper。通常每个任务只有一个 reaper，
      // 但在极少数情况下，如果 cancel(interrupt=False) 后跟着 cancel(interrupt=True)，
      // 一个任务可能有两个 reaper。因此使用 notifyAll() 避免丢失唤醒。
      notifyAll()
    }

    /**
     * 工具方法，用于任务失败时：
     *   1. 报告 Executor 运行时间和 JVM GC 时间
     *   2. 收集累加器更新
     *   3. 设置完成标志并清除当前线程的中断状态
     */
    private def collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs: Long) = {
      // 报告 Executor 运行时间和 JVM GC 时间
      Option(task).foreach(t => {
        t.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          // SPARK-32898: 任务可能在 taskStartTimeNs 仍为初始值(=0)时被终止，
          // 此时 executorRunTime 应为 0
          if (taskStartTimeNs > 0) (System.nanoTime() - taskStartTimeNs) * taskDescription.cpus
          else 0))
        t.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
      })

      // 收集最新的累加器值以汇报给 Driver
      val accums: Seq[AccumulatorV2[_, _]] =
        Option(task).map(_.collectAccumulatorUpdates(taskFailed = true)).getOrElse(Seq.empty)
      val accUpdates = accums.map(acc => acc.toInfoUpdate)

      setTaskFinishedAndClearInterruptStatus()
      (accums, accUpdates)
    }

    /**
     * 为给定的作业 Artifact 状态获取 IsolatedSessionState。
     * 从缓存获取或创建会话，然后获取会话引用。如果会话在 get() 和 acquire()
     * 之间被淘汰，需要重试缓存查找。这可能发生在缓存已满且另一个任务触发淘汰时。
     */
    private def obtainSession(jobArtifactState: JobArtifactState): IsolatedSessionState = {
      var session: IsolatedSessionState = null
      var acquired = false
      while (!acquired) {
        // 获取或创建会话。缓存加载器使用 sessions 映射作为权威存储。
        // 确保同一 UUID 同时只有一个 IsolatedSessionState 实例。
        session = isolatedSessionCache.get(jobArtifactState.uuid, () => {
          // 首先检查权威会话映射。tryUnEvict() 会在清理进行中时阻塞，
          // 所以当它返回 false 时，会话已经从映射中移除，可以安全创建新实例。
          val existingSession = IsolatedSessionState.sessions.get(jobArtifactState.uuid)
          if (existingSession != null && existingSession.tryUnEvict()) {
            existingSession
          } else {
            newSessionState(jobArtifactState)
          }
        })
        // acquire() 在会话于 get() 和此处之间被淘汰时可能返回 false。
        // 此时重试 - 会话已经从缓存中移除。
        acquired = session.acquire()
      }
      session
    }

    /**
     * TaskRunner 的主执行方法。
     * 负责完整的任务执行流程：环境设置、任务反序列化、执行、结果收集和状态汇报。
     */
    override def run(): Unit = {

      // 类加载器隔离：根据任务的 Artifact 状态获取对应的会话
      val isolatedSession = taskDescription.artifacts.state match {
        case Some(jobArtifactState) =>
          obtainSession(jobArtifactState)
        case _ =>
          // 默认会话不在缓存中且永不淘汰，无需 acquire/release
          defaultSessionState
      }

      // 设置 MDC 日志上下文
      setMDCForTask(taskName, mdcProperties)
      threadId = Thread.currentThread.getId
      Thread.currentThread.setName(threadName)
      val threadMXBean = ManagementFactory.getThreadMXBean
      // 创建任务内存管理器
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTimeNs = System.nanoTime()
      val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
        threadMXBean.getCurrentThreadCpuTime
      } else 0L
      // 设置线程上下文类加载器
      Thread.currentThread.setContextClassLoader(isolatedSession.replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(log"Running ${MDC(TASK_NAME, taskName)}")
      // 向后端汇报任务正在运行
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStartTimeNs: Long = 0
      var taskStartCpu: Long = 0
      startGCTime = computeTotalGcTime()
      var taskStarted: Boolean = false

      try {
        // 必须在调用 updateDependencies() 之前设置，因为获取依赖可能需要访问其中的属性
        // （例如访问控制相关的属性）
        Executor.taskDeserializationProps.set(taskDescription.properties)

        // 下载并更新任务所需的依赖（文件、JAR、归档）
        updateDependencies(
          taskDescription.artifacts.files,
          taskDescription.artifacts.jars,
          taskDescription.artifacts.archives,
          isolatedSession)
        // 始终重置线程类加载器，确保所有线程（不仅是更新依赖的线程）都能使用新的类加载器
        Thread.currentThread.setContextClassLoader(isolatedSession.replClassLoader)
        // 反序列化任务对象
        task = ser.deserialize[Task[Any]](
          taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
        task.localProperties = taskDescription.properties
        task.setTaskMemoryManager(taskMemoryManager)

        // 如果任务在反序列化前已被终止，直接退出。
        // 抛出异常而非 return，因为在 try{} 块内 return 会导致 NonLocalReturnControl 异常，
        // 该异常会被 catch 块捕获，导致不正确的 ExceptionFailure 报告。
        val killReason = reasonIfKilled
        if (killReason.isDefined) {
          throw new TaskKilledException(killReason.get)
        }

        // 更新 epoch 的目的是在发生 FetchFailure 时使 Executor 的 MapOutput 状态缓存失效。
        // 在本地模式下 `env.mapOutputTracker` 是 MapOutputTrackerMaster，
        // 其缓存失效不基于 epoch，所以不需要特殊调用。
        if (!isLocal) {
          logDebug(s"$taskName's epoch is ${task.epoch}")
          env.mapOutputTracker.asInstanceOf[MapOutputTrackerWorker].updateEpoch(task.epoch)
        }

        // 通知指标轮询器任务开始
        metricsPoller.onTaskStart(taskId, task.stageId, task.stageAttemptId)
        taskStarted = true

        // 运行实际任务并测量运行时间
        taskStartTimeNs = System.nanoTime()
        taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L
        var threwException = true
        // 将资源数量信息转换为 ResourceInformation 对象
        val resources = taskDescription.resources.map { case (rName, addressesAmounts) =>
          rName -> new ResourceInformation(rName, addressesAmounts.keys.toSeq.sorted.toArray)
        }
        // 执行任务，使用 tryWithSafeFinally 确保清理逻辑执行
        val value = Utils.tryWithSafeFinally {
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = taskDescription.attemptNumber,
            metricsSystem = env.metricsSystem,
            cpus = taskDescription.cpus,
            resources = resources,
            plugins = plugins)
          threwException = false
          res
        } {
          // 释放任务持有的所有 Block 锁
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          // 清理任务分配的所有内存
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

          // 检测内存泄漏：如果释放了非零内存且没有抛异常，说明有内存泄漏
          // 检测内存泄漏：如果释放了非零内存且没有抛异常，说明有内存泄漏
          if (freedMemory > 0 && !threwException) {
            val errMsg = log"Managed memory leak detected; size = " +
              log"${MDC(NUM_BYTES, freedMemory)} bytes, ${MDC(TASK_NAME, taskName)}"
            // 根据配置决定是抛出异常还是仅记录警告
            if (conf.get(UNSAFE_EXCEPTION_ON_MEMORY_LEAK)) {
              throw SparkException.internalError(errMsg.message, category = "EXECUTOR")
            } else {
              logWarning(errMsg)
            }
          }

          // 检测 Block 锁泄漏：如果有未释放的锁且没有抛异常，说明有锁泄漏
          if (releasedLocks.nonEmpty && !threwException) {
            val errMsg =
              log"${MDC(NUM_RELEASED_LOCKS, releasedLocks.size)} block locks" +
                log" were not released by ${MDC(TASK_NAME, taskName)}\n" +
                log" ${MDC(RELEASED_LOCKS, releasedLocks.mkString("[", ", ", "]"))})"
            if (conf.get(STORAGE_EXCEPTION_PIN_LEAK)) {
              throw SparkException.internalError(errMsg.message, category = "EXECUTOR")
            } else {
              logInfo(errMsg)
            }
          }
        }
        // 检查是否有被用户代码捕获但未重新抛出的 FetchFailure
        // 这种情况下用户代码可能错误地吞掉了 Spark 内部异常
        task.context.fetchFailed.foreach { fetchFailure =>
          logError(log"${MDC(TASK_NAME, taskName)} completed successfully though internally " +
            log"it encountered unrecoverable fetch failures! Most likely this means user code " +
            log"is incorrectly swallowing Spark's internal " +
            log"${MDC(CLASS_NAME, classOf[FetchFailedException])}", fetchFailure)
        }
        val taskFinishNs = System.nanoTime()
        val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime
        } else 0L

        // 如果任务被终止，让它失败
        task.context.killTaskIfInterrupted()

        // 序列化任务结果
        val resultSer = env.serializer.newInstance()
        val beforeSerializationNs = System.nanoTime()
        val valueByteBuffer = SerializerHelper.serializeToChunkedBuffer(resultSer, value)
        val afterSerializationNs = System.nanoTime()

        // 反序列化分两步进行：首先反序列化 Task 对象（包含 Partition），
        // 然后 Task.run() 反序列化 RDD 和要运行的函数。
        // 计算 Executor 反序列化时间
        // 计算 Executor 反序列化时间
        task.metrics.setExecutorDeserializeTime(TimeUnit.NANOSECONDS.toMillis(
          (taskStartTimeNs - deserializeStartTimeNs) + task.executorDeserializeTimeNs))
        task.metrics.setExecutorDeserializeCpuTime(
          (taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
        // 需要减去 Task.run() 的反序列化时间，避免重复计算
        task.metrics.setExecutorRunTime(TimeUnit.NANOSECONDS.toMillis(
          (taskFinishNs - taskStartTimeNs) * taskDescription.cpus
            - task.executorDeserializeTimeNs))
        task.metrics.setExecutorCpuTime(
          (taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
        task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
        task.metrics.setResultSerializationTime(TimeUnit.NANOSECONDS.toMillis(
          afterSerializationNs - beforeSerializationNs))
        task.metrics.setPeakOnHeapExecutionMemory(taskMemoryManager.getPeakOnHeapExecutionMemory)
        task.metrics.setPeakOffHeapExecutionMemory(taskMemoryManager.getPeakOffHeapExecutionMemory)
        // 使用 Dropwizard metrics 系统暴露任务指标
        // 更新任务指标计数器
        executorSource.METRIC_CPU_TIME.inc(task.metrics.executorCpuTime)
        executorSource.METRIC_RUN_TIME.inc(task.metrics.executorRunTime)
        executorSource.METRIC_JVM_GC_TIME.inc(task.metrics.jvmGCTime)
        executorSource.METRIC_DESERIALIZE_TIME.inc(task.metrics.executorDeserializeTime)
        executorSource.METRIC_DESERIALIZE_CPU_TIME.inc(task.metrics.executorDeserializeCpuTime)
        executorSource.METRIC_RESULT_SERIALIZE_TIME.inc(task.metrics.resultSerializationTime)
        executorSource.METRIC_INPUT_BYTES_READ
          .inc(task.metrics.inputMetrics.bytesRead)
        executorSource.METRIC_INPUT_RECORDS_READ
          .inc(task.metrics.inputMetrics.recordsRead)
        executorSource.METRIC_OUTPUT_BYTES_WRITTEN
          .inc(task.metrics.outputMetrics.bytesWritten)
        executorSource.METRIC_OUTPUT_RECORDS_WRITTEN
          .inc(task.metrics.outputMetrics.recordsWritten)
        executorSource.METRIC_DISK_BYTES_SPILLED.inc(task.metrics.diskBytesSpilled)
        executorSource.METRIC_MEMORY_BYTES_SPILLED.inc(task.metrics.memoryBytesSpilled)
        incrementShuffleMetrics(executorSource, task.metrics)

        // 注意：累加器更新必须在 TaskMetrics 更新之后收集
        val accumUpdates = task.collectAccumulatorUpdates()
        val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId)
        // TODO: 避免两次序列化 value
        val directResult = new DirectTaskResult(valueByteBuffer, accumUpdates, metricPeaks)
        // 尝试估计 DirectTaskResult 序列化的合理上限
        val serializedDirectResult = SerializerHelper.serializeToChunkedBuffer(ser, directResult,
          valueByteBuffer.size + accumUpdates.size * 32 + metricPeaks.length * 8)
        val resultSize = serializedDirectResult.size
        executorSource.METRIC_RESULT_SIZE.inc(resultSize)

        // directSend = 直接发送回 Driver
        // 根据结果大小选择发送方式
        val serializedResult: ByteBuffer = {
          // 如果结果超过最大结果大小限制，丢弃结果
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(log"Finished ${MDC(TASK_NAME, taskName)}. " +
              log"Result is larger than maxResultSize " +
              log"(${MDC(RESULT_SIZE_BYTES, Utils.bytesToString(resultSize))} > " +
              log"${MDC(RESULT_SIZE_BYTES_MAX, Utils.bytesToString(maxResultSize))}), " +
              log"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            // 如果结果超过直接发送大小限制，通过 BlockManager 存储
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              serializedDirectResult,
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(log"Finished ${MDC(TASK_NAME, taskName)}." +
              log" ${MDC(NUM_BYTES, resultSize)} bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            // 结果足够小，直接发送给 Driver
            logInfo(log"Finished ${MDC(TASK_NAME, taskName)}." +
              log" ${MDC(NUM_BYTES, resultSize)} bytes result sent to driver")
            // toByteBuffer 在此处是安全的，受 maxDirectResultSize 保护
            serializedDirectResult.toByteBuffer
          }
        }

        // 更新成功任务计数
        executorSource.SUCCEEDED_TASKS.inc(1L)
        setTaskFinishedAndClearInterruptStatus()
        // 通知插件任务成功
        plugins.foreach(_.onTaskSucceeded())
        // 向后端汇报任务完成
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
      } catch {
        // 处理任务被主动终止的情况
        // 处理任务被主动终止的情况
        case t: TaskKilledException =>
          logInfo(log"Executor killed ${MDC(TASK_NAME, taskName)}," +
            log" reason: ${MDC(REASON, t.reason)}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          // 将任务指标峰值放入 immutable.ArraySeq，以 immutable.Seq 形式暴露而无需复制
          val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId).toImmutableArraySeq
          val reason = TaskKilled(t.reason, accUpdates, accums, metricPeaks)
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        // 处理任务被中断终止的情况（通过 reasonIfKilled 判断是否是主动终止）
        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(log"Executor interrupted and killed ${MDC(TASK_NAME, taskName)}," +
            log" reason: ${MDC(REASON, killReason)}")

          val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
          val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId).toImmutableArraySeq
          val reason = TaskKilled(killReason, accUpdates, accums, metricPeaks)
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        // 处理 FetchFailure（Shuffle 数据获取失败）
        case t: Throwable if hasFetchFailure && !Executor.isFatalError(t, killOnFatalErrorDepth) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // 任务中发生了 FetchFailure，但用户代码包装了该异常并抛出了其他异常。
            // 无论如何，我们将其作为 FetchFailure 处理并忽略其他异常。
            logWarning(log"${MDC(TASK_NAME, taskName)} encountered a " +
              log"${MDC(CLASS_NAME, classOf[FetchFailedException].getName)} " +
              log"and failed, but the " +
              log"${MDC(CLASS_NAME, classOf[FetchFailedException].getName)} " +
              log"was hidden by another exception. Spark is handling this like a fetch failure " +
              log"and ignoring the other exception: ${MDC(ERROR, t)}")
          }
          setTaskFinishedAndClearInterruptStatus()
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        // 处理提交被拒绝的情况（推测执行场景）
        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskCommitDeniedReason
          setTaskFinishedAndClearInterruptStatus()
          plugins.foreach(_.onTaskFailed(reason))
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(reason))

        // 处理 SparkEnv 已停止时的异常（SPARK-19147）
        case t: Throwable if env.isStopped =>
          // 记录预期异常但不打印堆栈跟踪
          logError(log"Exception in ${MDC(TASK_NAME, taskName)}: ${MDC(ERROR, t.getMessage)}")

        // 处理其他所有异常
        case t: Throwable =>
          // 尝试通过通知 Driver 来干净地退出。
          // 如果出现问题（或这是致命异常），将委托给默认未捕获异常处理器，
          // 该处理器将终止 Executor。
          logError(log"Exception in ${MDC(TASK_NAME, taskName)}", t)

          // SPARK-20904: 如果异常发生在关闭期间，不要向 Driver 报告失败。
          // 因为库可能设置了与运行中任务竞争的关闭钩子，可能出现虚假失败，
          // 导致 Driver 中不正确的计数（例如，如果关闭是由于抢占而非应用问题，
          // 任务失败不应被忽略）。
          if (!ShutdownHookManager.inShutdown()) {
            val (accums, accUpdates) = collectAccumulatorsAndResetStatusOnFailure(taskStartTimeNs)
            val metricPeaks = metricsPoller.getTaskMetricPeaks(taskId).toImmutableArraySeq

            val (taskFailureReason, serializedTaskFailureReason) = {
              try {
                val ef = new ExceptionFailure(t, accUpdates).withAccums(accums)
                  .withMetricPeaks(metricPeaks)
                (ef, ser.serialize(ef))
              } catch {
                case _: NotSerializableException =>
                  // t is not serializable so just send the stacktrace
                  val ef = new ExceptionFailure(t, accUpdates, false).withAccums(accums)
                    .withMetricPeaks(metricPeaks)
                  (ef, ser.serialize(ef))
              }
            }
            setTaskFinishedAndClearInterruptStatus()
            plugins.foreach(_.onTaskFailed(taskFailureReason))
            execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskFailureReason)
          } else {
            logInfo("Not reporting error to driver during JVM shutdown.")
          }

          // 除非异常本身是致命的，否则不要强制退出，避免不必要地停止其他任务。
          if (Executor.isFatalError(t, killOnFatalErrorDepth)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        // 清理任务的 MDC 日志上下文
        cleanMDCForTask(taskName, mdcProperties)
        // 从运行中任务列表移除
        runningTasks.remove(taskId)
        // 如果任务已成功启动（已反序列化，stageId 和 stageAttemptId 已知，
        // 且调用了 metricsPoller.onTaskStart），通知指标轮询器任务完成
        if (taskStarted) {
          metricsPoller.onTaskCompletion(taskId, task.stageId, task.stageAttemptId)
        }
        // 释放会话引用。如果已淘汰且这是最后一个任务，则执行清理。
        // 跳过 defaultSessionState，因为它永不淘汰。
        if (isolatedSession ne defaultSessionState) {
          isolatedSession.release()
        }
        // 重置线程名称为空闲状态
        Thread.currentThread().setName(s"$IDLE_TASK_THREAD_NAME#$threadId" )
      }
    }

    /**
     * 递增 Shuffle 相关指标。
     * 将任务的 Shuffle 读写指标累加到 ExecutorSource 中。
     */
    private def incrementShuffleMetrics(
      executorSource: ExecutorSource,
      metrics: TaskMetrics
    ): Unit = {
      executorSource.METRIC_SHUFFLE_FETCH_WAIT_TIME
        .inc(metrics.shuffleReadMetrics.fetchWaitTime)
      executorSource.METRIC_SHUFFLE_WRITE_TIME.inc(metrics.shuffleWriteMetrics.writeTime)
      executorSource.METRIC_SHUFFLE_TOTAL_BYTES_READ
        .inc(metrics.shuffleReadMetrics.totalBytesRead)
      executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ
        .inc(metrics.shuffleReadMetrics.remoteBytesRead)
      executorSource.METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK
        .inc(metrics.shuffleReadMetrics.remoteBytesReadToDisk)
      executorSource.METRIC_SHUFFLE_LOCAL_BYTES_READ
        .inc(metrics.shuffleReadMetrics.localBytesRead)
      executorSource.METRIC_SHUFFLE_RECORDS_READ
        .inc(metrics.shuffleReadMetrics.recordsRead)
      executorSource.METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.remoteBlocksFetched)
      executorSource.METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.localBlocksFetched)
      executorSource.METRIC_SHUFFLE_REMOTE_REQS_DURATION
        .inc(metrics.shuffleReadMetrics.remoteReqsDuration)
      executorSource.METRIC_SHUFFLE_BYTES_WRITTEN
        .inc(metrics.shuffleWriteMetrics.bytesWritten)
      executorSource.METRIC_SHUFFLE_RECORDS_WRITTEN
        .inc(metrics.shuffleWriteMetrics.recordsWritten)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_CORRUPT_MERGED_BLOCK_CHUNKS
        .inc(metrics.shuffleReadMetrics.corruptMergedBlockChunks)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_FETCH_FALLBACK_COUNT
        .inc(metrics.shuffleReadMetrics.mergedFetchFallbackCount)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.remoteMergedBlocksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BLOCKS_FETCHED
        .inc(metrics.shuffleReadMetrics.localMergedBlocksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_CHUNKS_FETCHED
        .inc(metrics.shuffleReadMetrics.remoteMergedChunksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_CHUNKS_FETCHED
        .inc(metrics.shuffleReadMetrics.localMergedChunksFetched)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BYTES_READ
        .inc(metrics.shuffleReadMetrics.remoteMergedBytesRead)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BYTES_READ
        .inc(metrics.shuffleReadMetrics.localMergedBytesRead)
      executorSource.METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_REQS_DURATION
        .inc(metrics.shuffleReadMetrics.remoteMergedReqsDuration)
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }

    private[executor] def theadDump(): Option[ThreadStackTrace] = {
      Utils.getThreadDumpForThread(getThreadId)
    }
  }

  /**
   * 设置任务的 MDC（Mapped Diagnostic Context）日志上下文。
   */
  private def setMDCForTask(taskName: String, mdc: Seq[(String, String)]): Unit = {
    if (Executor.mdcIsSupported) {
      mdc.foreach { case (key, value) => SLF4JMDC.put(key, value) }
      // 避免用户覆盖 taskName
      SLF4JMDC.put(taskNameMDCKey, taskName)
    }
  }

  /**
   * 清理任务的 MDC 日志上下文。
   */
  private def cleanMDCForTask(taskName: String, mdc: Seq[(String, String)]): Unit = {
    if (Executor.mdcIsSupported) {
      mdc.foreach { case (key, _) => SLF4JMDC.remove(key) }
      SLF4JMDC.remove(taskNameMDCKey)
    }
  }

  /**
   * TaskReaper - 任务终止监督器。
   * 
   * 通过发送中断标志、可选地发送 Thread.interrupt() 并监控任务直到完成，
   * 来监督任务的终止/取消过程。
   *
   * Spark 当前的任务取消/终止机制是"尽力而为"，因为某些任务可能不可中断或不响应
   * "killed"标志。如果集群中大部分任务槽被标记为已终止但仍在运行的"僵尸任务"占用，
   * 可能导致新作业和任务无法获得资源。
   *
   * TaskReaper 在 SPARK-18761 中引入，用于监控和清理僵尸任务。
   * 为了向后兼容/可移植性，此组件默认禁用，需要通过设置
   * `spark.task.reaper.enabled=true` 显式启用。
   *
   * 当任务被终止/取消时，会为该任务创建一个 TaskReaper。
   * 通常一个任务只有一个 TaskReaper，但如果 kill 被调用两次且 `interrupt` 参数不同，
   * 一个任务可能有最多两个 reaper。
   *
   * 一旦创建，TaskReaper 将运行直到其监督的任务完成运行。
   * 如果 TaskReaper 未配置为在超时后终止 JVM（即 `spark.task.reaper.killTimeout < 0`），
   * 则如果被监督的任务永不退出，TaskReaper 可能无限期运行。
   */
  private class TaskReaper(
      taskRunner: TaskRunner,
      val interruptThread: Boolean,
      val reason: String)
    extends Runnable {

    // 被监督的任务 ID
    private[this] val taskId: Long = taskRunner.taskId

    // 轮询检查任务状态的间隔（毫秒）
    private[this] val killPollingIntervalMs: Long = conf.get(TASK_REAPER_POLLING_INTERVAL)

    // 终止超时时间（纳秒），超时后可能强制终止 JVM
    private[this] val killTimeoutNs: Long = {
      TimeUnit.MILLISECONDS.toNanos(conf.get(TASK_REAPER_KILL_TIMEOUT))
    }

    // 是否在超时时打印线程转储
    private[this] val takeThreadDump: Boolean = conf.get(TASK_REAPER_THREAD_DUMP)

    /**
     * TaskReaper 的主执行方法。
     * 发送终止信号后，持续监控任务直到完成或超时。
     */
    override def run(): Unit = {
      setMDCForTask(taskRunner.taskName, taskRunner.mdcProperties)
      val startTimeNs = System.nanoTime()
      def elapsedTimeNs = System.nanoTime() - startTimeNs
      def timeoutExceeded(): Boolean = killTimeoutNs > 0 && elapsedTimeNs > killTimeoutNs
      try {
        // 只尝试终止任务一次。如果 interruptThread = false，第二次终止尝试将无效；
        // 如果 interruptThread = true，多次中断可能不安全或无效。
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // 监控被终止的任务直到它退出。这里的同步逻辑较复杂，因为我们不想在
        // 可能打印线程转储时同步 taskRunner，但也需要避免在检查任务是否完成
        // 和 wait() 之间的竞争条件。
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // 需要在检查任务是否完成时同步 TaskRunner，以避免任务在我们检查后
            // 立即标记为完成然后我们调用 wait() 时的竞争条件。
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            val elapsedTimeMs = TimeUnit.NANOSECONDS.toMillis(elapsedTimeNs)
            logWarning(log"Killed task ${MDC(TASK_ID, taskId)} " +
              log"is still running after ${MDC(TIME_UNITS, elapsedTimeMs)} ms")
            if (takeThreadDump) {
              try {
                taskRunner.theadDump().foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(log"Thread dump from task ${MDC(TASK_ID, taskId)}:\n" +
                      log"${MDC(THREAD, thread.toString)}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        // 如果任务未完成且超时，处理超时情况
        if (!taskRunner.isFinished && timeoutExceeded()) {
          val killTimeoutMs = TimeUnit.NANOSECONDS.toMillis(killTimeoutNs)
          if (isLocal) {
            // 本地模式下不终止 JVM
            logError(log"Killed task ${MDC(TASK_ID, taskId)} could not be stopped within " +
              log"${MDC(TIMEOUT, killTimeoutMs)} ms; " +
              log"not killing JVM because we are running in local mode.")
          } else {
            // 非本地模式下，这里抛出的异常将冒泡到未捕获异常处理器并导致 Executor JVM 退出
            throw new KilledByTaskReaperException(s"Killing executor JVM because killed task " +
              s"$taskId could not be stopped within $killTimeoutMs ms.")
          }
        }
      } finally {
        cleanMDCForTask(taskRunner.taskName, taskRunner.mdcProperties)
        // 清理 taskReaperForTask 映射中的条目
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // 这是一个 interruptThread == false 的 TaskReaper，
              // 后续的 killTask() 调用使用 interruptThread == true 覆盖了映射条目。
            }
          }
        }
      }
    }
  }

  /**
   * 创建用于任务执行的类加载器，添加用户指定的 JAR 或解释器创建的类到搜索路径。
   */
  /**
   * 创建用于任务执行的类加载器，添加用户指定的 JAR 或解释器创建的类到搜索路径。
   */
  private def createClassLoader(
      currentJars: HashMap[String, Long],
      useStub: Boolean,
      isDefaultSession: Boolean): MutableURLClassLoader = {
    // 用用户类路径初始化 JAR 列表
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    // 为 jarSet 中的每个 JAR 添加到类加载器
    // 假设每个文件已经被获取
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    createClassLoader(urls, useStub, isDefaultSession)
  }

  /**
   * 创建类加载器，支持 UDF 存根（Spark Connect 场景）。
   */
  private def createClassLoader(urls: Array[URL],
                                useStub: Boolean,
                                isDefaultSession: Boolean): MutableURLClassLoader = {
    logInfo(
      log"Starting executor with user classpath" +
        log" (userClassPathFirst =" +
        log" ${MDC(LogKeys.EXECUTOR_USER_CLASS_PATH_FIRST, userClassPathFirst)}): " +
        log"${MDC(URLS, urls.mkString("'", ",", "'"))}"
    )

    if (useStub) {
      createClassLoaderWithStub(urls, conf.get(CONNECT_SCALA_UDF_STUB_PREFIXES), isDefaultSession)
    } else {
      createClassLoader(urls, isDefaultSession)
    }
  }

  /**
   * 创建基本类加载器。
   * 根据 userClassPathFirst 配置决定类加载顺序。
   */
  private def createClassLoader(urls: Array[URL],
                                isDefaultSession: Boolean): MutableURLClassLoader = {
    // SPARK-51537: 隔离会话必须*继承*默认会话的类加载器，
    // 默认会话已经包含了通过 --jars 指定的全局 JAR。
    // 对于 Spark 插件，不能简单地将插件 JAR 添加到隔离会话的类路径，
    // 因为这可能导致插件被重新加载，产生潜在冲突或意外行为。
    val loader = if (isDefaultSession) systemLoader else defaultSessionState.replClassLoader
    if (userClassPathFirst) {
      // 用户类优先加载
      new ChildFirstURLClassLoader(urls, loader)
    } else {
      // 父类优先加载（默认）
      new MutableURLClassLoader(urls, loader)
    }
  }

  /**
   * 创建带有 UDF 存根的类加载器（用于 Spark Connect 场景）。
   * 存根类加载器用于在客户端和服务端之间代理 UDF 类。
   */
  private def createClassLoaderWithStub(
      urls: Array[URL],
      binaryName: Seq[String],
      isDefaultSession: Boolean): MutableURLClassLoader = {
    // SPARK-51537: 隔离会话必须*继承*默认会话的类加载器
    val loader = if (isDefaultSession) systemLoader else defaultSessionState.replClassLoader
    if (userClassPathFirst) {
      // 用户优先: user -> (sys -> stub)
      val stubClassLoader =
        StubClassLoader(loader, binaryName)
      new ChildFirstURLClassLoader(urls, stubClassLoader)
    } else {
      // 系统优先: sys -> user -> stub
      val stubClassLoader =
        StubClassLoader(null, binaryName)
      new ChildFirstURLClassLoader(urls, stubClassLoader, loader)
    }
  }

  /**
   * 如果使用 REPL，添加另一个类加载器来读取用户在交互式会话中定义的新类。
   */
  private def addReplClassLoaderIfNeeded(
      parent: ClassLoader,
      sessionClassUri: Option[String],
      sessionUUID: String): ClassLoader = {
    val classUri = sessionClassUri.getOrElse(conf.get("spark.repl.class.uri", null))
    val classLoader = if (classUri != null) {
      logInfo(log"Using REPL class URI: ${MDC(LogKeys.URI, classUri)}")
      new ExecutorClassLoader(conf, env, classUri, parent, userClassPathFirst)
    } else {
      parent
    }
    logInfo(log"Created or updated repl class loader ${MDC(CLASS_LOADER, classLoader)}" +
      log" for ${MDC(SESSION_ID, sessionUUID)}.")
    classLoader
  }

  /**
   * 下载任何缺失的依赖（如果从 SparkContext 收到新的文件和 JAR）。
   * 同时将获取的新 JAR 添加到类加载器。
   * 此方法对测试可见。
   */
  private[executor] def updateDependencies(
      newFiles: immutable.Map[String, Long],
      newJars: immutable.Map[String, Long],
      newArchives: immutable.Map[String, Long],
      state: IsolatedSessionState,
      testStartLatch: Option[CountDownLatch] = None,
      testEndLatch: Option[CountDownLatch] = None): Unit = {
    var renewClassLoader = false;
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    // 使用可中断锁，允许等待中的任务被中断后快速退出
    updateDependenciesLock.lockInterruptibly()
    try {
      // 用于测试，模拟慢速文件下载
      testStartLatch.foreach(_.countDown())

      // 如果会话 ID 来自 SparkSession，说明是 Spark Connect 客户端
      // 为 Spark Connect 客户端指定专用目录
      lazy val root = if (!isDefaultState(state.sessionUUID)) {
        val newDest = new File(SparkFiles.getRootDirectory(), state.sessionUUID)
        newDest.mkdir()
        newDest
      } else {
        new File(SparkFiles.getRootDirectory())
      }

      // 获取缺失的依赖
      // 下载新文件
      // 下载新文件
      for ((name, timestamp) <- newFiles if state.currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo(log"Fetching ${MDC(FILE_NAME, name)} with" +
          log" timestamp ${MDC(TIMESTAMP, timestamp)}")
        // 使用缓存模式获取文件，本地模式关闭缓存
        Utils.fetchFile(name, root, conf, hadoopConf, timestamp, useCache = !isLocal)
        state.currentFiles(name) = timestamp
      }
      // 下载新的归档文件并解压
      for ((name, timestamp) <- newArchives if
          state.currentArchives.getOrElse(name, -1L) < timestamp) {
        logInfo(log"Fetching ${MDC(ARCHIVE_NAME, name)} with" +
          log" timestamp ${MDC(TIMESTAMP, timestamp)}")
        val sourceURI = new URI(name)
        val uriToDownload = Utils.getUriBuilder(sourceURI).fragment(null).build()
        val source = Utils.fetchFile(uriToDownload.toString, Utils.createTempDir(), conf,
          hadoopConf, timestamp, useCache = !isLocal, shouldUntar = false)
        val dest = new File(
          root,
          if (sourceURI.getFragment != null) sourceURI.getFragment else source.getName)
        logInfo(
          log"Unpacking an archive ${MDC(ARCHIVE_NAME, name)}" +
            log" (${MDC(BYTE_SIZE, source.length)} bytes)" +
            log" from ${MDC(SOURCE_PATH, source.getAbsolutePath)}" +
            log" to ${MDC(DESTINATION_PATH, dest.getAbsolutePath)}")
        Utils.deleteRecursively(dest)
        Utils.unpack(source, dest)
        state.currentArchives(name) = timestamp
      }
      // 下载新的 JAR 文件并添加到类加载器
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = state.currentJars.get(name)
          .orElse(state.currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo(log"Fetching ${MDC(JAR_URL, name)} with" +
            log" timestamp ${MDC(TIMESTAMP, timestamp)}")
          // 使用缓存模式获取 JAR，本地模式关闭缓存
          Utils.fetchFile(name, root, conf,
            hadoopConf, timestamp, useCache = !isLocal)
          state.currentJars(name) = timestamp
          // 将 JAR 添加到类加载器
          val url = new File(root, localName).toURI.toURL
          if (!state.urlClassLoader.getURLs().contains(url)) {
            logInfo(log"Adding ${MDC(LogKeys.URL, url)} to" +
              log" class loader ${MDC(UUID, state.sessionUUID)}")
            state.urlClassLoader.addURL(url)
            // 如果启用了 UDF 存根，需要重建类加载器
            if (isStubbingEnabledForState(state.sessionUUID)) {
              renewClassLoader = true
            }
          }
        }
      }
      // 如果需要，重建类加载器以确保所有类都已更新
      if (renewClassLoader) {
        state.urlClassLoader = createClassLoader(state.urlClassLoader.getURLs,
          useStub = true, isDefaultState(state.sessionUUID))
        state.replClassLoader =
          addReplClassLoaderIfNeeded(state.urlClassLoader, state.replClassDirUri, state.sessionUUID)
      }
      // For testing, so we can simulate a slow file download:
      testEndLatch.foreach(_.await())
    } finally {
      updateDependenciesLock.unlock()
    }
  }

  /**
   * 向 Driver 报告心跳和活动任务的指标。
   * 心跳包含累加器更新和 Executor 指标，用于 Driver 监控 Executor 状态。
   */
  private def reportHeartBeat(): Unit = {
    // 待发送的 (任务ID, 累加器更新) 列表
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    // 如果配置为在心跳时轮询指标，执行轮询
    if (pollOnHeartbeat) {
      metricsPoller.poll()
    }

    // 获取 Executor 级别的指标更新
    val executorUpdates = metricsPoller.getExecutorUpdates()

    // 收集所有运行中任务的指标
    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        val accumulatorsToReport = {
          if (HEARTBEAT_DROP_ZEROES) {
            taskRunner.task.metrics.accumulators().filterNot(_.isZero)
          } else {
            taskRunner.task.metrics.accumulators()
          }
        }.filterNot(_.excludeFromHeartbeat)
        accumUpdates += ((taskRunner.taskId, accumulatorsToReport))
      }
    }

    // 构建并发送心跳消息
    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId,
      executorUpdates)
    try {
      // 同步发送心跳并等待响应
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        message, new RpcTimeout(HEARTBEAT_INTERVAL_MS.millis, EXECUTOR_HEARTBEAT_INTERVAL.key))
      // 如果 Driver 要求重新注册 BlockManager（通常发生在 Driver 重启后）
      if (!executorShutdown.get && response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      // 心跳成功，重置失败计数
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        // 如果失败次数超过阈值，终止 Executor
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(log"Exit as unable to send heartbeats to driver " +
            log"more than ${MDC(MAX_ATTEMPTS, HEARTBEAT_MAX_FAILURES)} times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * 获取指定任务的线程转储。
   * 用于诊断和调试目的。
   */
  def getTaskThreadDump(taskId: Long): Option[ThreadStackTrace] = {
    val runner = runningTasks.get(taskId)
    if (runner != null) {
      runner.theadDump()
    } else {
      logWarning(log"Failed to dump thread for task ${MDC(TASK_ID, taskId)}")
      None
    }
  }
}

/**
 * Executor 伴生对象，包含常量和工具方法。
 */
private[spark] object Executor extends Logging {
  // 任务执行线程名称前缀
  val TASK_THREAD_NAME_PREFIX = "Executor task launch worker"
  // 空闲任务线程名称
  val IDLE_TASK_THREAD_NAME = "Executor task idle worker"

  // 保留给内部使用，用于在任务完全反序列化之前读取任务属性。
  // 如果可能，应该使用 TaskContext.getLocalProperty 调用替代。
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]

  // 仅用于存储本地模式的 executorSource
  var executorSourceLocalModeOnly: ExecutorSource = null

  // 检查 MDC（Mapped Diagnostic Context）是否受支持
  lazy val mdcIsSupported: Boolean = {
    try {
      // This tests if any class initialization error is thrown
      val testKey = System.nanoTime().toString
      SLF4JMDC.put(testKey, "testValue")
      SLF4JMDC.remove(testKey)

      true
    } catch {
      case t: Throwable =>
        logInfo("MDC is not supported.", t)
        false
    }
  }

  /**
   * 判断任务抛出的 Throwable 是否是致命错误。
   * 我们将根据此决定是否终止 Executor。
   *
   * @param depthToCheck 搜索致命错误的异常链最大深度。
   *                     0 表示不检查任何致命错误（即返回 false），
   *                     1 表示只检查异常本身不检查原因，以此类推。
   *                     这是为了在异常链中有循环时避免 StackOverflowError。
   */
  @scala.annotation.tailrec
  def isFatalError(t: Throwable, depthToCheck: Int): Boolean = {
    if (depthToCheck <= 0) {
      false
    } else {
      t match {
        // SparkOutOfMemoryError 不被视为致命错误（可以继续运行其他任务）
        case _: SparkOutOfMemoryError => false
        // 使用 Utils 中定义的致命错误判断
        case e if Utils.isFatalError(e) => true
        // 递归检查原因异常
        case e if e.getCause != null => isFatalError(e.getCause, depthToCheck - 1)
        case _ => false
      }
    }
  }
}

/**
 * 当任务被 TaskReaper 终止时抛出的异常。
 * 表示任务在指定超时时间内无法正常停止，需要强制终止 Executor JVM。
 */
class KilledByTaskReaperException(message: String) extends SparkException(message)

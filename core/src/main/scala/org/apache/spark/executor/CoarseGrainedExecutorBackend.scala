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

import java.net.URL
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import io.netty.util.internal.PlatformDependent

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.resource.ResourceProfile._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.rpc._
import org.apache.spark.scheduler.{ExecutorLossMessage, ExecutorLossReason, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, SignalUtils, ThreadUtils, Utils}

/**
 * 粗粒度 Executor 后端实现。
 *
 * 这是 Spark 在分布式模式下运行时 Executor 与 Driver 通信的核心组件。
 * "粗粒度"指的是资源分配粒度：整个 Executor 进程被分配给应用程序，
 * 而不是像"细粒度"模式那样按任务分配资源。
 *
 * == 主要职责 ==
 *  - 向 Driver 注册 Executor
 *  - 接收并执行 Driver 分发的任务
 *  - 向 Driver 汇报任务执行状态
 *  - 处理 Executor 退役（decommission）流程
 *
 * == 支持的部署模式 ==
 *  - YARN
 *  - Kubernetes
 *  - Standalone
 *
 * @param rpcEnv RPC 环境，用于与 Driver 通信
 * @param driverUrl Driver 的 RPC 地址
 * @param executorId Executor 唯一标识符
 * @param bindAddress 绑定的网络地址
 * @param hostname 主机名
 * @param cores 分配的 CPU 核心数
 * @param env SparkEnv 运行环境
 * @param resourcesFileOpt 资源配置文件路径（可选）
 * @param resourceProfile 资源配置文件
 */
private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    bindAddress: String,
    hostname: String,
    cores: Int,
    env: SparkEnv,
    resourcesFileOpt: Option[String],
    resourceProfile: ResourceProfile)
  extends IsolatedThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  import CoarseGrainedExecutorBackend._

  // 标记 Executor 是否正在停止
  private[spark] val stopping = new AtomicBoolean(false)
  // Executor 实例，在成功注册后创建
  var executor: Executor = null
  // Driver 的 RPC 端点引用
  @volatile var driver: Option[RpcEndpointRef] = None

  // 分配给此 Executor 的资源信息（如 GPU）
  private var _resources = Map.empty[String, ResourceInformation]

  // Executor 是否正在退役
  private var decommissioned = false

  // 追踪最后一个任务完成的时间（纳秒）
  // 如果没有任务运行且所有 Shuffle/RDD 数据迁移完成，退役中的 Executor 应该退出
  private val lastTaskFinishTime = new AtomicLong(System.nanoTime())

  /**
   * RPC 端点启动时的初始化逻辑。
   * 注册信号处理器、检查资源配置、向 Driver 注册。
   */
  override def onStart(): Unit = {
    // 如果启用了退役功能，注册信号处理器
    if (env.conf.get(DECOMMISSION_ENABLED)) {
      val signal = env.conf.get(EXECUTOR_DECOMMISSION_SIGNAL)
      logInfo(log"Registering SIG${MDC(LogKeys.SIGNAL, signal)}" +
        log" handler to trigger decommissioning.")
      // 注册信号处理器，收到信号时触发退役流程
      SignalUtils.register(signal, log"Failed to register SIG${MDC(LogKeys.SIGNAL, signal)} " +
        log"handler - disabling executor decommission feature.")(
        self.askSync[Boolean](ExecutorDecommissionSigReceived))
    }

    logInfo(log"Connecting to driver: ${MDC(LogKeys.URL, driverUrl)}" )
    try {
      // 创建安全管理器和网络传输配置
      val securityManager = new SecurityManager(env.conf)
      val shuffleClientTransportConf = SparkTransportConf.fromSparkConf(
        env.conf, "shuffle", sslOptions = Some(securityManager.getRpcSSLOptions()))
      // 检查 Netty 直接内存配置是否足够
      if (NettyUtils.preferDirectBufs(shuffleClientTransportConf) &&
          PlatformDependent.maxDirectMemory() < env.conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)) {
        throw new SparkException(s"Netty direct memory should at least be bigger than " +
          s"'${MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM.key}', but got " +
          s"${PlatformDependent.maxDirectMemory()} bytes < " +
          s"${env.conf.get(MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM)}")
      }

      // 解析或发现分配给此 Executor 的资源
      _resources = parseOrFindResources(resourcesFileOpt)
    } catch {
      case NonFatal(e) =>
        exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
    }
    // 异步连接到 Driver 并注册 Executor
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // 这是一个非常快的操作，可以使用 "ThreadUtils.sameThread"
      driver = Some(ref)
      env.executorBackend = Option(this)
      // 向 Driver 发送注册请求，包含 Executor 信息
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls,
        extractAttributes, _resources, resourceProfile.id))
    }(ThreadUtils.sameThread).onComplete {
      case Success(_) =>
        // 注册成功，发送 RegisteredExecutor 消息触发 Executor 创建
        self.send(RegisteredExecutor)
      case Failure(e) =>
        // 注册失败，退出 Executor
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
    }(ThreadUtils.sameThread)
  }

  /**
   * 创建用于资源发现的类加载器。
   * 用户可能提供自定义类作为默认资源发现类的替代，需要能够从用户指定的 JAR 加载。
   */
  private def createClassLoader(): MutableURLClassLoader = {
    val currentLoader = Utils.getContextOrSparkClassLoader
    val urls = getUserClassPath.toArray
    if (env.conf.get(EXECUTOR_USER_CLASS_PATH_FIRST)) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * 解析或发现 Executor 的资源配置。
   * 使用包含用户类路径的类加载器，以防用户指定了自定义资源发现类。
   * 此方法对测试可见。
   */
  def parseOrFindResources(resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    // use a classloader that includes the user classpath in case they specified a class for
    // resource discovery
    val urlClassLoader = createClassLoader()
    logDebug(s"Resource profile id is: ${resourceProfile.id}")
    Utils.withContextClassLoader(urlClassLoader) {
      val resources = getOrDiscoverAllResourcesForResourceProfile(
        resourcesFileOpt,
        SPARK_EXECUTOR_PREFIX,
        resourceProfile,
        env.conf)
      logResourceInfo(SPARK_EXECUTOR_PREFIX, resources)
      resources
    }
  }

  /** 获取用户类路径，子类可重写以提供自定义类路径 */
  def getUserClassPath: Seq[URL] = Nil

  /** 从环境变量提取日志 URL（如 SPARK_LOG_URL_STDOUT） */
  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filter { case (k, _) => k.startsWith(prefix) }
      .map(e => (e._1.substring(prefix.length).toLowerCase(Locale.ROOT), e._2))
  }

  /** 从环境变量提取 Executor 属性（如 SPARK_EXECUTOR_ATTRIBUTE_*） */
  def extractAttributes: Map[String, String] = {
    val prefix = "SPARK_EXECUTOR_ATTRIBUTE_"
    sys.env.filter { case (k, _) => k.startsWith(prefix) }
      .map(e => (e._1.substring(prefix.length).toUpperCase(Locale.ROOT), e._2))
  }

  /** 通知 Driver Shuffle 数据推送完成（Push-based Shuffle） */
  def notifyDriverAboutPushCompletion(shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    val msg = ShufflePushCompletion(shuffleId, shuffleMergeId, mapIndex)
    driver.foreach(_.send(msg))
  }

  /**
   * 处理来自 Driver 的消息（无需回复）。
   */
  override def receive: PartialFunction[Any, Unit] = {
    // 处理注册成功消息，创建 Executor 实例
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, getUserClassPath, isLocal = false,
          resources = _resources)
        // 通知 Driver Executor 已启动
        driver.get.send(LaunchedExecutor(executorId))
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }
    // 处理日志级别更新请求
    case UpdateExecutorLogLevel(newLogLevel) =>
      Utils.setLogLevelIfNeeded(newLogLevel)

    // 处理启动任务请求
    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        // 反序列化任务描述并启动任务
        val taskDesc = TaskDescription.decode(data.value)
        logInfo(log"Got assigned task ${MDC(LogKeys.TASK_ID, taskDesc.taskId)}")
        executor.launchTask(this, taskDesc)
      }

    // 处理终止任务请求
    case KillTask(taskId, _, interruptThread, reason) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread, reason)
      }

    // 处理停止 Executor 请求
    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // 不能在这里直接关闭，因为可能需要向调用者发送确认消息
      // 所以发送消息给自己来实际执行关闭
      self.send(Shutdown)

    // 实际执行关闭流程
    case Shutdown =>
      stopping.set(true)
      // 在新线程中执行关闭，避免死锁
      // 在新线程中执行关闭，避免死锁
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // 如果 onStart 中出错或创建 Executor 失败，executor 可能为 null
          if (executor == null) {
            System.exit(1)
          } else {
            // executor.stop() 会调用 SparkEnv.stop()，后者会等待 RpcEnv 完全停止
            // 但如果 executor.stop() 在 RpcEnv 的某个线程中运行，RpcEnv 无法停止
            // 直到 executor.stop() 返回，这会造成死锁（参见 SPARK-14180）
            // 因此，将此行放在新线程中执行
            executor.stop()
          }
        }
      }.start()

    // 处理委托令牌更新（用于访问安全集群如 Kerberos）
    case UpdateDelegationTokens(tokenBytes) =>
      logInfo(log"Received tokens of ${MDC(LogKeys.NUM_BYTES, tokenBytes.length)} bytes")
      SparkHadoopUtil.get.addDelegationTokens(tokenBytes, env.conf)

    // 处理退役请求
    case DecommissionExecutor =>
      decommissionSelf()
  }

  /**
   * 处理需要回复的 RPC 消息。
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // 处理收到退役信号的情况
    // 处理收到退役信号的情况
    case ExecutorDecommissionSigReceived =>
      var driverNotified = false
      try {
        driver.foreach { driverRef =>
          // 告知 Driver 我们开始退役，让它停止调度任务到此 Executor
          driverNotified = driverRef.askSync[Boolean](ExecutorDecommissioning(executorId))
          if (driverNotified) decommissionSelf()
        }
      } catch {
        case e: Exception =>
          if (driverNotified) {
            logError("Fail to decommission self (but driver has been notified).", e)
          } else {
            logError("Fail to tell driver that we are starting decommissioning", e)
          }
          decommissioned = false
      }
      context.reply(decommissioned)

    // 处理获取任务线程转储的请求
    case TaskThreadDump(taskId) =>
      context.reply(executor.getTaskThreadDump(taskId))
  }

  /**
   * 当与远程端点断开连接时的处理逻辑。
   * 如果是与 Driver 断开连接，需要关闭 Executor。
   */
  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(log"Driver from ${MDC(LogKeys.RPC_ADDRESS, remoteAddress)}" +
        log" disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.", null,
        notifyDriver = false)
    } else {
      logWarning(log"An unknown (${MDC(LogKeys.REMOTE_ADDRESS, remoteAddress)} " +
        log"driver disconnected.")
    }
  }

  /**
   * 向 Driver 汇报任务状态更新。
   * 实现 ExecutorBackend 接口的核心方法。
   */
  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = {
    val taskDescription = executor.runningTasks.get(taskId).taskDescription
    val resources = taskDescription.resources
    val cpus = taskDescription.cpus
    val msg = StatusUpdate(executorId, taskId, state, data, cpus, resources)
    // 如果任务完成，更新最后任务完成时间
    if (TaskState.isFinished(state)) {
      lastTaskFinishTime.set(System.nanoTime())
    }
    // 发送状态更新给 Driver
    driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None =>
        logWarning(log"Drop ${MDC(LogKeys.MESSAGE, msg)} because has not yet connected to driver")
    }
  }

  /**
   * 退出 Executor 并通知 Driver。
   * 子类可重写此方法以实现不同的退出行为。
   * 例如，某些后端可能不希望子进程退出时影响父进程。
   */
  protected def exitExecutor(code: Int,
                             reason: String,
                             throwable: Throwable = null,
                             notifyDriver: Boolean = true) = {
    if (stopping.compareAndSet(false, true)) {
      val message = log"Executor self-exiting due to : ${MDC(LogKeys.REASON, reason)}"
      if (throwable != null) {
        logError(message, throwable)
      } else {
        if (code == 0) {
          logInfo(message)
        } else {
          logError(message)
        }
      }

      if (notifyDriver && driver.nonEmpty) {
        driver.get.send(RemoveExecutor(executorId, new ExecutorLossReason(reason)))
      }
      self.send(Shutdown)
    } else {
      logInfo("Skip exiting executor since it's been already asked to exit before.")
    }
  }

  /**
   * 执行 Executor 自身的退役流程。
   * 
   * 退役（Decommission）是 Spark 优雅关闭 Executor 的机制，允许：
   * - 完成正在运行的任务
   * - 迁移缓存的 RDD 和 Shuffle 数据到其他 Executor
   * - 避免因突然关闭导致的数据丢失和任务失败
   */
  private def decommissionSelf(): Unit = {
    // 检查退役功能是否启用
    if (!env.conf.get(DECOMMISSION_ENABLED)) {
      logWarning("Receive decommission request, but decommission feature is disabled.")
      return
    } else if (decommissioned) {
      logWarning(log"Executor ${MDC(LogKeys.EXECUTOR_ID, executorId)} " +
        log"already started decommissioning.")
      return
    }
    logInfo(log"Decommission executor ${MDC(LogKeys.EXECUTOR_ID, executorId)}.")
    try {
      decommissioned = true
      // 检查是否启用了数据迁移
      val migrationEnabled = env.conf.get(STORAGE_DECOMMISSION_ENABLED) &&
        (env.conf.get(STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED) ||
          env.conf.get(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED))
      // 如果启用了数据迁移，开始迁移 Block 数据
      if (migrationEnabled) {
        env.blockManager.decommissionBlockManager()
      } else if (env.conf.get(STORAGE_DECOMMISSION_ENABLED)) {
        logError(log"Storage decommissioning attempted but neither " +
          log"${MDC(LogKeys.CONFIG, STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key)} or " +
          log"${MDC(LogKeys.CONFIG2, STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key)} is enabled ")
      }
      // 通知 Executor 进入退役状态
      if (executor != null) {
        executor.decommission()
      }
      // 启动后台线程，在所有任务完成且数据迁移完成后关闭 Executor
      // 检测迁移完成不需要完美精确，我们希望最小化对非退役状态 Executor 的开销
      // 例如，这不会捕获在迁移开始前已经从远程 Executor 推送的 Block
      // 这种权衡被认为是可接受的

      val shutdownThread = new Thread("wait-for-blocks-to-migrate") {
        override def run(): Unit = {
          val sleep_time = 1000 // 1s
          // This config is internal and only used by unit tests to force an executor
          // to hang around for longer when decommissioned.
          val initialSleepMillis = env.conf.getInt(
            "spark.test.executor.decommission.initial.sleep.millis", sleep_time)
          if (initialSleepMillis > 0) {
            Thread.sleep(initialSleepMillis)
          }
          while (true) {
            logInfo("Checking to see if we can shutdown.")
            if (executor == null || executor.numRunningTasks == 0) {
              if (migrationEnabled) {
                logInfo("No running tasks, checking migrations")
                val (migrationTime, allBlocksMigrated) = env.blockManager.lastMigrationInfo()
                // We can only trust allBlocksMigrated boolean value if there were no tasks running
                // since the start of computing it.
                if (allBlocksMigrated && (migrationTime > lastTaskFinishTime.get())) {
                  logInfo("No running tasks, all blocks migrated, stopping.")
                  exitExecutor(0, ExecutorLossMessage.decommissionFinished, notifyDriver = true)
                } else {
                  logInfo("All blocks not yet migrated.")
                }
              } else {
                logInfo("No running tasks, no block migration configured, stopping.")
                exitExecutor(0, ExecutorLossMessage.decommissionFinished, notifyDriver = true)
              }
            } else {
              logInfo(log"Blocked from shutdown by" +
                log" ${MDC(LogKeys.NUM_TASKS, executor.numRunningTasks)} running tasks")
            }
            Thread.sleep(sleep_time)
          }
        }
      }
      shutdownThread.setDaemon(true)
      shutdownThread.start()

      logInfo("Will exit when finished decommissioning")
    } catch {
      case e: Exception =>
        decommissioned = false
        logError("Unexpected error while decommissioning self", e)
    }
  }
}

/**
 * CoarseGrainedExecutorBackend 伴生对象。
 * 包含 Executor 后端的入口点（main 方法）和启动逻辑。
 */
private[spark] object CoarseGrainedExecutorBackend extends Logging {

  // 内部消息：Driver 成功接受注册请求后用于启动 Executor
  case object RegisteredExecutor

  /**
   * Executor 后端的命令行参数。
   */
  case class Arguments(
      driverUrl: String,
      executorId: String,
      bindAddress: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      resourcesFileOpt: Option[String],
      resourceProfileId: Int)

  /**
   * Executor 后端的入口点。
   * 由 YARN、Kubernetes 或 Standalone 模式的资源管理器启动。
   */
  def main(args: Array[String]): Unit = {
    // 创建 Executor 后端的工厂函数
    val createFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = { case (rpcEnv, arguments, env, resourceProfile) =>
      new CoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl, arguments.executorId,
        arguments.bindAddress, arguments.hostname, arguments.cores,
        env, arguments.resourcesFileOpt, resourceProfile)
    }
    run(parseArguments(args, this.getClass.getCanonicalName.stripSuffix("$")), createFn)
    System.exit(0)
  }

  /**
   * 运行 Executor 后端。
   * 连接到 Driver，获取配置，创建 SparkEnv 和 Executor。
   */
  def run(
      arguments: Arguments,
      backendCreateFn: (RpcEnv, Arguments, SparkEnv, ResourceProfile) =>
        CoarseGrainedExecutorBackend): Unit = {

    // 重置结构化日志配置
    Utils.resetStructuredLogging()
    Utils.initDaemon(log)

    // 以 Spark 用户身份运行
    SparkHadoopUtil.get.runAsSparkUser { () =>
      // 调试代码：校验主机名
      Utils.checkHost(arguments.hostname)

      // 引导程序：获取 Driver 的 Spark 属性
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        arguments.bindAddress,
        arguments.hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        numUsableCores = 0,
        clientMode = true)

      // 尝试连接 Driver（最多重试 3 次）
      var driver: RpcEndpointRef = null
      val nTries = 3
      for (i <- 0 until nTries if driver == null) {
        try {
          driver = fetcher.setupEndpointRefByURI(arguments.driverUrl)
        } catch {
          case e: Throwable => if (i == nTries - 1) {
            throw e
          }
        }
      }

      // 从 Driver 获取应用配置
      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig(arguments.resourceProfileId))
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", arguments.appId))
      fetcher.shutdown()

      // 使用从 Driver 获取的属性创建 SparkEnv
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // 这是 Standalone 模式 SSL 所需的配置
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      // 在 `spark.log.structuredLogging.enabled` 生效后重新初始化日志系统
      Utils.resetStructuredLogging(driverConf)
      Logging.uninitialize()

      // 如果有 Hadoop 委托凭证，添加到配置中
      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      // 设置 Executor ID
      driverConf.set(EXECUTOR_ID, arguments.executorId)
      // 设置日志级别
      cfg.logLevel.foreach(logLevel => Utils.setLogLevelIfNeeded(logLevel))

      // 根据资源配置文件设置 Executor 内存相关配置
      if (cfg.resourceProfile.id != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) {
        cfg.resourceProfile
          .executorResources
          .foreach {
            case (ResourceProfile.OFFHEAP_MEM, request) =>
              driverConf.set(MEMORY_OFFHEAP_SIZE.key, request.amount.toString + "m")
              logInfo(log"Set executor off-heap memory to " +
                log"${MDC(LogKeys.EXECUTOR_MEMORY_OFFHEAP, request)}")
            case (ResourceProfile.MEMORY, request) =>
              driverConf.set(EXECUTOR_MEMORY.key, request.amount.toString + "m")
              logInfo(log"Set executor memory to ${MDC(LogKeys.EXECUTOR_MEMORY_SIZE, request)}")
            case (ResourceProfile.OVERHEAD_MEM, request) =>
              // Maybe don't need to set this since it's nearly used by tasks.
              driverConf.set(EXECUTOR_MEMORY_OVERHEAD.key, request.amount.toString + "m")
              logInfo(log"Set executor memory_overhead to " +
                log"${MDC(LogKeys.EXECUTOR_MEMORY_OVERHEAD_SIZE, request)}")
            case (ResourceProfile.CORES, request) =>
              driverConf.set(EXECUTOR_CORES.key, request.amount.toString)
              logInfo(log"Set executor cores to ${MDC(LogKeys.NUM_EXECUTOR_CORES, request)}")
            case _ =>
          }
      }
      // 创建 Executor 端的 SparkEnv
      val env = SparkEnv.createExecutorEnv(driverConf, arguments.executorId, arguments.bindAddress,
        arguments.hostname, arguments.cores, cfg.ioEncryptionKey, isLocal = false)
      // 如果有应用尝试 ID，设置到 BlockStoreClient
      val appAttemptId = env.conf.get(APP_ATTEMPT_ID)
      appAttemptId.foreach(attemptId =>
        env.blockManager.blockStoreClient.setAppAttemptId(attemptId)
      )
      // 创建 Executor 后端实例
      val backend = backendCreateFn(env.rpcEnv, arguments, env, cfg.resourceProfile)
      // 注册 Executor 端点
      env.rpcEnv.setupEndpoint("Executor", backend)
      // 如果有 Worker URL（Standalone 模式），设置 WorkerWatcher 端点
      // WorkerWatcher 用于在 Worker 进程退出时感知并退出 Executor
      arguments.workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher",
          new WorkerWatcher(env.rpcEnv, url, isChildProcessStopping = backend.stopping))
      }
      // 等待 RpcEnv 终止
      env.rpcEnv.awaitTermination()
    }
  }

  /**
   * 解析命令行参数。
   */
  def parseArguments(args: Array[String], classNameForEntry: String): Arguments = {
    var driverUrl: String = null
    var executorId: String = null
    var bindAddress: String = null
    var hostname: String = null
    var cores: Int = 0
    var resourcesFileOpt: Option[String] = None
    var appId: String = null
    var workerUrl: Option[String] = None
    var resourceProfileId: Int = DEFAULT_RESOURCE_PROFILE_ID

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--bind-address") :: value :: tail =>
          bindAddress = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--resourcesFile") :: value :: tail =>
          resourcesFileOpt = Some(value)
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--resourceProfileId") :: value :: tail =>
          resourceProfileId = value.toInt
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit(classNameForEntry)
      }
    }

    if (hostname == null) {
      hostname = Utils.localHostName()
      log.info(s"Executor hostname is not provided, will use '$hostname' to advertise itself")
    }

    if (driverUrl == null || executorId == null || cores <= 0 || appId == null) {
      printUsageAndExit(classNameForEntry)
    }

    if (bindAddress == null) {
      bindAddress = hostname
    }

    Arguments(driverUrl, executorId, bindAddress, hostname, cores, appId, workerUrl,
      resourcesFileOpt, resourceProfileId)
  }

  private def printUsageAndExit(classNameForEntry: String): Unit = {
    // scalastyle:off println
    System.err.println(
      s"""
      |Usage: $classNameForEntry [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --bind-address <bindAddress>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --resourcesFile <fileWithJSONResourceInformation>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --resourceProfileId <id>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }
}

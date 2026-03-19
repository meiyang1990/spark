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

package org.apache.spark

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.concurrent
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Properties

import com.google.common.base.Preconditions
import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.{PythonWorker, PythonWorkerFactory}
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * :: DeveloperApi ::
 * 持有运行中Spark实例（Driver或Executor）的所有运行时环境对象，
 * 包括序列化器、RpcEnv、BlockManager、MapOutputTracker等。
 * 当前Spark通过全局变量查找SparkEnv，所有线程可以访问同一个SparkEnv实例。
 * 可通过SparkEnv.get获取（需在创建SparkContext之后）。
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  // ShuffleManager延迟到SparkContext和Executor中初始化，以允许用户jar定义自定义ShuffleManager
  @volatile private var _shuffleManager: ShuffleManager = _

  // 用于通知ShuffleManager已初始化完成的CountDownLatch
  private val shuffleManagerInitLatch = new CountDownLatch(1)

  /** 获取ShuffleManager实例 */
  def shuffleManager: ShuffleManager = _shuffleManager

  /** 在指定超时时间内等待ShuffleManager初始化完成 */
  private[spark] def waitForShuffleManagerInit(timeoutMs: Long): Boolean = {
    shuffleManagerInitLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
  }

  /** 检查ShuffleManager是否已初始化 */
  private[spark] def isShuffleManagerInitialized: Boolean = {
    _shuffleManager != null
  }

  // MemoryManager延迟到SparkContext中DriverPlugin加载后初始化，以允许插件覆盖Executor内存配置
  private var _memoryManager: MemoryManager = _

  def memoryManager: MemoryManager = _memoryManager

  @volatile private[spark] var isStopped = false

  /**
   * PythonWorkerFactory缓存的键，包含Python可执行文件路径、工作模块、守护模块和环境变量。
   */
  private case class PythonWorkersKey(
      pythonExec: String, workerModule: String, daemonModule: String, envVars: Map[String, String])
  private val pythonWorkers = mutable.HashMap[PythonWorkersKey, PythonWorkerFactory]()

  // 通用的软引用缓存，用于HadoopRDD分区计算时所需的元数据（如JobConf和InputFormat）
  private[spark] val hadoopJobMetadata =
    CacheBuilder.newBuilder().maximumSize(1000).softValues().build[String, AnyRef]().asMap()

  private[spark] var driverTmpDir: Option[String] = None

  private[spark] var executorBackend: Option[ExecutorBackend] = None

  /**
   * 停止SparkEnv中的所有组件：Python工作进程、MapOutputTracker、ShuffleManager、
   * BroadcastManager、BlockManager、MetricsSystem、OutputCommitCoordinator和RpcEnv。
   * Driver端还会清理临时目录。
   */
  private[spark] def stop(): Unit = {

    if (!isStopped) {
      isStopped = true
      // 停止所有Python工作进程工厂
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      if (shuffleManager != null) {
        shuffleManager.stop()
      }
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      // 关闭RPC环境并等待终止
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // 如果只是停止SparkContext但Driver进程仍作为服务运行，需要删除临时目录，
      // 否则会创建过多的临时目录。仅Driver端需要删除。
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(log"Exception while deleting Spark temp dir: " +
                log"${MDC(LogKeys.PATH, path)}", e)
          }
        case None => // Executor端无需处理
      }
    }
  }

  /**
   * 创建Python工作进程。使用工厂模式，对于相同参数组合复用同一个PythonWorkerFactory。
   * 返回(PythonWorker, 可选的进程句柄)。
   */
  private[spark] def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      useDaemon: Boolean): (PythonWorker, Option[ProcessHandle]) = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      // 获取或创建对应的PythonWorkerFactory
      val workerFactory = pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(
          pythonExec, workerModule, daemonModule, envVars, useDaemon))
      // 同一个工厂的useDaemon设置必须一致
      if (workerFactory.useDaemonEnabled != useDaemon) {
        throw SparkException.internalError("PythonWorkerFactory is already created with " +
          s"useDaemon = ${workerFactory.useDaemonEnabled}, but now is requested with " +
          s"useDaemon = $useDaemon. This is not allowed to change after the PythonWorkerFactory " +
          s"is created given the same key: $key.")
      }
      workerFactory.create()
    }
  }

  /** 使用默认守护模块创建Python工作进程 */
  private[spark] def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      useDaemon: Boolean): (PythonWorker, Option[ProcessHandle]) = {
    createPythonWorker(
      pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule, envVars, useDaemon)
  }

  /** 使用配置中的useDaemon设置创建Python工作进程 */
  private[spark] def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String]): (PythonWorker, Option[ProcessHandle]) = {
    val useDaemon = conf.get(Python.PYTHON_USE_DAEMON)
    createPythonWorker(
      pythonExec, workerModule, daemonModule, envVars, useDaemon)
  }

  /** 销毁指定的Python工作进程 */
  private[spark] def destroyPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  /** 使用默认守护模块销毁Python工作进程 */
  private[spark] def destroyPythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    destroyPythonWorker(
      pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule, envVars, worker)
  }

  /** 释放Python工作进程回到池中复用 */
  private[spark] def releasePythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }

  /** 使用默认守护模块释放Python工作进程 */
  private[spark] def releasePythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    releasePythonWorker(
      pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule, envVars, worker)
  }

  /** 初始化ShuffleManager，只能调用一次。完成后通过countDown通知等待者。 */
  private[spark] def initializeShuffleManager(): Unit = {
    Preconditions.checkState(null == _shuffleManager,
      "Shuffle manager already initialized to %s", _shuffleManager)
    try {
      _shuffleManager = ShuffleManager.create(conf, executorId == SparkContext.DRIVER_IDENTIFIER)
    } finally {
      // 无论成功与否都发出初始化完成信号
      shuffleManagerInitLatch.countDown()
    }
  }

  /** 初始化MemoryManager（UnifiedMemoryManager），只能调用一次 */
  private[spark] def initializeMemoryManager(numUsableCores: Int): Unit = {
    Preconditions.checkState(null == memoryManager,
      "Memory manager already initialized to %s", _memoryManager)
    _memoryManager = UnifiedMemoryManager(conf, numUsableCores)
  }
}

/** SparkEnv伴生对象，持有全局SparkEnv实例并提供创建Driver/Executor环境的工厂方法 */
object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  /** 设置全局SparkEnv实例 */
  def set(e: SparkEnv): Unit = {
    env = e
  }

  /** 获取当前全局SparkEnv实例 */
  def get: SparkEnv = {
    env
  }

  /**
   * 为Driver端创建SparkEnv。
   * 需要conf中已设置DRIVER_HOST_ADDRESS和DRIVER_PORT。
   * 如果启用了IO加密则生成加密密钥。
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get(DRIVER_PORT)
    // 如果启用了IO加密，创建加密密钥
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * 为Executor端创建SparkEnv。
   * 在粗粒度模式下，Executor提供一个已实例化的RpcEnv。
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      bindAddress,
      hostname,
      None,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    // Executor端需要显式初始化MemoryManager
    env.initializeMemoryManager(numCores)
    SparkEnv.set(env)
    env
  }

  /**
   * 创建SparkEnv的核心方法，Driver和Executor共用。
   * 按顺序创建以下组件：SecurityManager、RpcEnv、Serializer、SerializerManager、
   * BroadcastManager、MapOutputTracker、BlockManagerMaster、BlockManager、
   * MetricsSystem、OutputCommitCoordinator。
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Option[Int],
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // 事件监听总线仅在Driver端使用
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }
    // 根据角色选择不同的认证密钥文件配置
    val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
    // 创建安全管理器
    val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
    if (isDriver) {
      securityManager.initializeAuth()
    }

    // 如果启用了IO加密但未启用RPC加密，警告密钥会以明文传输
    ioEncryptionKey.foreach { _ =>
      if (!(securityManager.isEncryptionEnabled() || securityManager.isSslRpcEnabled())) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    // 创建RPC环境
    val systemName = if (isDriver) driverSystemName else executorSystemName
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)

    // 如果原始端口为0或被占用，获取RpcEnv实际绑定的端口并更新配置
    if (isDriver) {
      conf.set(DRIVER_PORT, rpcEnv.address.port)
    }

    // 创建数据序列化器（用于RDD数据序列化）
    val serializer = Utils.instantiateSerializerFromConf[Serializer](SERIALIZER, conf, isDriver)
    logDebug(s"Using serializer: ${serializer.getClass}")

    // 创建序列化管理器（管理序列化、压缩和加密）
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    // 创建闭包序列化器（用于Task闭包的序列化，固定使用JavaSerializer）
    val closureSerializer = new JavaSerializer(conf)

    /**
     * 辅助方法：Driver端注册RPC端点，Executor端查找Driver端已注册的端点引用
     */
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        logInfo(log"Registering ${MDC(LogKeys.ENDPOINT_NAME, name)}")
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }

    // 创建广播管理器
    val broadcastManager = new BroadcastManager(isDriver, conf)

    // 创建MapOutputTracker：Driver端创建Master版本，Executor端创建Worker版本
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // MapOutputTrackerEndpoint依赖MapOutputTracker本身，所以在初始化后才能赋值
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Driver和Executor使用不同的BlockManager端口配置
    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }

    // 如果启用了外部Shuffle服务，创建ExternalBlockStoreClient
    val externalShuffleClient = if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      val transConf = SparkTransportConf.fromSparkConf(
        conf,
        "shuffle",
        numUsableCores,
        sslOptions = Some(securityManager.getRpcSSLOptions())
      )
      Some(new ExternalBlockStoreClient(transConf, securityManager,
        securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
    } else {
      None
    }

    // BlockManagerId到BlockManagerInfo的映射表
    val blockManagerInfo = new concurrent.TrieMap[BlockManagerId, BlockManagerInfo]()
    // 创建BlockManagerMaster（包含Driver端点和心跳端点）
    val blockManagerMaster = new BlockManagerMaster(
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_ENDPOINT_NAME,
        new BlockManagerMasterEndpoint(
          rpcEnv,
          isLocal,
          conf,
          listenerBus,
          if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
            externalShuffleClient
          } else {
            None
          }, blockManagerInfo,
          mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
          _shuffleManager = null,
          isDriver)),
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_HEARTBEAT_ENDPOINT_NAME,
        new BlockManagerMasterHeartbeatEndpoint(rpcEnv, isLocal, blockManagerInfo)),
      conf,
      isDriver)

    // 创建基于Netty的Block传输服务
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, serializerManager, bindAddress,
        advertiseAddress, blockManagerPort, numUsableCores, blockManagerMaster.driverEndpoint)

    // 注意：BlockManager在调用initialize()之前不可用。
    // SPARK-45762: ShuffleManager延迟到SparkContext/Executor中初始化，
    // BlockManager使用lazy val从SparkEnv获取ShuffleManager。
    val blockManager = new BlockManager(
      executorId,
      rpcEnv,
      blockManagerMaster,
      serializerManager,
      conf,
      _memoryManager = null,
      mapOutputTracker,
      _shuffleManager = null,
      blockTransferService,
      securityManager,
      externalShuffleClient)

    // 创建MetricsSystem：Driver端延迟启动（等待TaskScheduler分配app ID），
    // Executor端立即启动
    val metricsSystem = if (isDriver) {
      MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf)
    } else {
      // Executor端需要在创建MetricsSystem前设置executor ID，
      // 因为指标源和接收器会在上报中使用此ID
      conf.set(EXECUTOR_ID, executorId)
      val ms = MetricsSystem.createMetricsSystem(MetricsSystemInstances.EXECUTOR, conf)
      ms.start(conf.get(METRICS_STATIC_SOURCES_ENABLED))
      ms
    }

    // 创建OutputCommitCoordinator（协调多个任务的输出提交，防止重复提交）
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    // 组装所有组件创建SparkEnv实例
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      outputCommitCoordinator,
      conf)

    // Driver端创建临时目录用于存放用户上传的文件，停止时会清理
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * 返回JVM信息、Spark属性、系统属性、类路径的Map表示。
   * 主要用于SparkListenerEnvironmentUpdate事件。
   */
  private[spark] def environmentDetails(
      conf: SparkConf,
      hadoopConf: Configuration,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String],
      addedArchives: Seq[String],
      metricsProperties: Map[String, String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    // JVM基本信息
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark配置属性（包含调度模式，无论是否显式配置都会包含，SparkUI需要）
    val schedulerMode =
      if (!conf.contains(SCHEDULER_MODE)) {
        Seq((SCHEDULER_MODE.key, schedulingMode))
      } else {
        Seq.empty[(String, String)]
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // 系统属性（排除Java classpath和spark.*开头的属性）
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // 类路径（包含用户添加的JAR/文件/归档和系统classpath）
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles ++ addedArchives).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    // Hadoop配置属性
    val hadoopProperties = hadoopConf.asScala
      .map(entry => (entry.getKey, entry.getValue)).toSeq.sorted
    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties.toImmutableArraySeq,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths,
      "Metrics Properties" -> metricsProperties.toSeq.sorted)
  }
}

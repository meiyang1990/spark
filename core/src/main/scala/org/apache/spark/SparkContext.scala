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

package org.apache.spark

import java.io._
import java.net.URI
import java.util.{Arrays, Locale, Properties, ServiceLoader, UUID}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.collection.Map
import scala.collection.concurrent.{Map => ScalaConcurrentMap}
import scala.collection.immutable
import scala.collection.mutable.HashMap
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

import com.google.common.collect.MapMaker
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.{Executor, ExecutorMetrics, ExecutorMetricsSource}
import org.apache.spark.input.{FixedLengthBinaryInputFormat, PortableDataStream, StreamInputFormat, WholeTextFileInputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests._
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.io.CompressionCodec
import org.apache.spark.launcher.{JavaModuleOptions, SparkLauncher}
import org.apache.spark.metrics.source.JVMCPUSource
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.resource._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.shuffle.ShuffleDataIOUtils
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.status.{AppStatusSource, AppStatusStore}
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.{TriggerHeapHistogram, TriggerThreadDump}
import org.apache.spark.ui.{ConsoleProgressBar, SparkUI}
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.logging.DriverLogger

/**
 * Spark功能的主入口点。SparkContext代表与Spark集群的连接，
 * 可用于在集群上创建RDD、累加器和广播变量。
 *
 * @note 每个JVM中只能有一个活跃的`SparkContext`。在创建新的SparkContext之前，
 *       必须先调用`stop()`停止当前活跃的SparkContext。
 * @param config SparkConf对象，描述应用程序的配置。此配置中的设置将覆盖默认配置和系统属性。
 */
class SparkContext(config: SparkConf) extends Logging {

  // 记录此SparkContext被构造时的调用位置
  private val creationSite: CallSite = Utils.getCallSite()

  // 记录此SparkContext被停止时的调用位置
  private var stopSite: Option[CallSite] = None

  if (!config.get(EXECUTOR_ALLOW_SPARK_CONTEXT)) {
    // 防止在Executor端创建SparkContext，只允许在Driver端创建
    SparkContext.assertOnDriver()
  }

  // 为防止多个SparkContext同时处于活跃状态，标记此上下文已开始构造。
  // 注意：此代码必须放在SparkContext构造函数的最开始位置。
  SparkContext.markPartiallyConstructed(this)

  val startTime = System.currentTimeMillis()

  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)

  /** 断言SparkContext尚未停止，若已停止则抛出异常，包含创建和停止位置的详细信息 */
  private[spark] def assertNotStopped(): Unit = {
    if (stopped.get()) {
      val activeContext = SparkContext.activeContext.get()
      val activeCreationSite =
        if (activeContext == null) {
          "(No active SparkContext.)" // 当前没有活跃的SparkContext
        } else {
          activeContext.creationSite.longForm // 获取当前活跃上下文的创建位置
        }
      throw new IllegalStateException(
        s"""Cannot call methods on a stopped SparkContext.
           |This stopped SparkContext was created at:
           |
           |${creationSite.longForm}
           |
           |And it was stopped at:
           |
           |${stopSite.getOrElse(CallSite.empty).longForm}
           |
           |The currently active SparkContext was created at:
           |
           |$activeCreationSite
         """.stripMargin)
    }
  }

  /**
   * 创建一个从系统属性加载设置的SparkContext（例如通过./bin/spark-submit启动时）
   */
  def this() = this(new SparkConf())

  /**
   * 允许直接设置常用Spark属性的替代构造函数
   *
   * @param master 要连接的集群URL（例如 spark://host:port, local[4]）
   * @param appName 应用程序名称，将显示在集群Web UI上
   * @param conf SparkConf对象，指定其他Spark参数
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(SparkContext.updatedConf(conf, master, appName))

  /**
   * 允许直接设置常用Spark属性的替代构造函数
   *
   * @param master 要连接的集群URL（例如 spark://host:port, local[4]）
   * @param appName 应用程序名称，将显示在集群Web UI上
   * @param sparkHome Spark在集群节点上的安装位置
   * @param jars 要发送到集群的JAR包集合。可以是本地文件系统路径或HDFS/HTTP/HTTPS/FTP URL
   * @param environment 要在Worker节点上设置的环境变量
   */
  def this(
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()) = {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
  }

  // 以下构造函数是Java代码直接访问SparkContext时所需的，参见SI-4278

  /**
   * 允许直接设置常用Spark属性的替代构造函数
   *
   * @param master 要连接的集群URL（例如 spark://host:port, local[4]）
   * @param appName 应用程序名称，将显示在集群Web UI上
   */
  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map())

  /**
   * 允许直接设置常用Spark属性的替代构造函数
   *
   * @param master 要连接的集群URL（例如 spark://host:port, local[4]）
   * @param appName 应用程序名称，将显示在集群Web UI上
   * @param sparkHome Spark在集群节点上的安装位置
   */
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map())

  /**
   * 允许直接设置常用Spark属性的替代构造函数
   *
   * @param master 要连接的集群URL（例如 spark://host:port, local[4]）
   * @param appName 应用程序名称，将显示在集群Web UI上
   * @param sparkHome Spark在集群节点上的安装位置
   * @param jars 要发送到集群的JAR包集合。可以是本地文件系统路径或HDFS/HTTP/HTTPS/FTP URL
   */
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map())

  // 在Spark Driver日志中输出Spark版本信息
  logInfo(log"Running Spark version ${MDC(LogKeys.SPARK_VERSION, SPARK_VERSION)}")
  logInfo(log"OS info ${MDC(LogKeys.OS_NAME, Utils.osName)}," +
    log" ${MDC(LogKeys.OS_VERSION, Utils.osVersion)}, " +
    log"${MDC(LogKeys.OS_ARCH, Utils.osArch)}")
  logInfo(log"Java version ${MDC(LogKeys.JAVA_VERSION, Utils.javaVersion)}")

  /* ------------------------------------------------------------------------------------- *
   | 私有变量区域。这些变量保存上下文的内部状态，外部不可访问。                              |
   | 它们是可变的，因为需要在构造过程中先初始化为中性值，                                    |
   | 以确保在构造函数仍在运行时调用"stop()"是安全的。                                       |
   * ------------------------------------------------------------------------------------- */

  private var _conf: SparkConf = _
  private var _eventLogDir: Option[URI] = None
  private var _eventLogCodec: Option[String] = None
  private var _listenerBus: LiveListenerBus = _
  private var _env: SparkEnv = _
  private var _statusTracker: SparkStatusTracker = _
  private var _progressBar: Option[ConsoleProgressBar] = None
  private var _ui: Option[SparkUI] = None
  private var _hadoopConfiguration: Configuration = _
  private var _executorMemory: Int = _
  private var _schedulerBackend: SchedulerBackend = _
  private var _taskScheduler: TaskScheduler = _
  private var _heartbeatReceiver: RpcEndpointRef = _
  @volatile private var _dagScheduler: DAGScheduler = _
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _eventLogger: Option[EventLoggingListener] = None
  private var _driverLogger: Option[DriverLogger] = None
  private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
  private var _cleaner: Option[ContextCleaner] = None
  private var _listenerBusStarted: Boolean = false
  private var _jars: Seq[String] = _
  private var _files: Seq[String] = _
  private var _archives: Seq[String] = _
  private var _shutdownHookRef: AnyRef = _
  private var _statusStore: AppStatusStore = _
  private var _heartbeater: Heartbeater = _
  private var _resources: immutable.Map[String, ResourceInformation] = _
  private var _shuffleDriverComponents: ShuffleDriverComponents = _
  private var _plugins: Option[PluginContainer] = None
  private var _resourceProfileManager: ResourceProfileManager = _

  /* ------------------------------------------------------------------------------------- *
   | 访问器和公共字段区域。这些方法提供对上下文内部状态的访问。                              |
   * ------------------------------------------------------------------------------------- */

  private[spark] def conf: SparkConf = _conf

  /** 获取Spark配置的只读引用。相比[[getConf]]，此方法是首选版本。 */
  def getReadOnlyConf: ReadOnlySparkConf = _conf

  /**
   * 返回此SparkContext配置的副本。配置在运行时''不能''被更改。
   */
  def getConf: SparkConf = conf.clone()

  def resources: Map[String, ResourceInformation] = _resources

  def jars: Seq[String] = _jars
  def files: Seq[String] = _files
  def archives: Seq[String] = _archives
  def master: String = _conf.get("spark.master")
  def deployMode: String = _conf.get(SUBMIT_DEPLOY_MODE)
  def appName: String = _conf.get("spark.app.name")

  private[spark] def isEventLogEnabled: Boolean = _conf.get(EVENT_LOG_ENABLED)
  private[spark] def eventLogDir: Option[URI] = _eventLogDir
  private[spark] def eventLogCodec: Option[String] = _eventLogCodec

  def isLocal: Boolean = Utils.isLocalMaster(_conf)

  /**
   * @return true if context is stopped or in the midst of stopping.
   */
  def isStopped: Boolean = stopped.get()

  private[spark] def statusStore: AppStatusStore = _statusStore

  // 异步事件监听总线，用于分发Spark事件
  private[spark] def listenerBus: LiveListenerBus = _listenerBus

  // 此函数允许SparkEnv创建的组件在单元测试中被mock
  /** 创建SparkEnv运行时环境（包含缓存、MapOutputTracker等核心组件） */
  private[spark] def createSparkEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master, conf))
  }

  private[spark] def env: SparkEnv = _env

  // 存储每个会话的UUID及对应的静态文件/JAR的URL和本地时间戳。结构：会话UUID -> (URL -> 时间戳)
  private[spark] val addedFiles = new ConcurrentHashMap[
    String, ScalaConcurrentMap[String, Long]]().asScala
  private[spark] val addedArchives = new ConcurrentHashMap[
    String, ScalaConcurrentMap[String, Long]]().asScala
  private[spark] val addedJars = new ConcurrentHashMap[
    String, ScalaConcurrentMap[String, Long]]().asScala

  /** 获取所有会话中已添加的文件（扁平化为统一Map） */
  private[spark] def allAddedFiles = addedFiles.values.flatten.toMap
  /** 获取所有会话中已添加的归档文件 */
  private[spark] def allAddedArchives = addedArchives.values.flatten.toMap
  /** 获取所有会话中已添加的JAR包 */
  private[spark] def allAddedJars = addedJars.values.flatten.toMap

  // 跟踪所有已持久化的RDD，使用弱引用值以允许GC回收
  private[spark] val persistentRdds = {
    val map: ConcurrentMap[Int, RDD[_]] = new MapMaker().weakValues().makeMap[Int, RDD[_]]()
    map.asScala
  }
  def statusTracker: SparkStatusTracker = _statusTracker

  private[spark] def progressBar: Option[ConsoleProgressBar] = _progressBar

  private[spark] def ui: Option[SparkUI] = _ui

  def uiWebUrl: Option[String] = _ui.map(_.webUrl)

  /**
   * Hadoop代码（如文件系统）所复用的默认Hadoop配置。
   *
   * @note 由于此配置会在所有Hadoop RDD中复用，除非计划为所有Hadoop RDD设置全局配置，
   *       否则最好不要修改它。
   */
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  private[spark] def executorMemory: Int = _executorMemory

  // 传递给Executor的环境变量
  private[spark] val executorEnvs = HashMap[String, String]()

  // 设置运行SparkContext的用户名
  val sparkUser = Utils.getCurrentUserName()

  private[spark] def schedulerBackend: SchedulerBackend = _schedulerBackend

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler
  private[spark] def taskScheduler_=(ts: TaskScheduler): Unit = {
    _taskScheduler = ts
  }

  private[spark] def dagScheduler: DAGScheduler = _dagScheduler
  private[spark] def dagScheduler_=(ds: DAGScheduler): Unit = {
    _dagScheduler = ds
  }

  private[spark] def shuffleDriverComponents: ShuffleDriverComponents = _shuffleDriverComponents

  /**
   * Spark应用程序的唯一标识符。
   * 其格式取决于调度器实现：
   * （例如：
   *  本地模式类似 'local-1433865536131'
   *  YARN模式类似 'application_1433865536131_34483'
   * ）
   */
   */
  def applicationId: String = _applicationId
  def applicationAttemptId: Option[String] = _applicationAttemptId

  private[spark] def eventLogger: Option[EventLoggingListener] = _eventLogger

  private[spark] def executorAllocationManager: Option[ExecutorAllocationManager] =
    _executorAllocationManager

  private[spark] def resourceProfileManager: ResourceProfileManager = _resourceProfileManager

  private[spark] def cleaner: Option[ContextCleaner] = _cleaner

  private[spark] var checkpointDir: Option[String] = None

  // 线程局部变量，用户可通过它在调用栈中向下传递信息
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override def childValue(parent: Properties): Properties = {
      // 注意：创建克隆副本，使父线程属性的变更不会反映到子线程中，
      // 避免令人困惑的语义（SPARK-10563）。
      Utils.cloneProperties(parent)
    }
    override protected def initialValue(): Properties = new Properties()
  }

  /* ------------------------------------------------------------------------------------- *
   | 初始化代码区域。此代码以异常安全的方式初始化上下文。                                    |
   | 所有保存状态的内部字段都在此处初始化，任何错误都会触发stop()方法的调用。                  |
   * ------------------------------------------------------------------------------------- */

  /** 控制日志级别。此设置将覆盖任何用户自定义的日志设置。
   * @param logLevel 期望的日志级别字符串。
   *                 有效值包括：ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String): Unit = {
    // 允许小写或混合大小写的输入
    val upperCased = logLevel.toUpperCase(Locale.ROOT)
    require(SparkContext.VALID_LOG_LEVELS.contains(upperCased),
      s"Supplied level $logLevel did not match one of:" +
        s" ${SparkContext.VALID_LOG_LEVELS.mkString(",")}")
    Utils.setLogLevelIfNeeded(upperCased)
    if (conf.get(EXECUTOR_ALLOW_SYNC_LOG_LEVEL) && _schedulerBackend != null) {
      _schedulerBackend.updateExecutorsLogLevel(upperCased) // 同步更新所有Executor的日志级别
    }
  }

  try {
    _conf = config.clone() // 克隆配置，避免外部修改影响内部状态
    _conf.get(SPARK_LOG_LEVEL).foreach { level =>
      if (Logging.setLogLevelPrinted) {
        System.err.printf("Setting Spark log level to \"%s\".\n", level)
      }
      setLogLevel(level) // 根据配置设置日志级别
    }
    _conf.validateSettings() // 校验配置参数的合法性
    _conf.set("spark.app.startTime", startTime.toString) // 记录应用启动时间

    if (!_conf.contains("spark.master")) {
      throw new SparkException("A master URL must be set in your configuration") // 必须配置master URL
    }
    if (!_conf.contains("spark.app.name")) {
      throw new SparkException("An application name must be set in your configuration") // 必须配置应用名称
    }
    // HADOOP-19229: 云存储上的向量IO优化：增加范围合并阈值
    // Apache Hadoop 3.4.2发布后可以移除此配置
    conf.setIfMissing("spark.hadoop.fs.s3a.vectored.read.min.seek.size", "128K")
    conf.setIfMissing("spark.hadoop.fs.s3a.vectored.read.max.merged.size", "2M")
    // 此配置应尽早设置
    SparkContext.enableMagicCommitterIfNeeded(_conf) // 按需启用S3 Magic Committer

    SparkContext.supplementJavaModuleOptions(_conf) // 补充Java模块选项
    SparkContext.supplementJavaIPv6Options(_conf) // 补充IPv6选项

    _driverLogger = DriverLogger(_conf) // 初始化Driver日志记录器

    val resourcesFileOpt = conf.get(DRIVER_RESOURCES_FILE)
    _resources = getOrDiscoverAllResources(_conf, SPARK_DRIVER_PREFIX, resourcesFileOpt) // 发现并获取Driver端资源（如GPU）
    logResourceInfo(SPARK_DRIVER_PREFIX, _resources)

    // 在Spark Driver日志中记录应用名称
    logInfo(log"Submitted application: ${MDC(LogKeys.APP_NAME, appName)}")

    // 如果是YARN集群模式，必须设置spark.yarn.app.id系统属性
    if (master == "yarn" && deployMode == "cluster" && !_conf.contains("spark.yarn.app.id")) {
      throw new SparkException("Detected yarn cluster mode, but isn't running on a cluster. " +
        "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
    }

    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo(log"Spark configuration:\n${MDC(LogKeys.CONFIG, _conf.toDebugString)}") // 输出完整配置信息
    }

    // 设置Spark Driver的主机地址和端口系统属性
    if (SparkMasterRegex.isK8s(master) &&
        _conf.getBoolean("spark.kubernetes.executor.useDriverPodIP", false)) {
      // K8s模式下，当配置了使用Driver Pod IP时，用绑定地址替代主机地址
      logInfo("Use DRIVER_BIND_ADDRESS instead of DRIVER_HOST_ADDRESS as driver address " +
        "because spark.kubernetes.executor.useDriverPodIP is true in K8s mode.")
      _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_BIND_ADDRESS))
    } else {
      _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
    }
    _conf.setIfMissing(DRIVER_PORT, 0) // 如果未设置Driver端口，默认为0（随机分配）

    _conf.set(EXECUTOR_ID, SparkContext.DRIVER_IDENTIFIER) // 设置Executor ID为Driver标识符

    _jars = Utils.getUserJars(_conf) // 获取用户指定的JAR包列表
    _files = _conf.getOption(FILES.key).map(_.split(",")).map(_.filter(_.nonEmpty))
      .toSeq.flatten // 解析用户指定的文件列表
    _archives = _conf.getOption(ARCHIVES.key).map(Utils.stringToSeq).toSeq.flatten // 解析归档文件列表

    _eventLogDir =
      if (isEventLogEnabled) {
        val unresolvedDir = conf.get(EVENT_LOG_DIR).stripSuffix("/")
        Some(Utils.resolveURI(unresolvedDir)) // 解析事件日志目录URI
      } else {
        None
      }

    _eventLogCodec = {
      val compress = _conf.get(EVENT_LOG_COMPRESS) &&
          !_conf.get(EVENT_LOG_COMPRESSION_CODEC).equalsIgnoreCase("none")
      if (compress && isEventLogEnabled) {
        Some(_conf.get(EVENT_LOG_COMPRESSION_CODEC)).map(CompressionCodec.getShortName) // 获取事件日志压缩编解码器的短名称
      } else {
        None
      }
    }

    _listenerBus = new LiveListenerBus(_conf) // 创建实时事件监听总线

    // 在创建SparkEnv之前初始化应用状态存储和监听器，以确保能接收所有事件
    val appStatusSource = AppStatusSource.createSource(conf)
    _statusStore = AppStatusStore.createLiveStore(conf, appStatusSource) // 创建应用状态的实时存储
    listenerBus.addToStatusQueue(_statusStore.listener.get) // 将状态监听器添加到状态队列

    // 创建Spark执行环境（包含缓存、MapOutputTracker等核心组件）
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env) // 将SparkEnv设置为全局可访问

    // 如果运行REPL，将REPL的输出目录注册到文件服务器
    _conf.getOption("spark.repl.class.outputDir").foreach { path =>
      val replUri = _env.rpcEnv.fileServer.addDirectory("/classes", new File(path))
      _conf.set("spark.repl.class.uri", replUri)
    }

    _statusTracker = new SparkStatusTracker(this, _statusStore) // 创建作业状态跟踪器

    _progressBar =
      if (_conf.get(UI_SHOW_CONSOLE_PROGRESS)) {
        Some(new ConsoleProgressBar(this)) // 创建控制台进度条
      } else {
        None
      }

    _ui =
      if (conf.get(UI_ENABLED)) {
        Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "",
          startTime)) // 创建Spark Web UI
      } else {
        // 测试环境下不启用UI
        None
      }
    // 在启动TaskScheduler之前绑定UI，以便将绑定端口正确传达给集群管理器
    _ui.foreach(_.bind())

    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf) // 创建Hadoop配置
    // 性能优化：调用.size()触发Configuration内部`properties`字段的预计算，
    // 确保在SessionState.newHadoopConf()使用`sc.hadoopConfiguration`创建
    // 新的per-session Configuration之前完成计算和缓存。
    // 如果未预先计算，每个新创建的Configuration都将执行昂贵的IO和XML解析来加载默认配置。
    // 通过预计算父配置的properties，子Configuration只需简单克隆父配置即可。
    _hadoopConfiguration.size()

    // 通过构造函数传入的JAR包逐个添加到SparkContext
    if (jars != null) {
      jars.foreach(jar => addJar(jar, true))
      if (allAddedJars.nonEmpty) {
        _conf.set("spark.app.initial.jar.urls", allAddedJars.keys.toSeq.mkString(","))
      }
    }

    // 添加用户指定的文件到SparkContext
    if (files != null) {
      files.foreach(file => addFile(file, false, true))
      if (allAddedFiles.nonEmpty) {
        _conf.set("spark.app.initial.file.urls", allAddedFiles.keys.toSeq.mkString(","))
      }
    }

    // 添加用户指定的归档文件到SparkContext
    if (archives != null) {
      archives.foreach(file => addFile(file, false, true, isArchive = true))
      if (allAddedArchives.nonEmpty) {
        _conf.set("spark.app.initial.archive.urls", allAddedArchives.keys.toSeq.mkString(","))
      }
    }

    _executorMemory = SparkContext.executorMemoryInMb(_conf) // 计算Executor内存大小（MB）

    // 将Java选项转换为环境变量作为变通方案，因为无法在sbt中直接设置环境变量
    for { (envKey, propKey) <- Seq(("SPARK_TESTING", IS_TESTING.key))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
      executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
      executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    executorEnvs ++= _conf.getExecutorEnv // 从配置中获取Executor环境变量
    executorEnvs("SPARK_USER") = sparkUser // 设置Spark用户名

    if (_conf.getOption("spark.executorEnv.OMP_NUM_THREADS").isEmpty) {
      // 如果未显式设置OMP_NUM_THREADS，使用spark.task.cpus的值覆盖
      // SPARK-41188: 限制OpenBLAS线程数为分配给此Executor的核心数，
      // 因为某些Spark ML算法通过netlib-java调用OpenBLAS
      // SPARK-28843: 限制OpenMP线程池大小为分配的核心数，
      // 避免pandas/numpy因大量OpenMP线程导致的高内存消耗
      executorEnvs.put("OMP_NUM_THREADS", _conf.get("spark.task.cpus", "1"))
    }

    // 必须在createTaskScheduler之前注册HeartbeatReceiver，
    // 因为Executor在构造函数中会获取HeartbeatReceiver（SPARK-6640）
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))

    // 在初始化TaskScheduler和ResourceProfileManager之前，先初始化插件
    _plugins = PluginContainer(this, _resources.asJava)
    _resourceProfileManager = new ResourceProfileManager(_conf, _listenerBus) // 创建资源配置管理器
    _env.initializeShuffleManager() // 初始化Shuffle管理器
    _env.initializeMemoryManager(SparkContext.numDriverCores(master, conf)) // 初始化内存管理器

    // 创建并启动调度器
    val (sched, ts) = SparkContext.createTaskScheduler(this, master) // 根据master URL创建对应的调度后端和任务调度器
    _schedulerBackend = sched
    _taskScheduler = ts
    _dagScheduler = new DAGScheduler(this) // 创建DAG调度器（负责将作业划分为Stage）
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet) // 通知HeartbeatReceiver任务调度器已就绪

    if (_conf.get(EXECUTOR_ALLOW_SYNC_LOG_LEVEL)) {
      _conf.get(SPARK_LOG_LEVEL)
        .foreach(logLevel => _schedulerBackend.updateExecutorsLogLevel(logLevel)) // 同步日志级别到所有Executor
    }

    _conf.get(CHECKPOINT_DIR).foreach(setCheckpointDir) // 设置检查点目录

    val _executorMetricsSource =
      if (_conf.get(METRICS_EXECUTORMETRICS_SOURCE_ENABLED)) {
        Some(new ExecutorMetricsSource) // 创建Executor指标源
      } else {
        None
      }

    // 创建并启动心跳发送器，用于收集内存指标
    _heartbeater = new Heartbeater(
      () => SparkContext.this.reportHeartBeat(_executorMetricsSource),
      "driver-heartbeater",
      conf.get(DRIVER_METRICS_POLLING_INTERVAL))
    _heartbeater.start()

    // 在DAGScheduler的构造函数中设置了对TaskScheduler的引用后，才启动TaskScheduler
    _taskScheduler.start()

    _applicationId = _taskScheduler.applicationId() // 从TaskScheduler获取应用ID
    _applicationAttemptId = _taskScheduler.applicationAttemptId() // 获取应用尝试ID
    _conf.set("spark.app.id", _applicationId)
    _applicationAttemptId.foreach { attemptId =>
      _conf.set(APP_ATTEMPT_ID, attemptId)
      _env.blockManager.blockStoreClient.setAppAttemptId(attemptId) // 设置BlockStore客户端的尝试ID
    }

    // 在应用ID和尝试ID初始化后，初始化Shuffle Driver组件
    _shuffleDriverComponents = ShuffleDataIOUtils.loadShuffleDataIO(_conf).driver()
    _shuffleDriverComponents.initializeApplication().asScala.foreach { case (k, v) =>
      _conf.set(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX + k, v) // 将Shuffle组件的配置写入SparkConf
    }

    if (_conf.get(UI_REVERSE_PROXY)) {
      val proxyUrl = _conf.get(UI_REVERSE_PROXY_URL).getOrElse("").stripSuffix("/")
      System.setProperty("spark.ui.proxyBase", proxyUrl + "/proxy/" + _applicationId) // 设置UI反向代理基础路径
    }
    _ui.foreach(_.setAppId(_applicationId)) // 为UI设置应用ID
    _env.blockManager.initialize(_applicationId) // 初始化BlockManager
    FallbackStorage.registerBlockManagerIfNeeded(
      _env.blockManager.master, _conf, _hadoopConfiguration) // 按需注册回退存储的BlockManager

    // Driver的指标系统需要spark.app.id，所以在获取到应用ID后才启动
    _env.metricsSystem.start(_conf.get(METRICS_STATIC_SOURCES_ENABLED))

    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration) // 创建事件日志监听器
        logger.start()
        listenerBus.addToEventLogQueue(logger) // 将事件日志监听器添加到事件日志队列
        Some(logger)
      } else {
        None
      }

    _cleaner =
      if (_conf.get(CLEANER_REFERENCE_TRACKING)) {
        Some(new ContextCleaner(this, _shuffleDriverComponents)) // 创建上下文清理器，用于清理不再使用的RDD/Shuffle/Broadcast
      } else {
        None
      }
    _cleaner.foreach(_.start()) // 启动清理器

    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf) // 检查是否启用了动态资源分配
    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        schedulerBackend match {
          case b: ExecutorAllocationClient =>
            // 创建Executor动态分配管理器，根据工作负载自动增减Executor数量
            Some(new ExecutorAllocationManager(
              schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf,
              cleaner = cleaner, resourceProfileManager = resourceProfileManager,
              reliableShuffleStorage = _shuffleDriverComponents.supportsReliableStorage()))
          case _ =>
            None
        }
      } else {
        None
      }
    _executorAllocationManager.foreach(_.start()) // 启动动态分配管理器

    setupAndStartListenerBus() // 设置并启动事件监听总线
    postEnvironmentUpdate() // 发布环境更新事件
    postApplicationStart() // 发布应用启动事件

    // 应用启动后，将处理器附加到已启动的服务器并启动处理器
    _ui.foreach(_.attachAllHandlers())
    // 在指标系统启动后，将Driver指标的servlet处理器附加到Web UI
    _env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))

    // 确保即使用户忘记关闭，SparkContext也会被停止。
    // 这避免了JVM正常退出后留下未完成的事件日志。但如果JVM被强制杀死，则无法保证。
    logDebug("Adding shutdown hook") // 强制创建logger的预加载
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
      logInfo("Invoking stop() from shutdown hook")
      try {
        stop() // 在JVM关闭时调用stop()
      } catch {
        case e: Throwable =>
          logWarning("Ignoring Exception while stopping SparkContext from shutdown hook", e)
      }
    }

    // 初始化后处理
    _taskScheduler.postStartHook()
    if (isLocal) {
      _env.metricsSystem.registerSource(Executor.executorSourceLocalModeOnly)
    }
    _env.metricsSystem.registerSource(_dagScheduler.metricsSource)
    _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
    _env.metricsSystem.registerSource(new JVMCPUSource())
    _executorMetricsSource.foreach(_.register(_env.metricsSystem))
    _executorAllocationManager.foreach { e =>
      _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
    }
    appStatusSource.foreach(_env.metricsSystem.registerSource(_))
    _plugins.foreach(_.registerMetrics(applicationId)) // 为插件注册指标

    // 设置调用者上下文，用于审计日志和HDFS操作的标识
    new CallerContext("DRIVER", config.get(APP_CALLER_CONTEXT),
      Some(applicationId), applicationAttemptId).setCurrentContext()
  } catch {
    case NonFatal(e) =>
      logError("Error initializing SparkContext.", e) // 初始化SparkContext时出错
      try {
        stop() // 尝试停止SparkContext以清理已分配的资源
      } catch {
        case NonFatal(inner) =>
          logError("Error stopping SparkContext after init error.", inner)
      } finally {
        throw e // 重新抛出原始异常
      }
  }

  /**
   * 由Web UI调用以获取Executor的线程转储信息。此方法可能开销较大。
   * 如果获取线程转储失败（可能由于Executor已死亡、无响应或网络问题），则记录错误并返回None。
   */
  private[spark] def getExecutorThreadDump(executorId: String): Option[Array[ThreadStackTrace]] = {
    try {
      if (executorId == SparkContext.DRIVER_IDENTIFIER) {
        Some(Utils.getThreadDump()) // Driver端直接获取本地线程转储
      } else {
        env.blockManager.master.getExecutorEndpointRef(executorId) match {
          case Some(endpointRef) =>
            Some(endpointRef.askSync[Array[ThreadStackTrace]](TriggerThreadDump)) // 通过RPC向Executor请求线程转储
          case None =>
            logWarning(log"Executor ${MDC(LogKeys.EXECUTOR_ID, executorId)} " +
              log"might already have stopped and can not request thread dump from it.")
            None
        }
      }
    } catch {
      case e: Exception =>
        logError(
          log"Exception getting thread dump from executor ${MDC(LogKeys.EXECUTOR_ID, executorId)}",
          e)
        None
    }
  }

  /** 获取指定任务的线程转储信息 */
  private[spark] def getTaskThreadDump(
      taskId: Long,
      executorId: String): Option[ThreadStackTrace] = {
    schedulerBackend.getTaskThreadDump(taskId, executorId)
  }

  /**
   * 由Web UI调用以获取Executor的堆内存直方图信息
   */
  private[spark] def getExecutorHeapHistogram(executorId: String): Option[Array[String]] = {
    try {
      if (executorId == SparkContext.DRIVER_IDENTIFIER) {
        Some(Utils.getHeapHistogram()) // Driver端直接获取本地堆直方图
      } else {
        env.blockManager.master.getExecutorEndpointRef(executorId) match {
          case Some(endpointRef) =>
            Some(endpointRef.askSync[Array[String]](TriggerHeapHistogram)) // 通过RPC向Executor请求堆直方图
          case None =>
            logWarning(log"Executor ${MDC(LogKeys.EXECUTOR_ID, executorId)} " +
              log"might already have stopped and can not request heap histogram from it.")
            None
        }
      }
    } catch {
      case e: Exception =>
        logError(
          log"Exception getting heap histogram from " +
            log"executor ${MDC(LogKeys.EXECUTOR_ID, executorId)}", e)
        None
    }
  }

  /** 获取当前线程的本地属性 */
  private[spark] def getLocalProperties: Properties = localProperties.get()

  /** 设置当前线程的本地属性 */
  private[spark] def setLocalProperties(props: Properties): Unit = {
    localProperties.set(props)
  }

  /**
   * 设置影响当前线程提交作业的本地属性，例如Spark公平调度器池。
   * 用户自定义属性也可在此设置。这些属性会传播到Worker任务中，
   * 可通过[[org.apache.spark.TaskContext#getLocalProperty]]访问。
   *
   * 这些属性会被从此线程派生的子线程继承。在使用线程池时可能产生意外后果：
   * Java标准线程池实现中Worker线程会派生其他Worker线程，
   * 因此本地属性可能以不可预测的方式传播。
   *
   * 要移除/取消属性，只需将`value`设为null，例如 sc.setLocalProperty("key", null)
   */
  def setLocalProperty(key: String, value: String): Unit = {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  /**
   * 获取当前线程中设置的本地属性，如果不存在则返回null。
   * 参见`org.apache.spark.SparkContext.setLocalProperty`。
   */
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  /** 设置当前作业的人类可读描述信息 */
  def setJobDescription(value: String): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**
   * 为当前线程启动的所有作业分配一个组ID，直到该组ID被设置为其他值或被清除。
   *
   * 通常，应用程序中的一个执行单元由多个Spark Action或Job组成。
   * 应用程序可使用此方法将所有这些作业分组并添加组描述。
   * 设置后，Spark Web UI会将这些作业关联到此组。
   *
   * 应用程序还可使用`org.apache.spark.SparkContext.cancelJobGroup`取消此组中所有运行的作业。
   * 例如：
   * {{{
   * // 在主线程中：
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // 在另一个线程中：
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   *
   * @param interruptOnCancel 如果为true，取消作业时会对作业的Executor线程调用`Thread.interrupt()`。
   *                          这有助于确保任务能及时停止，但默认关闭，因为HDFS-1208中HDFS
   *                          可能会将被中断的节点标记为死亡。
   */
  def setJobGroup(groupId: String,
      description: String, interruptOnCancel: Boolean = false): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
    // 注意：在setJobGroup中指定interruptOnCancel（而非cancelJobGroup），避免修改多个公共API，
    // 并允许cancelJobGroup API之外的Spark取消操作也能利用此属性（如内部作业失败或从JobProgressTab UI取消）
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /** 清除当前线程的作业组ID及其描述 */
  def clearJobGroup(): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
  }

  /**
   * 设置当前线程启动的作业在取消时的行为。
   *
   * @param interruptOnCancel 如果为true，取消作业时会对作业的Executor线程调用`Thread.interrupt()`。
   *                          这有助于确保任务能及时停止，但默认关闭，因为HDFS-1208中HDFS
   *                          可能会将被中断的节点标记为死亡。
   *
   * @since 3.5.0
   */
  def setInterruptOnCancel(interruptOnCancel: Boolean): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
  }

  /**
   * 为当前线程启动的所有作业添加一个标签。
   *
   * 通常，应用程序中的一个执行单元由多个Spark Action或Job组成。
   * 应用程序可使用此方法将所有这些作业分组并添加组标签。
   * 应用程序可使用`org.apache.spark.sql.SparkSession.interruptTag`取消所有带此标签的运行中执行。
   * 例如：
   * {{{
   * // 在主线程中：
   * sc.addJobTag("myjobs")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // 在另一个线程中：
   * spark.cancelJobsWithTag("myjobs")
   * }}}
   *
   * 同一时间可以存在多个标签，因此应用程序的不同部分可使用不同标签以不同粒度执行取消操作。
   *
   * @param tag 要添加的标签。不能包含','（逗号）字符。
   *
   * @since 3.5.0
   */
  def addJobTag(tag: String): Unit = addJobTags(Set(tag))

  /**
   * 为当前线程启动的所有作业添加多个标签。
   * 详见[[addJobTag]]。
   *
   * @param tags 要添加的标签集合。不能包含','（逗号）字符。
   *
   * @since 4.0.0
   */
  def addJobTags(tags: Set[String]): Unit = {
    tags.foreach(SparkContext.throwIfInvalidTag) // 校验标签合法性
    val existingTags = getJobTags()
    val newTags = (existingTags ++ tags).mkString(SparkContext.SPARK_JOB_TAGS_SEP) // 合并新旧标签
    setLocalProperty(SparkContext.SPARK_JOB_TAGS, newTags)
  }

  /**
   * 移除之前添加到当前线程所有作业的某个标签。
   * 如果该标签之前未添加过，则此操作无效。
   *
   * @param tag 要移除的标签。不能包含','（逗号）字符。
   *
   * @since 3.5.0
   */
  def removeJobTag(tag: String): Unit = removeJobTags(Set(tag))

  /**
   * 移除当前线程所有作业的多个标签。
   * 详见[[removeJobTag]]。
   *
   * @param tags 要移除的标签集合。不能包含','（逗号）字符。
   *
   * @since 4.0.0
   */
  def removeJobTags(tags: Set[String]): Unit = {
    tags.foreach(SparkContext.throwIfInvalidTag)
    val existingTags = getJobTags()
    val newTags = (existingTags -- tags).mkString(SparkContext.SPARK_JOB_TAGS_SEP)
    if (newTags.isEmpty) {
      clearJobTags()
    } else {
      setLocalProperty(SparkContext.SPARK_JOB_TAGS, newTags)
    }
  }

  /**
   * 获取当前设置为分配给此线程所有作业的标签集合。
   *
   * @since 3.5.0
   */
  def getJobTags(): Set[String] = {
    Option(getLocalProperty(SparkContext.SPARK_JOB_TAGS))
      .map(_.split(SparkContext.SPARK_JOB_TAGS_SEP).toSet)
      .getOrElse(Set())
      .filter(!_.isEmpty) // 防御性编程：过滤空字符串标签
  }

  /**
   * 清除当前线程的所有作业标签。
   *
   * @since 3.5.0
   */
  def clearJobTags(): Unit = {
    setLocalProperty(SparkContext.SPARK_JOB_TAGS, null)
  }

  /**
   * 在一个作用域内执行代码块，使得所有在此代码块中创建的新RDD都属于同一作用域。
   * 详见{{org.apache.spark.rdd.RDDOperationScope}}。
   *
   * @note 给定的代码块中不允许使用return语句。
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  // RDD创建方法

  /** 将本地Scala集合分发为RDD。
   *
   * @note parallelize是惰性操作。如果`seq`是可变集合，在调用parallelize之后、
   *       在RDD上执行第一个action之前修改了集合，生成的RDD将反映修改后的集合。
   *       传递参数的副本可避免此问题。
   * @note 避免使用`parallelize(Seq())`创建空RDD。考虑使用`emptyRDD`创建无分区的RDD，
   *       或使用`parallelize(Seq[T]())`创建带空分区的`T`类型RDD。
   * @param seq 要分发的Scala集合
   * @param numSlices 将集合划分的分区数
   * @return 代表分布式集合的RDD
   */
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /**
   * 创建一个新的RDD[Long]，包含从`start`到`end`（不含）的元素，每个元素递增`step`。
   *
   * @note 如果需要缓存此RDD，应确保每个分区不超过限制。
   *
   * @param start 起始值
   * @param end 结束值（不含）
   * @param step 递增步长
   * @param numSlices 将集合划分的分区数
   * @return 代表分布式范围的RDD
   */
  def range(
      start: Long,
      end: Long,
      step: Long = 1,
      numSlices: Int = defaultParallelism): RDD[Long] = withScope {
    assertNotStopped()
    // step为0时range会无限运行
    require(step != 0, "step cannot be 0")
    val numElements: BigInt = {
      val safeStart = BigInt(start)
      val safeEnd = BigInt(end)
      if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
        (safeEnd - safeStart) / step // 计算精确的元素数量
      } else {
        // 余数与范围方向相同时，需要多加1个元素
        (safeEnd - safeStart) / step + 1
      }
    }
    parallelize(0 until numSlices, numSlices).mapPartitionsWithIndex { (i, _) =>
      val partitionStart = (i * numElements) / numSlices * step + start // 计算当前分区的起始值
      val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start // 计算当前分区的结束值
      def getSafeMargin(bi: BigInt): Long =
        if (bi.isValidLong) {
          bi.toLong
        } else if (bi > 0) {
          Long.MaxValue // 超出Long范围时使用最大值
        } else {
          Long.MinValue
        }
      val safePartitionStart = getSafeMargin(partitionStart)
      val safePartitionEnd = getSafeMargin(partitionEnd)

      new Iterator[Long] {
        private[this] var number: Long = safePartitionStart
        private[this] var overflow: Boolean = false // 溢出标志

        override def hasNext =
          if (!overflow) {
            if (step > 0) {
              number < safePartitionEnd
            } else {
              number > safePartitionEnd
            }
          } else false

        override def next() = {
          val ret = number
          number += step
          if (number < ret ^ step < 0) {
            // Long.MaxValue + Long.MaxValue < Long.MaxValue 且
            // Long.MinValue + Long.MinValue > Long.MinValue，
            // 因此当step导致回退时，可以确定发生了溢出
            overflow = true
          }
          ret
        }
      }
    }
  }

  /** 将本地Scala集合分发为RDD。
   *
   * 此方法与`parallelize`完全相同。
   * @param seq 要分发的Scala集合
   * @param numSlices 将集合划分的分区数
   * @return 代表分布式集合的RDD
   */
  def makeRDD[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }

  /**
   * 将本地Scala集合分发为RDD，每个对象可指定一个或多个位置偏好（Spark节点主机名）。
   * 为每个集合元素创建一个新分区。
   * @param seq 数据和位置偏好（Spark节点主机名）的元组列表
   * @return 根据位置偏好分区的RDD
   */
  def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = withScope {
    assertNotStopped()
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap // 构建分区索引到位置偏好的映射
    new ParallelCollectionRDD[T](this, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
  }

  /**
   * 从HDFS、本地文件系统（所有节点可用）或任何Hadoop支持的文件系统URI读取文本文件，
   * 并以String类型的RDD返回。文本文件必须是UTF-8编码。
   *
   * @param path 支持的文件系统上文本文件的路径
   * @param minPartitions 建议的结果RDD最小分区数
   * @return 文本文件行组成的RDD
   */
  def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  /**
   * 从HDFS、本地文件系统或任何Hadoop支持的文件系统URI读取文本文件目录。
   * 每个文件作为单条记录读取，返回键值对RDD，其中键为文件路径，值为文件内容。
   * 文本文件必须是UTF-8编码。
   *
   * <p> 例如，如果有以下文件：
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * 执行 `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`，
   *
   * <p> 则`rdd`包含：
   * {{{
   *   (a-hdfs-path/part-00000, 其内容)
   *   (a-hdfs-path/part-00001, 其内容)
   *   ...
   *   (a-hdfs-path/part-nnnnn, 其内容)
   * }}}
   *
   * @note 适合小文件，大文件也允许但可能导致性能不佳。
   * @note 在某些文件系统上，`.../path/&#42;`比`.../path/`或`.../path`更高效地读取目录中的所有文件。
   * @note 分区由数据本地性决定，默认情况下可能导致分区数过少。
   *
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @param minPartitions 输入数据最小切分数的建议值
   * @return 表示文件路径和对应文件内容元组的RDD
   */
  def wholeTextFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, String)] = withScope {
    assertNotStopped()
    val job = NewHadoopJob.getInstance(hadoopConfiguration)
    // 使用setInputPaths使wholeTextFiles与hadoopFile/textFile保持一致，
    // 支持逗号分隔的文件作为输入（参见SPARK-7155）
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new WholeTextFileRDD(
      this,
      classOf[WholeTextFileInputFormat],
      classOf[Text],
      classOf[Text],
      updateConf,
      minPartitions).map(record => (record._1.toString, record._2.toString)).setName(path)
  }

  /**
   * 获取Hadoop可读数据集的RDD，每个文件作为PortableDataStream返回（适用于二进制数据）。
   *
   * 例如，如果有以下文件：
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * 执行 `val rdd = sparkContext.binaryFiles("hdfs://a-hdfs-path")`，
   *
   * 则`rdd`包含：
   * {{{
   *   (a-hdfs-path/part-00000, 其内容)
   *   (a-hdfs-path/part-00001, 其内容)
   *   ...
   *   (a-hdfs-path/part-nnnnn, 其内容)
   * }}}
   *
   * @note 适合小文件；非常大的文件可能导致性能不佳。
   * @note 在某些文件系统上，`.../path/&#42;`比`.../path/`或`.../path`更高效地读取目录中的所有文件。
   * @note 分区由数据本地性决定，默认情况下可能导致分区数过少。
   *
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @param minPartitions 输入数据最小切分数的建议值
   * @return 表示文件路径和对应文件内容元组的RDD
   */
  def binaryFiles(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[(String, PortableDataStream)] = withScope {
    assertNotStopped()
    val job = NewHadoopJob.getInstance(hadoopConfiguration)
    // 使用setInputPaths使binaryFiles与hadoopFile/textFile保持一致，
    // 支持逗号分隔的文件作为输入（参见SPARK-7155）
    NewFileInputFormat.setInputPaths(job, path)
    val updateConf = job.getConfiguration
    new BinaryFileRDD(
      this,
      classOf[StreamInputFormat],
      classOf[String],
      classOf[PortableDataStream],
      updateConf,
      minPartitions).setName(path)
  }

  /**
   * 从扁平二进制文件加载数据，假设每条记录的长度是固定的。
   *
   * @note 确保结果RDD中每条记录的字节数组具有指定的记录长度。
   *
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @param recordLength 切分记录的长度
   * @param conf 用于设置数据集的Configuration
   *
   * @return 值为字节数组的数据RDD
   */
  def binaryRecords(
      path: String,
      recordLength: Int,
      conf: Configuration = hadoopConfiguration): RDD[Array[Byte]] = withScope {
    assertNotStopped()
    conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
    val br = newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](path,
      classOf[FixedLengthBinaryInputFormat],
      classOf[LongWritable],
      classOf[BytesWritable],
      conf = conf)
    br.map { case (k, v) =>
      val bytes = v.copyBytes()
      assert(bytes.length == recordLength, "Byte array does not have correct length")
      bytes
    }
  }

  /**
   * 使用旧版MapReduce API（`org.apache.hadoop.mapred`），
   * 通过Hadoop JobConf获取Hadoop可读数据集的RDD，需要指定InputFormat和其他必要信息
   * （如文件系统数据集的文件名，HyperTable的表名）。
   *
   * @param conf 用于设置数据集的JobConf。注意：此配置会被放入Broadcast中，
   *             因此如果计划复用此conf创建多个RDD，需要确保不会修改conf。
   *             安全做法是每次创建新RDD时总是创建新的conf。
   * @param inputFormatClass 要读取数据的存储格式
   * @param keyClass 与`inputFormatClass`参数关联的键的`Class`
   * @param valueClass 与`inputFormatClass`参数关联的值的`Class`
   * @param minPartitions 要生成的最小Hadoop Split数量
   * @return 键值元组的RDD
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或直接将其传递给聚合或Shuffle操作会创建对同一对象的多个引用。
   *       如果计划直接缓存、排序或聚合Hadoop writable对象，应先使用`map`函数复制它们。
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // 这是一个强制加载hdfs-site.xml的hack方式，参见SPARK-11227
    FileSystem.getLocal(conf)

    // 在广播JobConf之前添加必要的安全凭证
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /** 获取具有任意InputFormat的Hadoop文件的RDD
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或直接将其传递给聚合或Shuffle操作会创建对同一对象的多个引用。
   *       如果计划直接缓存、排序或聚合Hadoop writable对象，应先使用`map`函数复制它们。
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @param inputFormatClass 要读取数据的存储格式
   * @param keyClass 与`inputFormatClass`参数关联的键的`Class`
   * @param valueClass 与`inputFormatClass`参数关联的值的`Class`
   * @param minPartitions 建议的结果RDD最小分区数
   * @return 键值元组的RDD
   */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KiB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
   * }}}
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  /**
   * hadoopFile()的隐式类型版本，使用ClassTag自动推断键、值和InputFormat的类型，
   * 用户无需直接传递。调用者只需写：
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @param minPartitions 建议的结果RDD最小分区数
   * @return 键值元组的RDD
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]]
      (path: String, minPartitions: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile(path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]],
      minPartitions)
  }

  /**
   * hadoopFile()的更智能版本，使用ClassTag自动推断键、值和InputFormat的类型。
   * 使用默认最小分区数。
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @return 键值元组的RDD
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    hadoopFile[K, V, F](path, defaultMinPartitions)
  }

  /**
   * `newApiHadoopFile`的更智能版本，使用ClassTag自动推断键、值和
   * `org.apache.hadoop.mapreduce.InputFormat`（新MapReduce API）的类型。
   * 调用者只需写：
   * ```
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * ```
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @return 键值元组的RDD
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
      (path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
    newAPIHadoopFile(
      path,
      fm.runtimeClass.asInstanceOf[Class[F]],
      km.runtimeClass.asInstanceOf[Class[K]],
      vm.runtimeClass.asInstanceOf[Class[V]])
  }

  /**
   * 使用新版API InputFormat获取给定Hadoop文件的RDD，并传递额外配置选项给InputFormat。
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，路径可以是逗号分隔的多个输入路径
   * @param fClass 要读取数据的存储格式
   * @param kClass 与`fClass`参数关联的键的`Class`
   * @param vClass 与`fClass`参数关联的值的`Class`
   * @param conf Hadoop配置
   * @return 键值元组的RDD
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = withScope {
    assertNotStopped()

    // 强制加载hdfs-site.xml的hack方式，参见SPARK-11227
    FileSystem.getLocal(hadoopConfiguration)

    // 调用NewHadoopJob会自动将安全凭证添加到conf，无需显式添加
    val job = NewHadoopJob.getInstance(conf)
    // 使用setInputPaths使newAPIHadoopFile与hadoopFile/textFile保持一致，
    // 支持逗号分隔的文件输入（参见SPARK-7155）
    NewFileInputFormat.setInputPaths(job, path)
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * 使用新版API InputFormat获取给定Hadoop文件的RDD，并传递额外配置选项给InputFormat。
   *
   * @param conf 用于设置数据集的Configuration。注意：此配置会被放入Broadcast中，
   *             因此如果计划复用此conf创建多个RDD，需要确保不会修改conf。
   *             安全做法是每次创建新RDD时总是创建新的conf。
   * @param fClass 要读取数据的存储格式
   * @param kClass 与`fClass`参数关联的键的`Class`
   * @param vClass 与`fClass`参数关联的值的`Class`
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration = hadoopConfiguration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()

    // 强制加载hdfs-site.xml的hack方式，参见SPARK-11227
    FileSystem.getLocal(conf)

    // 将必要的安全凭证添加到JobConf，访问安全HDFS所需
    val jconf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jconf)
    new NewHadoopRDD(this, fClass, kClass, vClass, jconf)
  }

  /**
   * 获取具有给定键值类型的Hadoop SequenceFile的RDD。
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，可以是逗号分隔的多个路径
   * @param keyClass 与`SequenceFileInputFormat`关联的键的`Class`
   * @param valueClass 与`SequenceFileInputFormat`关联的值的`Class`
   * @param minPartitions 建议的结果RDD最小分区数
   * @return 键值元组的RDD
   */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
      ): RDD[(K, V)] = withScope {
    assertNotStopped()
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
  }

  /**
   * 获取具有给定键值类型的Hadoop SequenceFile的RDD（使用默认最小分区数）。
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，可以是逗号分隔的多个路径
   * @param keyClass 与`SequenceFileInputFormat`关联的键的`Class`
   * @param valueClass 与`SequenceFileInputFormat`关联的值的`Class`
   * @return 键值元组的RDD
   */
  def sequenceFile[K, V](
      path: String,
      keyClass: Class[K],
      valueClass: Class[V]): RDD[(K, V)] = withScope {
    assertNotStopped()
    sequenceFile(path, keyClass, valueClass, defaultMinPartitions)
  }

  /**
   * sequenceFile()的版本，用于可通过WritableConverter隐式转换为Writable的类型。
   * 例如，访问键为Text、值为IntWritable的SequenceFile，只需：
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *
   * WritableConverter以隐式函数的方式提供（而非隐式对象），以同时支持Writable子类
   * 和我们定义了转换器的类型（如Int到IntWritable）。最自然的做法是为转换器提供
   * 隐式对象，但由于不能为每个Writable子类提供参数化的单例对象，所以使用函数
   * 来创建适当类型的新转换器。此外，我们将ClassTag传递给转换器，以允许它在子类情况下
   * 确定要使用的Writable类。
   *
   * @note 由于Hadoop的RecordReader类会为每条记录复用同一个Writable对象，
   *       直接缓存返回的RDD或传递给聚合/Shuffle操作会创建对同一对象的多个引用。
   *       应先使用`map`函数复制。
   * @param path 输入数据文件的目录，可以是逗号分隔的多个路径
   * @param minPartitions 建议的结果RDD最小分区数
   * @return 键值元组的RDD
   */
   def sequenceFile[K, V]
       (path: String, minPartitions: Int = defaultMinPartitions)
       (implicit km: ClassTag[K], vm: ClassTag[V],
        kcf: () => WritableConverter[K], vcf: () => WritableConverter[V]): RDD[(K, V)] = {
    withScope {
      assertNotStopped()
      val kc = clean(kcf)() // 清理并创建键转换器
      val vc = clean(vcf)() // 清理并创建值转换器
      val format = classOf[SequenceFileInputFormat[Writable, Writable]]
      val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
      writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) } // 将Writable转换为目标类型
    }
  }

  /**
   * 加载以SequenceFile格式保存的包含序列化对象的RDD，
   * 使用NullWritable键和包含序列化分区的BytesWritable值。
   * 这仍是实验性的存储格式，未来Spark版本可能不完全支持。
   * 使用默认序列化器（Java序列化）时会比较慢，但其优势在于保存任意对象几乎不需要额外工作。
   *
   * @param path 输入数据文件的目录，可以是逗号分隔的多个路径
   * @param minPartitions 建议的结果RDD最小分区数
   * @return 表示从文件反序列化数据的RDD
   */
  def objectFile[T: ClassTag](
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[T] = withScope {
    assertNotStopped()
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
  }

  /** 从检查点路径创建可靠的检查点RDD */
  protected[spark] def checkpointFile[T: ClassTag](path: String): RDD[T] = withScope {
    new ReliableCheckpointRDD[T](this, path)
  }

  /** 构建一个RDD列表的并集 */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = withScope {
    val nonEmptyRdds = rdds.filter(!_.partitions.isEmpty) // 过滤掉空RDD
    val partitioners = nonEmptyRdds.flatMap(_.partitioner).toSet
    if (nonEmptyRdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
      new PartitionerAwareUnionRDD(this, nonEmptyRdds) // 所有RDD使用相同分区器时，使用分区感知的Union
    } else {
      new UnionRDD(this, nonEmptyRdds) // 否则使用普通Union
    }
  }

  /** 构建变长参数传入的RDD列表的并集 */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] = withScope {
    union(Seq(first) ++ rest)
  }

  /** 获取一个没有分区和元素的空RDD */
  def emptyRDD[T: ClassTag]: RDD[T] = new EmptyRDD[T](this)

  // 创建共享变量的方法

  /**
   * 注册给定的累加器。
   *
   * @note 累加器必须在使用前注册，否则会抛出异常。
   */
  def register(acc: AccumulatorV2[_, _]): Unit = {
    acc.register(this)
  }

  /**
   * 使用给定名称注册累加器。
   *
   * @note 累加器必须在使用前注册，否则会抛出异常。
   */
  def register(acc: AccumulatorV2[_, _], name: String): Unit = {
    acc.register(this, name = Option(name))
  }

  /** 创建并注册一个Long类型累加器，初始值为0，通过`add`累加输入 */
  def longAccumulator: LongAccumulator = {
    val acc = new LongAccumulator
    register(acc)
    acc
  }

  /** 创建并注册一个带名称的Long类型累加器，初始值为0，通过`add`累加输入 */
  def longAccumulator(name: String): LongAccumulator = {
    val acc = new LongAccumulator
    register(acc, name)
    acc
  }

  /** 创建并注册一个Double类型累加器，初始值为0，通过`add`累加输入 */
  def doubleAccumulator: DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc)
    acc
  }

  /** 创建并注册一个带名称的Double类型累加器，初始值为0，通过`add`累加输入 */
  def doubleAccumulator(name: String): DoubleAccumulator = {
    val acc = new DoubleAccumulator
    register(acc, name)
    acc
  }

  /** 创建并注册一个`CollectionAccumulator`，初始为空列表，通过添加输入到列表中来累加 */
  def collectionAccumulator[T]: CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc)
    acc
  }

  /** 创建并注册一个带名称的`CollectionAccumulator`，初始为空列表 */
  def collectionAccumulator[T](name: String): CollectionAccumulator[T] = {
    val acc = new CollectionAccumulator[T]
    register(acc, name)
    acc
  }

  /**
   * 将只读变量广播到集群，返回一个[[org.apache.spark.broadcast.Broadcast]]对象，
   * 用于在分布式函数中读取。该变量只会发送到每个Executor一次。
   *
   * @param value 要广播到Spark节点的值
   * @return `Broadcast`对象，缓存在每台机器上的只读变量
   */
  def broadcast[T: ClassTag](value: T): Broadcast[T] = {
    broadcastInternal(value, serializedOnly = false)
  }

  /**
   * broadcast的内部版本——将只读变量广播到集群，返回Broadcast对象。
   *
   * @param value 要广播到Spark节点的值
   * @param serializedOnly 如果为true，不在Driver端缓存未序列化的值
   * @return `Broadcast`对象，缓存在每台机器上的只读变量
   */
  private[spark] def broadcastInternal[T: ClassTag](
      value: T,
      serializedOnly: Boolean): Broadcast[T] = {
    assertNotStopped()
    require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
      "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.") // 不能直接广播RDD
    val bc = env.broadcastManager.newBroadcast[T](value, isLocal, serializedOnly) // 通过广播管理器创建新广播变量
    val callSite = getCallSite()
    logInfo(log"Created broadcast ${MDC(LogKeys.BROADCAST_ID, bc.id)}" +
      log" from ${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}")
    cleaner.foreach(_.registerBroadcastForCleanup(bc)) // 注册到清理器以便后续清理
    bc
  }

  /**
   * 添加一个文件，使其随Spark作业一起下载到每个节点。
   *
   * 如果在执行过程中添加文件，在下一个TaskSet开始之前该文件不可用。
   *
   * @param path 可以是本地文件、HDFS（或其他Hadoop支持的文件系统）中的文件，
   *             或HTTP/HTTPS/FTP URI。在Spark作业中使用`SparkFiles.get(fileName)`访问下载位置。
   *
   * @note 路径只能添加一次。后续相同路径的添加将被忽略。
   */
  def addFile(path: String): Unit = {
    addFile(path, false, false)
  }

  /** 返回已添加到资源中的文件路径列表 */
  def listFiles(): Seq[String] = allAddedFiles.keySet.toSeq

  /**
   * :: 实验性 ::
   * 添加一个归档文件，使其随Spark作业一起下载并解压到每个节点。
   *
   * 如果在执行过程中添加归档，在下一个TaskSet开始之前该归档不可用。
   *
   * @param path 可以是本地文件、HDFS（或其他Hadoop支持的文件系统）中的文件，
   *             或HTTP/HTTPS/FTP URI。在Spark作业中使用`SparkFiles.get(paths-to-files)`访问下载/解压位置。
   *             路径应为 .zip, .tar, .tar.gz, .tgz 或 .jar 格式之一。
   *
   * @note 路径只能添加一次。后续相同路径的添加将被忽略。
   *
   * @since 3.1.0
   */
  @Experimental
  def addArchive(path: String): Unit = {
    addFile(path, false, false, isArchive = true)
  }

  /**
   * :: 实验性 ::
   * 返回已添加到资源中的归档路径列表。
   *
   * @since 3.1.0
   */
  @Experimental
  def listArchives(): Seq[String] = allAddedArchives.keySet.toSeq

  /**
   * 添加一个文件，使其随Spark作业一起下载到每个节点。
   *
   * 如果在执行过程中添加文件，在下一个TaskSet开始之前该文件不可用。
   *
   * @param path 可以是本地文件、HDFS（或其他Hadoop支持的文件系统）中的文件，
   *             或HTTP/HTTPS/FTP URI。
   * @param recursive 如果为true，`path`中可以传入目录。目前目录仅支持Hadoop支持的文件系统。
   *
   * @note 路径只能添加一次。后续相同路径的添加将被忽略。
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    addFile(path, recursive, false)
  }

  /** 添加文件或归档到SparkContext的内部方法 */
  private def addFile(
      path: String, recursive: Boolean, addedOnSubmit: Boolean, isArchive: Boolean = false
    ): Unit = {
    val jobArtifactUUID = JobArtifactSet
      .getCurrentJobArtifactState.map(_.uuid).getOrElse("default") // 获取当前作业的Artifact UUID
    val uri = Utils.resolveURI(path)
    val schemeCorrectedURI = uri.getScheme match {
      case null => new File(path).getCanonicalFile.toURI // 无scheme时转为本地文件URI
      case "local" =>
        logWarning(log"File with 'local' scheme ${MDC(LogKeys.PATH, path)} " +
          log"is not supported to add to file server, " +
          log"since it is already available on every node.") // local scheme的文件无需添加
        return
      case _ => uri
    }

    val hadoopPath = new Path(schemeCorrectedURI)
    val scheme = schemeCorrectedURI.getScheme
    if (!Array("http", "https", "ftp", "spark").contains(scheme) && !isArchive) {
      val fs = hadoopPath.getFileSystem(hadoopConfiguration)
      val isDir = fs.getFileStatus(hadoopPath).isDirectory
      if (!isLocal && scheme == "file" && isDir) {
        throw SparkCoreErrors.addLocalDirectoryError(hadoopPath) // 非本地模式不允许添加本地目录
      }
      if (!recursive && isDir) {
        throw SparkCoreErrors.addDirectoryError(hadoopPath) // 非递归模式不允许添加目录
      }
    } else {
      // SPARK-17650: 在添加到依赖列表之前确保URL有效
      Utils.validateURL(uri)
    }

    val key = if (!isLocal && scheme == "file") {
      env.rpcEnv.fileServer.addFile(new File(uri.getPath)) // 非本地模式将文件添加到RPC文件服务器
    } else if (uri.getScheme == null) {
      schemeCorrectedURI.toString
    } else {
      uri.toString
    }

    val timestamp = if (addedOnSubmit) startTime else System.currentTimeMillis
    // 如果会话ID来自SparkSession，说明是Spark Connect客户端。
    // 为Spark Connect客户端指定专用目录。
    lazy val root = if (jobArtifactUUID != "default") {
      val newDest = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
      newDest.mkdir()
      newDest
    } else {
      new File(SparkFiles.getRootDirectory())
    }
    if (
      !isArchive &&
        addedFiles
          .getOrElseUpdate(jobArtifactUUID, new ConcurrentHashMap[String, Long]().asScala)
          .putIfAbsent(key, timestamp).isEmpty) {
      logInfo(log"Added file ${MDC(LogKeys.PATH, path)} at ${MDC(LogKeys.KEY, key)} with" +
        log" timestamp ${MDC(LogKeys.TIMESTAMP, timestamp)}")
      // 将文件下载到本地，使得在Driver上运行的闭包仍可通过SparkFiles API访问文件
      Utils.fetchFile(uri.toString, root, conf, hadoopConfiguration, timestamp, useCache = false)
      postEnvironmentUpdate() // 发布环境更新事件
    } else if (
      isArchive &&
        addedArchives
          .getOrElseUpdate(jobArtifactUUID, new ConcurrentHashMap[String, Long]().asScala)
          .putIfAbsent(
          Utils.getUriBuilder(new URI(key)).fragment(uri.getFragment).build().toString,
          timestamp).isEmpty) {
      logInfo(log"Added archive ${MDC(LogKeys.PATH, path)} at ${MDC(LogKeys.KEY, key)}" +
        log" with timestamp ${MDC(LogKeys.TIMESTAMP, timestamp)}")
      // 如果scheme为file，使用URI直接复制而非下载
      val uriToUse = if (!isLocal && scheme == "file") uri else new URI(key)
      val uriToDownload = Utils.getUriBuilder(uriToUse).fragment(null).build()
      val source = Utils.fetchFile(uriToDownload.toString, Utils.createTempDir(), conf,
        hadoopConfiguration, timestamp, useCache = false, shouldUntar = false)
      val dest = new File(
        root,
        if (uri.getFragment != null) uri.getFragment else source.getName)
      logInfo(
        log"Unpacking an archive ${MDC(LogKeys.PATH, path)}" +
          log" (${MDC(LogKeys.BYTE_SIZE, source.length)} bytes)" +
          log" from ${MDC(LogKeys.SOURCE_PATH, source.getAbsolutePath)}" +
          log" to ${MDC(LogKeys.DESTINATION_PATH, dest.getAbsolutePath)}")
      Utils.deleteRecursively(dest) // 删除目标目录以确保解压干净
      Utils.unpack(source, dest) // 解压归档文件
      postEnvironmentUpdate()
    } else {
      logWarning(log"The path ${MDC(LogKeys.PATH, path)} " +
        log"has been added already. Overwriting of added paths " +
        log"is not supported in the current version.") // 路径已添加，当前版本不支持覆盖
    }
  }

  /**
   * :: 开发者API ::
   * 注册一个监听器，接收执行过程中发生的事件的回调通知。
   */
  @DeveloperApi
  def addSparkListener(listener: SparkListenerInterface): Unit = {
    listenerBus.addToSharedQueue(listener)
  }

  /**
   * :: 开发者API ::
   * 从Spark的监听器总线中取消注册监听器。
   */
  @DeveloperApi
  def removeSparkListener(listener: SparkListenerInterface): Unit = {
    listenerBus.removeListener(listener)
  }

  /** 获取当前所有Executor的ID列表 */
  private[spark] def getExecutorIds(): Seq[String] = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.getExecutorIds()
      case _ =>
        logWarning("Requesting executors is not supported by current scheduler.") // 当前调度器不支持请求Executor
        Nil
    }
  }

  /**
   * 根据给定的ResourceProfile获取当前可以并发启动的最大任务数，
   * 即使某些资源正在使用中也会计算在内。
   * 注意：不要缓存此方法的返回值，因为添加/移除Executor会导致数值变化。
   *
   * @param rp 用于计算最大并发任务数的ResourceProfile
   * @return 当前可以并发启动的最大任务数
   */
  private[spark] def maxNumConcurrentTasks(rp: ResourceProfile): Int = {
    schedulerBackend.maxNumConcurrentTasks(rp)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions. This applies to the default ResourceProfile.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  @DeveloperApi
  def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: immutable.Map[String, Int]
    ): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        // this is being applied to the default resource profile, would need to add api to support
        // others
        val defaultProfId = resourceProfileManager.defaultResourceProfile.id
        b.requestTotalExecutors(immutable.Map(defaultProfId-> numExecutors),
          immutable.Map(localityAwareTasks -> defaultProfId),
          immutable.Map(defaultProfId -> hostToLocalTaskCount))
      case _ =>
        logWarning("Requesting executors is not supported by current scheduler.")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is received.
   */
  @DeveloperApi
  def requestExecutors(numAdditionalExecutors: Int): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.requestExecutors(numAdditionalExecutors)
      case _ =>
        logWarning("Requesting executors is not supported by current scheduler.")
        false
    }
  }

  /**
   * :: DeveloperApi ::
   * Request that the cluster manager kill the specified executors.
   *
   * This is not supported when dynamic allocation is turned on.
   *
   * @note 这是向集群管理器表明应用程序希望减少资源使用。如果应用程序希望用新的Executor替换
   *       通过此方法杀死的Executor，应随后显式调用{{SparkContext#requestExecutors}}。
   *
   * @return 请求是否被接收
   */
  @DeveloperApi
  def killExecutors(executorIds: Seq[String]): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        require(executorAllocationManager.isEmpty,
          "killExecutors() unsupported with Dynamic Allocation turned on") // 开启动态分配时不支持
        b.killExecutors(executorIds, adjustTargetNumExecutors = true, countFailures = false,
          force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is not supported by current scheduler.")
        false
    }
  }

  /**
   * :: 开发者API ::
   * 请求集群管理器杀死指定的Executor。
   *
   * @note 这是向集群管理器表明应用程序希望减少资源使用。
   *
   * @return 请求是否被接收
   */
  @DeveloperApi
  def killExecutor(executorId: String): Boolean = killExecutors(Seq(executorId))

  /**
   * 请求集群管理器杀死指定的Executor，但不调整应用程序资源需求。
   * 效果是会启动一个新的Executor来替代被杀死的Executor。
   * 这假设集群管理器会自动且最终满足所有缺失的应用程序资源请求。
   *
   * @note 替换不能保证一定发生；同一集群上的其他应用程序可能在此期间抢占资源。
   *
   * @return 请求是否被接收
   */
  private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
    schedulerBackend match {
      case b: ExecutorAllocationClient =>
        b.killExecutors(Seq(executorId), adjustTargetNumExecutors = false, countFailures = true,
          force = true).nonEmpty
      case _ =>
        logWarning("Killing executors is not supported by current scheduler.")
        false
    }
  }

  /** 当前应用程序运行的Spark版本 */
  def version: String = SPARK_VERSION

  /**
   * 返回从BlockManager到最大可用缓存内存和剩余可用缓存内存的映射。
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    assertNotStopped()
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * :: 开发者API ::
   * 返回已缓存RDD的信息，包括是否在内存或磁盘中、占用空间等。
   */
  @DeveloperApi
  def getRDDStorageInfo: Array[RDDInfo] = {
    getRDDStorageInfo(_ => true)
  }

  /** 获取符合过滤条件的已缓存RDD的存储信息 */
  private[spark] def getRDDStorageInfo(filter: RDD[_] => Boolean): Array[RDDInfo] = {
    assertNotStopped()
    val rddInfos = persistentRdds.values.filter(filter).map(RDDInfo.fromRdd).toArray
    rddInfos.foreach { rddInfo =>
      val rddId = rddInfo.id
      val rddStorageInfo = statusStore.asOption(statusStore.rdd(rddId))
      rddInfo.numCachedPartitions = rddStorageInfo.map(_.numCachedPartitions).getOrElse(0) // 已缓存分区数
      rddInfo.memSize = rddStorageInfo.map(_.memoryUsed).getOrElse(0L) // 内存占用
      rddInfo.diskSize = rddStorageInfo.map(_.diskUsed).getOrElse(0L) // 磁盘占用
    }
    rddInfos.filter(_.isCached)
  }

  /**
   * 返回通过cache()调用标记为持久化的RDD的不可变映射。
   *
   * @note 这并不一定意味着缓存或计算已成功完成。
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  /**
   * :: 开发者API ::
   * 返回公平调度器的调度池列表
   */
  @DeveloperApi
  def getAllPools: Seq[Schedulable] = {
    assertNotStopped()
    taskScheduler.rootPool.schedulableQueue.asScala.toSeq
  }

  /**
   * :: 开发者API ::
   * 返回与给定名称关联的调度池（如果存在）
   */
  @DeveloperApi
  def getPoolForName(pool: String): Option[Schedulable] = {
    assertNotStopped()
    Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
  }

  /** 返回当前调度模式 */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    assertNotStopped()
    taskScheduler.schedulingMode
  }

  /**
   * 获取特定RDD中分区的位置偏好信息
   * @param rdd 目标RDD
   * @param partition 要查询位置偏好的分区
   * @return 分区的首选位置列表
   */
  private [spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    dagScheduler.getPreferredLocs(rdd, partition)
  }

  /** 注册一个RDD以持久化到内存和/或磁盘存储中 */
  private[spark] def persistRDD(rdd: RDD[_]): Unit = {
    persistentRdds(rdd.id) = rdd
  }

  /** 从内存和/或磁盘存储中取消持久化RDD */
  private[spark] def unpersistRDD(rddId: Int, blocking: Boolean): Unit = {
    env.blockManager.master.removeRdd(rddId, blocking) // 从BlockManager中移除RDD
    persistentRdds.remove(rddId) // 从持久化RDD追踪表中移除
    listenerBus.post(SparkListenerUnpersistRDD(rddId)) // 发布取消持久化事件
  }

  /**
   * 为此`SparkContext`上将来执行的所有任务添加JAR依赖。
   *
   * 如果在执行过程中添加JAR，在下一个TaskSet开始之前该JAR不可用。
   *
   * @param path 可以是本地文件、HDFS（或其他Hadoop支持的文件系统）中的文件，
   *             HTTP/HTTPS/FTP URI，或local:/path表示每个Worker节点上的文件。
   *
   * @note 路径只能添加一次。后续相同路径的添加将被忽略。
   */
  def addJar(path: String): Unit = {
    addJar(path, false)
  }

  /** 添加JAR到SparkContext的内部方法 */
  private def addJar(path: String, addedOnSubmit: Boolean): Unit = {
    val jobArtifactUUID = JobArtifactSet
      .getCurrentJobArtifactState.map(_.uuid).getOrElse("default")
    /** 添加本地JAR文件到RPC文件服务器 */
    def addLocalJarFile(file: File): Seq[String] = {
      try {
        if (!file.exists()) {
          throw new FileNotFoundException(s"Jar ${file.getAbsolutePath} not found")
        }
        if (file.isDirectory) {
          throw new IllegalArgumentException(
            s"Directory ${file.getAbsoluteFile} is not allowed for addJar")
        }

        Seq(env.rpcEnv.fileServer.addJar(file))
      } catch {
        case NonFatal(e) =>
          logError(log"Failed to add ${MDC(LogKeys.PATH, path)} to Spark environment", e)
          Nil
      }
    }

    /** 检查远程JAR文件是否存在且有效 */
    def checkRemoteJarFile(path: String): Seq[String] = {
      val hadoopPath = new Path(path)
      val scheme = hadoopPath.toUri.getScheme
      if (!Array("http", "https", "ftp", "spark").contains(scheme)) {
        try {
          val fs = hadoopPath.getFileSystem(hadoopConfiguration)
          if (!fs.exists(hadoopPath)) {
            throw new FileNotFoundException(s"Jar ${path} not found")
          }
          if (fs.getFileStatus(hadoopPath).isDirectory) {
            throw new IllegalArgumentException(
              s"Directory ${path} is not allowed for addJar")
          }
          Seq(path)
        } catch {
          case NonFatal(e) =>
            logError(log"Failed to add ${MDC(LogKeys.PATH, path)} to Spark environment", e)
            Nil
        }
      } else {
        Seq(path)
      }
    }

    if (path == null || path.isEmpty) {
      logWarning("null or empty path specified as parameter to addJar")
    } else {
      val (keys, scheme) = if (path.contains("\\") && Utils.isWindows) {
        // For local paths with backslashes on Windows, URI throws an exception
        (addLocalJarFile(new File(path)), "local")
      } else {
        val uri = Utils.resolveURI(path)
        // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
        Utils.validateURL(uri)
        val uriScheme = uri.getScheme
        val jarPaths = uriScheme match {
          // A JAR file which exists only on the driver node
          case null =>
            // SPARK-22585 path without schema is not url encoded
            addLocalJarFile(new File(uri.getPath))
          // A JAR file which exists only on the driver node
          case "file" => addLocalJarFile(new File(uri.getPath))
          // A JAR file which exists locally on every worker node
          case "local" => Seq("file:" + uri.getPath)
          case "ivy" =>
            // Since `new Path(path).toUri` will lose query information,
            // so here we use `URI.create(path)`
            DependencyUtils.resolveMavenDependencies(URI.create(path))
              .flatMap(jar => addLocalJarFile(new File(jar)))
          case _ => checkRemoteJarFile(path)
        }
        (jarPaths, uriScheme)
      }
      if (keys.nonEmpty) {
        val timestamp = if (addedOnSubmit) startTime else System.currentTimeMillis
        val (added, existed) = keys.partition(addedJars
          .getOrElseUpdate(jobArtifactUUID, new ConcurrentHashMap[String, Long]().asScala)
          .putIfAbsent(_, timestamp).isEmpty)
        if (added.nonEmpty) {
          val jarMessage = if (scheme != "ivy") {
            log"Added JAR"
          } else {
            log"Added dependency jars of Ivy URI"
          }
          logInfo(jarMessage + log" ${MDC(LogKeys.PATH, path)}" +
            log" at ${MDC(LogKeys.ADDED_JARS, added.mkString(","))}" +
            log" with timestamp ${MDC(LogKeys.TIMESTAMP, timestamp)}")
          postEnvironmentUpdate()
        }
        if (existed.nonEmpty) {
          val jarMessage = if (scheme != "ivy") "JAR" else "dependency jars of Ivy URI"
          logWarning(log"The ${MDC(LogKeys.JAR_MESSAGE, jarMessage)} ${MDC(LogKeys.PATH, path)} " +
            log"at ${MDC(LogKeys.EXISTING_PATH, existed.mkString(","))} has been added already." +
            log" Overwriting of added jar is not supported in the current version.")
        }
      }
    }
  }

  /** 返回已添加到资源中的JAR文件路径列表 */
  def listJars(): Seq[String] = allAddedJars.keySet.toSeq

  /**
   * 在Spark内部组件中停止SparkContext时，容易因等待内部线程完成而导致死锁。
   * 建议使用此方法在新线程中停止SparkContext以避免死锁。
   */
  private[spark] def stopInNewThread(): Unit = {
    new Thread("stop-spark-context") {
      setDaemon(true)

      override def run(): Unit = {
        try {
          SparkContext.this.stop()
        } catch {
          case e: Throwable =>
            logError(e.getMessage, e)
            throw e
        }
      }
    }.start()
  }

  /** 关闭SparkContext */
  def stop(): Unit = stop(0)

  /**
   * 关闭SparkContext并传递退出码给调度后端。
   * 在Client模式下，客户端可能调用`SparkContext.stop()`进行清理但使用非0的退出码退出。
   * 这种行为会导致资源调度器（如ApplicationMaster）以成功状态退出，
   * 但客户端却以失败状态退出。Spark可调用此方法停止SparkContext并将正确的退出码传递给
   * 调度后端，然后调度后端将退出码发送给相应的资源调度器以保持一致。
   *
   * @param exitCode 在Client模式下传递给调度后端的退出码
   */
  def stop(exitCode: Int): Unit = {
    stopSite = Some(getCallSite()) // 记录stop被调用的位置
    logInfo(log"SparkContext is stopping with exitCode ${MDC(LogKeys.EXIT_CODE, exitCode)}" +
      log" from ${MDC(LogKeys.STOP_SITE_SHORT_FORM, stopSite.get.shortForm)}.")
    if (LiveListenerBus.withinListenerThread.value) {
      throw new SparkException(s"Cannot stop SparkContext within listener bus thread.") // 不允许在监听器线程中停止
    }
    // 使用stopped变量确保stop操作不会产生竞争
    if (!stopped.compareAndSet(false, true)) {
      logInfo("SparkContext already stopped.")
      return
    }
    if (_shutdownHookRef != null) {
      ShutdownHookManager.removeShutdownHook(_shutdownHookRef) // 移除关闭钩子
    }

    if (listenerBus != null) {
      Utils.tryLogNonFatalError {
        postApplicationEnd(exitCode) // 发布应用结束事件
      }
    }
    Utils.tryLogNonFatalError {
      _driverLogger.foreach(_.stop()) // 停止Driver日志记录器
    }
    Utils.tryLogNonFatalError {
      _ui.foreach(_.stop()) // 停止Web UI
    }
    Utils.tryLogNonFatalError {
      _cleaner.foreach(_.stop()) // 停止上下文清理器
    }
    Utils.tryLogNonFatalError {
      _executorAllocationManager.foreach(_.stop()) // 停止动态资源分配管理器
    }
    if (_dagScheduler != null) {
      Utils.tryLogNonFatalError {
        _dagScheduler.stop(exitCode) // 停止DAG调度器
      }
      _dagScheduler = null
    }
    // 为防止插件关闭期间仍有事件发布，在停止listenerBus之前先关闭每个插件
    Utils.tryLogNonFatalError {
      _plugins.foreach(_.shutdown()) // 关闭所有插件
    }
    if (_listenerBusStarted) {
      Utils.tryLogNonFatalError {
        listenerBus.stop() // 停止事件监听总线
        _listenerBusStarted = false
      }
    }
    if (env != null) {
      Utils.tryLogNonFatalError {
        env.metricsSystem.report() // 最后一次上报指标
      }
    }
    Utils.tryLogNonFatalError {
      FallbackStorage.cleanUp(_conf, _hadoopConfiguration) // 清理回退存储
    }
    Utils.tryLogNonFatalError {
      _eventLogger.foreach(_.stop()) // 停止事件日志记录器
    }
    if (_shuffleDriverComponents != null) {
      Utils.tryLogNonFatalError {
        _shuffleDriverComponents.cleanupApplication() // 清理Shuffle驱动组件
      }
    }
    if (_heartbeater != null) {
      Utils.tryLogNonFatalError {
        _heartbeater.stop() // 停止心跳发送器
      }
      _heartbeater = null
    }
    if (env != null && _heartbeatReceiver != null) {
      Utils.tryLogNonFatalError {
        env.rpcEnv.stop(_heartbeatReceiver) // 停止心跳接收器RPC端点
      }
    }
    Utils.tryLogNonFatalError {
      _progressBar.foreach(_.stop()) // 停止控制台进度条
    }
    _taskScheduler = null
    if (_env != null) {
      Utils.tryLogNonFatalError {
        _env.stop() // 停止SparkEnv（包含RpcEnv、BlockManager等）
      }
      SparkEnv.set(null)
    }
    if (_statusStore != null) {
      _statusStore.close() // 关闭应用状态存储
    }
    // 清除此InheritableThreadLocal，否则即使SparkContext已停止，子线程仍会继承它
    localProperties.remove()
    ResourceProfile.clearDefaultProfile()
    // 清除YARN模式系统环境变量，允许在集群类型之间切换
    SparkContext.clearActiveContext()
    logInfo(log"Successfully stopped SparkContext (Uptime: " +
      log"${MDC(LogKeys.TOTAL_TIME, System.currentTimeMillis() - startTime)} ms)")
  }


  /**
   * 从构造函数设置的值、spark.home Java属性或SPARK_HOME环境变量中
   * 获取Spark的安装位置（按优先级顺序）。如果都未设置，返回None。
   */
  private[spark] def getSparkHome(): Option[String] = {
    conf.getOption("spark.home").orElse(Option(System.getenv("SPARK_HOME")))
  }

  /** 设置线程局部属性，用于覆盖Action和RDD的调用位置信息 */
  def setCallSite(shortCallSite: String): Unit = {
    setLocalProperty(CallSite.SHORT_FORM, shortCallSite)
  }

  /** 设置线程局部属性，用于覆盖Action和RDD的调用位置信息（完整版） */
  private[spark] def setCallSite(callSite: CallSite): Unit = {
    setLocalProperty(CallSite.SHORT_FORM, callSite.shortForm)
    setLocalProperty(CallSite.LONG_FORM, callSite.longForm)
  }

  /** 清除线程局部属性中覆盖的Action和RDD调用位置信息 */
  def clearCallSite(): Unit = {
    setLocalProperty(CallSite.SHORT_FORM, null)
    setLocalProperty(CallSite.LONG_FORM, null)
  }

  /**
   * 捕获当前用户调用位置并返回格式化版本用于打印。
   * 如果用户通过`setCallSite()`覆盖了调用位置，则返回用户的版本。
   */
  private[spark] def getCallSite(): CallSite = {
    lazy val callSite = Utils.getCallSite()
    CallSite(
      Option(getLocalProperty(CallSite.SHORT_FORM)).getOrElse(callSite.shortForm),
      Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse(callSite.longForm)
    )
  }

  /**
   * 在RDD的指定分区集上运行函数，并将结果传递给给定的处理函数。
   * 这是Spark中所有Action的主入口点。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param func 要在RDD的每个分区上运行的函数
   * @param partitions 要运行的分区集；某些作业可能不需要计算目标RDD的所有分区，如`first()`
   * @param resultHandler 用于传递每个结果的回调函数
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite()
    val cleanedFunc = clean(func) // 清理闭包，移除不必要的外部引用
    logInfo(log"Starting job: ${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}")
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo(log"RDD's recursive dependencies:\n" +
        log"${MDC(LogKeys.RDD_DEBUG_STRING, rdd.toDebugString)}") // 记录RDD的完整血缘关系
    }
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get) // 提交作业到DAG调度器
    progressBar.foreach(_.finishAll()) // 完成进度条显示
    rdd.doCheckpoint() // 如果RDD设置了检查点，在作业完成后执行
  }

  /**
   * 在RDD的指定分区集上运行函数，以数组形式返回结果。
   * 每个分区运行的函数还接受`TaskContext`参数。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param func 要在RDD的每个分区上运行的函数
   * @param partitions 要运行的分区集
   * @return 内存中的结果集合（每个集合元素包含一个分区的结果）
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  /**
   * 在RDD的指定分区集上运行函数，以数组形式返回结果。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param func 要在RDD的每个分区上运行的函数
   * @param partitions 要运行的分区集
   * @return 内存中的结果集合（每个集合元素包含一个分区的结果）
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int]): Array[U] = {
    val cleanedFunc = clean(func)
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
  }

  /**
   * 在RDD的所有分区上运行函数，以数组形式返回结果。
   * 每个分区运行的函数还接受`TaskContext`参数。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param func 要在RDD的每个分区上运行的函数
   * @return 内存中的结果集合（每个集合元素包含一个分区的结果）
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, rdd.partitions.indices)
  }

  /**
   * 在RDD的所有分区上运行函数，以数组形式返回结果。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param func 要在RDD的每个分区上运行的函数
   * @return 内存中的结果集合（每个集合元素包含一个分区的结果）
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, rdd.partitions.indices)
  }

  /**
   * 在RDD的所有分区上运行函数，将结果传递给处理函数。
   * 每个分区运行的函数还接受`TaskContext`参数。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param processPartition 要在RDD的每个分区上运行的函数
   * @param resultHandler 用于传递每个结果的回调函数
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit): Unit = {
    runJob[T, U](rdd, processPartition, rdd.partitions.indices, resultHandler)
  }

  /**
   * 在RDD的所有分区上运行函数，将结果传递给处理函数。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param processPartition 要在RDD的每个分区上运行的函数
   * @param resultHandler 用于传递每个结果的回调函数
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit): Unit = {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, rdd.partitions.indices, resultHandler)
  }

  /**
   * :: 开发者API ::
   * 运行一个可以返回近似结果的作业。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param func 要在RDD的每个分区上运行的函数
   * @param evaluator 接收部分结果的`ApproximateEvaluator`
   * @param timeout 等待作业的最大时间（毫秒）
   * @return 部分结果（结果的完整程度取决于作业是在超时前还是超时后完成的）
   */
  @DeveloperApi
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
    assertNotStopped()
    val callSite = getCallSite()
    logInfo(log"Starting job: ${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}")
    val start = System.currentTimeMillis()
    val cleanedFunc = clean(func)
    val result = dagScheduler.runApproximateJob(rdd, cleanedFunc, evaluator, callSite, timeout,
      localProperties.get)
    logInfo(
      log"Job finished: ${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}," +
        log" took ${MDC(LogKeys.TOTAL_TIME, System.currentTimeMillis() - start)}ms")
    result
  }

  /**
   * 提交作业执行，返回持有结果的FutureJob。
   *
   * @param rdd 要在其上运行任务的目标RDD
   * @param processPartition 要在RDD的每个分区上运行的函数
   * @param partitions 要运行的分区集
   * @param resultHandler 用于传递每个结果的回调函数
   * @param resultFunc 结果就绪时要执行的函数
   */
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    assertNotStopped()
    val cleanF = clean(processPartition)
    val callSite = getCallSite()
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }

  /**
   * 提交Map阶段执行。目前仅为内部API，未来可能提升为DeveloperApi。
   */
  private[spark] def submitMapStage[K, V, C](dependency: ShuffleDependency[K, V, C])
      : SimpleFutureAction[MapOutputStatistics] = {
    assertNotStopped()
    val callSite = getCallSite()
    var result: MapOutputStatistics = null
    val waiter = dagScheduler.submitMapStage(
      dependency,
      (r: MapOutputStatistics) => { result = r },
      callSite,
      localProperties.get)
    new SimpleFutureAction[MapOutputStatistics](waiter, result)
  }

  /**
   * 取消指定组的活跃作业。参见`org.apache.spark.SparkContext.setJobGroup`。
   *
   * @param groupId 要取消的组ID
   * @param reason 取消原因
   *
   * @since 4.0.0
   */
  def cancelJobGroup(groupId: String, reason: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = false, Option(reason))
  }

  /**
   * 取消指定组的活跃作业。参见`org.apache.spark.SparkContext.setJobGroup`。
   *
   * @param groupId 要取消的组ID
   */
  def cancelJobGroup(groupId: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = false, None)
  }

  /**
   * 取消指定组的活跃作业及此作业组中的未来作业。
   * 注意：可跟踪的作业组最大数量由'spark.scheduler.numCancelledJobGroupsToTrack'设置。
   * 达到限制后添加新的作业组时，最旧的被跟踪作业组将被丢弃。
   *
   * @param groupId 要取消的组ID
   * @param reason 取消原因
   *
   * @since 4.0.0
   */
  def cancelJobGroupAndFutureJobs(groupId: String, reason: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = true, Option(reason))
  }

  /**
   * 取消指定组的活跃作业及此作业组中的未来作业。
   *
   * @param groupId 要取消的组ID
   */
  def cancelJobGroupAndFutureJobs(groupId: String): Unit = {
    assertNotStopped()
    dagScheduler.cancelJobGroup(groupId, cancelFutureJobs = true, None)
  }

  /**
   * 取消具有指定标签的活跃作业（内部方法，返回Future）。
   * 参见`org.apache.spark.SparkContext.addJobTag`。
   *
   * @param tag 要取消的标签。不能包含','（逗号）字符。
   * @param reason 取消原因
   * @return 包含[[ActiveJob]]列表的Future，可提取作业ID和标签等信息
   */
  private[spark] def cancelJobsWithTagWithFuture(
      tag: String,
      reason: String): Future[Seq[ActiveJob]] = {
    SparkContext.throwIfInvalidTag(tag)
    assertNotStopped()

    val cancelledJobs = Promise[Seq[ActiveJob]]()
    dagScheduler.cancelJobsWithTag(tag, Some(reason), Some(cancelledJobs))
    cancelledJobs.future
  }

  /**
   * 取消具有指定标签的活跃作业。参见`org.apache.spark.SparkContext.addJobTag`。
   *
   * @param tag 要取消的标签。不能包含','（逗号）字符。
   * @param reason 取消原因
   *
   * @since 4.0.0
   */
  def cancelJobsWithTag(tag: String, reason: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    assertNotStopped()
    dagScheduler.cancelJobsWithTag(tag, Option(reason), cancelledJobs = None)
  }

  /**
   * 取消具有指定标签的活跃作业。参见`org.apache.spark.SparkContext.addJobTag`。
   *
   * @param tag 要取消的标签。不能包含','（逗号）字符。
   *
   * @since 3.5.0
   */
  def cancelJobsWithTag(tag: String): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    assertNotStopped()
    dagScheduler.cancelJobsWithTag(tag, reason = None, cancelledJobs = None)
  }

  /** 取消所有已调度或正在运行的作业 */
  def cancelAllJobs(): Unit = {
    assertNotStopped()
    dagScheduler.cancelAllJobs()
  }

  /**
   * 取消指定的作业（如果它已调度或正在运行）。
   *
   * @param jobId 要取消的作业ID
   * @param reason 取消原因
   * @note 如果无法发送取消消息，将抛出`InterruptedException`
   */
  def cancelJob(jobId: Int, reason: String): Unit = {
    dagScheduler.cancelJob(jobId, Option(reason))
  }

  /**
   * 取消指定的作业（如果它已调度或正在运行）。
   *
   * @param jobId 要取消的作业ID
   * @note 如果无法发送取消消息，将抛出`InterruptedException`
   */
  def cancelJob(jobId: Int): Unit = {
    dagScheduler.cancelJob(jobId, None)
  }

  /**
   * 取消给定的Stage及其关联的所有作业。
   *
   * @param stageId 要取消的Stage ID
   * @param reason 取消原因
   * @note 如果无法发送取消消息，将抛出`InterruptedException`
   */
  def cancelStage(stageId: Int, reason: String): Unit = {
    dagScheduler.cancelStage(stageId, Option(reason))
  }

  /**
   * Cancel a given stage and all jobs associated with it.
   *
   * @param stageId the stage ID to cancel
   * @note Throws `InterruptedException` if the cancel message cannot be sent
   */
  def cancelStage(stageId: Int): Unit = {
    dagScheduler.cancelStage(stageId, None)
  }

  /**
   * Kill and reschedule the given task attempt. Task ids can be obtained from the Spark UI
   * or through SparkListener.onTaskStart.
   *
   * @param taskId the task ID to kill. This id uniquely identifies the task attempt.
   * @param interruptThread whether to interrupt the thread running the task.
   * @param reason the reason for killing the task, which should be a short string. If a task
   *   is killed multiple times with different reasons, only one reason will be reported.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean = true,
      reason: String = "killed via SparkContext.killTaskAttempt"): Boolean = {
    dagScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Clean a closure to make it ready to be serialized and sent to tasks
   * (removes unreferenced variables in $outer's, updates REPL variables)
   * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
   * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
   * if not.
   *
   * @param f the closure to clean
   * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
   * @throws SparkException if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
   *   serializable
   * @return the cleaned closure
   */
  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    SparkClosureCleaner.clean(f, checkSerializable)
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed.
   * @param directory path to the directory where checkpoint files will be stored
   * (must be HDFS path if running in cluster)
   */
  /**
   * 设置RDD检查点的目录。此目录中的RDD数据将被保存到可靠存储中以实现容错。
   *
   * 如果在集群上运行，且目录为本地路径则记录警告。
   * 因为Driver可能尝试从自己的本地文件系统重建检查点RDD，
   * 但检查点文件实际上在Executor机器上。
   */
  def setCheckpointDir(directory: String): Unit = {

    if (!isLocal && Utils.nonLocalPaths(directory).isEmpty) {
      logWarning(log"Spark is not running in local mode, therefore the checkpoint directory " +
        log"must not be on the local filesystem. Directory '${MDC(LogKeys.PATH, directory)}' " +
        log"appears to be on the local filesystem.")
    }

    checkpointDir = Option(directory).map { dir =>
      val path = new Path(dir, UUID.randomUUID().toString) // 使用UUID创建唯一子目录
      val fs = path.getFileSystem(hadoopConfiguration)
      fs.mkdirs(path) // 创建目录
      fs.getFileStatus(path).getPath.toString
    }
  }

  /** 获取检查点目录 */
  def getCheckpointDir: Option[String] = checkpointDir

  /** 用户未指定时的默认并行度（如parallelize和makeRDD） */
  def defaultParallelism: Int = {
    assertNotStopped()
    taskScheduler.defaultParallelism()
  }

  /**
   * 用户未指定时Hadoop RDD的默认最小分区数。
   * 使用math.min使defaultMinPartitions不超过2。
   * 对于大文件，Hadoop InputFormat库总会创建更多分区（即使默认值为2）。
   * 对于小文件，快速处理是有益的。当Spark将小表与大表join时，
   * 大部分时间仍花在大表的map阶段。
   */
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

  private val nextShuffleId = new AtomicInteger(0)

  /** 生成新的Shuffle ID */
  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private val nextRddId = new AtomicInteger(0)

  /** 注册新RDD，返回其RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /**
   * 注册spark.extraListeners中指定的监听器，然后启动监听器总线。
   * 应在所有内部监听器注册完毕后调用（例如Web UI和事件日志监听器注册之后）。
   */
  private def setupAndStartListenerBus(): Unit = {
    try {
      conf.get(EXTRA_LISTENERS).foreach { classNames =>
        val listeners = Utils.loadExtensions(classOf[SparkListenerInterface], classNames, conf)
        listeners.foreach { listener =>
          listenerBus.addToSharedQueue(listener) // 将额外监听器添加到共享队列
          logInfo(log"Registered listener" +
            log"${MDC(LogKeys.CLASS_NAME, listener.getClass().getName())}")
        }
      }
    } catch {
      case e: Exception =>
        try {
          stop()
        } finally {
          throw new SparkException(s"Exception when registering SparkListener", e)
        }
    }

    listenerBus.start(this, _env.metricsSystem) // 启动监听器总线
    _listenerBusStarted = true
  }

  /** 发布应用启动事件 */
  private def postApplicationStart(): Unit = {
    // 注意：此代码假设TaskScheduler已初始化并已联系集群管理器获取应用ID
    listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),
      startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls,
      schedulerBackend.getDriverAttributes))
    _driverLogger.foreach(_.startSync(_hadoopConfiguration)) // 开始同步Driver日志
  }

  /** 发布应用结束事件并上报最终心跳 */
  private def postApplicationEnd(exitCode: Int): Unit = {
    try {
      _heartbeater.doReportHeartbeat() // 最后一次上报心跳指标
    } catch {
      case t: Throwable =>
        logInfo("Unable to report driver heartbeat metrics when stopping spark context", t);
    }
    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis, Some(exitCode)))
  }

  /** TaskScheduler就绪后发布环境更新事件 */
  private[spark] def postEnvironmentUpdate(): Unit = {
    if (taskScheduler != null) {
      val schedulingMode = getSchedulingMode.toString
      val addedJarPaths = allAddedJars.keys.toSeq
      val addedFilePaths = allAddedFiles.keys.toSeq
      val addedArchivePaths = allAddedArchives.keys.toSeq
      val environmentDetails = SparkEnv.environmentDetails(conf, hadoopConfiguration,
        schedulingMode, addedJarPaths, addedFilePaths, addedArchivePaths,
        env.metricsSystem.metricsProperties().asScala.toMap)
      val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
      listenerBus.post(environmentUpdate)
    }
  }

  /** 上报Driver的心跳指标 */
  private def reportHeartBeat(executorMetricsSource: Option[ExecutorMetricsSource]): Unit = {
    val currentMetrics = ExecutorMetrics.getCurrentMetrics(env.memoryManager)
    executorMetricsSource.foreach(_.updateMetricsSnapshot(currentMetrics))

    val driverUpdates = new HashMap[(Int, Int), ExecutorMetrics]
    // 在Driver中不跟踪per-stage指标，所以使用虚拟Stage作为键
    driverUpdates.put(EventLoggingListener.DRIVER_STAGE_KEY, new ExecutorMetrics(currentMetrics))
    val accumUpdates = new Array[(Long, Int, Int, Seq[AccumulableInfo])](0).toImmutableArraySeq
    listenerBus.post(SparkListenerExecutorMetricsUpdate("driver", accumUpdates,
      driverUpdates))
  }

  // 为防止多个SparkContext同时处于活跃状态，标记此上下文已完成构造。
  // 注意：此代码必须放在SparkContext构造函数的最末尾。
  SparkContext.setActiveContext(this)
}

/**
 * SparkContext伴生对象，包含许多隐式转换和参数，用于支持各种Spark功能。
 * 同时负责SparkContext的单例生命周期管理（创建、激活、清理）。
 */
object SparkContext extends Logging {
  // 合法的日志级别集合
  private[spark] val VALID_LOG_LEVELS =
    Set("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")

  /**
   * 用于保护跟踪SparkContext构造过程的全局变量的同步锁。
   */
  private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

  /**
   * 当前活跃的、已完全构造完成的SparkContext。若无活跃上下文则为null。
   * 对该字段的访问由SPARK_CONTEXT_CONSTRUCTOR_LOCK保护。
   */
  private val activeContext: AtomicReference[SparkContext] =
    new AtomicReference[SparkContext](null)

  /**
   * 指向正在构造中的SparkContext（若有其他线程正在SparkContext构造函数中），
   * 若没有正在构造的SparkContext则为None。
   * 对该字段的访问由SPARK_CONTEXT_CONSTRUCTOR_LOCK保护。
   */
  private var contextBeingConstructed: Option[SparkContext] = None

  /**
   * 确保当前JVM中没有其他SparkContext正在运行。
   * 若检测到已运行的上下文则抛出异常；若另一个线程正在构造SparkContext则记录警告。
   */
  private def assertNoOtherContextIsRunning(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      // 检查是否已有活跃的SparkContext（且不是当前这个）
      Option(activeContext.get()).filter(_ ne sc).foreach { ctx =>
          val errMsg = "Only one SparkContext should be running in this JVM (see SPARK-2243)." +
            s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
          throw new SparkException(errMsg)
        }

      // 检查是否有其他线程正在构造SparkContext
      contextBeingConstructed.filter(_ ne sc).foreach { otherContext =>
        // 由于otherContext可能指向部分构造的上下文，需防范其creationSite字段为null
        val otherContextCreationSite =
          Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
        val warnMsg = log"Another SparkContext is being constructed (or threw an exception in its" +
          log" constructor). This may indicate an error, since only one SparkContext should be" +
          log" running in this JVM (see SPARK-2243)." +
          log" The other SparkContext was created at:\n" +
          log"${MDC(LogKeys.CREATION_SITE, otherContextCreationSite)}"
        logWarning(warnMsg)
      }
    }
  }

  /**
   * 确保SparkContext仅在Driver端创建或访问。
   * 若在Executor的任务执行中尝试创建SparkContext则抛出异常。
   */
  private def assertOnDriver(): Unit = {
    if (Utils.isInRunningSparkTask) {
      throw new IllegalStateException(
        "SparkContext should only be created and accessed on the driver.")
    }
  }

  /**
   * 获取或创建SparkContext并注册为单例对象。
   * 由于每个JVM只能有一个活跃的SparkContext，当应用需要共享SparkContext时非常有用。
   *
   * @param config 用于初始化SparkContext的SparkConf配置
   * @return 当前活跃的SparkContext（若之前不存在则新建一个）
   */
  def getOrCreate(config: SparkConf): SparkContext = {
    // 加同步锁，防止多个创建请求同时触发assertNoOtherContextIsRunning异常
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext(config)) // 不存在则创建新的SparkContext
      } else {
        if (config.getAll.nonEmpty) {
          logWarning("Using an existing SparkContext; some configuration may not take effect.")
        }
      }
      activeContext.get()
    }
  }

  /**
   * 获取或创建SparkContext并注册为单例对象（无参版本，适用于仅获取已有上下文的场景）。
   *
   * @return 当前活跃的SparkContext（若之前不存在则使用默认配置新建一个）
   */
  def getOrCreate(): SparkContext = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      if (activeContext.get() == null) {
        setActiveContext(new SparkContext())
      }
      activeContext.get()
    }
  }

  /** 返回当前活跃的SparkContext（如果有的话） */
  private[spark] def getActive: Option[SparkContext] = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      Option(activeContext.get())
    }
  }

  /**
   * 在SparkContext构造函数开始时调用，标记当前SparkContext正在构造中。
   * 若检测到已有运行中的上下文则抛出异常，若有其他线程正在构造则记录警告。
   */
  private[spark] def markPartiallyConstructed(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc)
      contextBeingConstructed = Some(sc) // 标记为正在构造中
    }
  }

  /**
   * 在SparkContext构造函数结束时调用，将其设置为活跃上下文。
   * 同时确保没有其他SparkContext与此构造函数竞争。
   */
  private[spark] def setActiveContext(sc: SparkContext): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      assertNoOtherContextIsRunning(sc)
      contextBeingConstructed = None // 清除正在构造的标记
      activeContext.set(sc) // 设置为活跃上下文
    }
  }

  /**
   * 清除活跃SparkContext的元数据。由SparkContext#stop()调用。
   * 在单元测试中也会调用，以防止未正确清理SparkContext的测试套件产生大量警告。
   */
  private[spark] def clearActiveContext(): Unit = {
    SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
      activeContext.set(null)
    }
  }

  // ---- 本地属性和作业控制相关的配置键常量 ----
  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"
  private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel"
  private[spark] val SPARK_JOB_TAGS = "spark.job.tags"
  private[spark] val SPARK_SCHEDULER_POOL = "spark.scheduler.pool"
  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"
  private[spark] val SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"

  /**
   * Driver的Executor标识符。早期版本中使用`<driver>`，
   * 后改为`driver`以避免尖括号在URL和XML中引起的转义问题（见SPARK-6716）。
   */
  private[spark] val DRIVER_IDENTIFIER = "driver"

  /** 作业标签在SPARK_JOB_TAGS属性中的分隔符 */
  private[spark] val SPARK_JOB_TAGS_SEP = ","

  // 校验作业标签是否合法（同样的规则也适用于Spark Connect执行标签）
  private[spark] def throwIfInvalidTag(tag: String) = {
    if (tag == null) {
      throw new IllegalArgumentException("Spark job tag cannot be null.")
    }
    if (tag.contains(SPARK_JOB_TAGS_SEP)) {
      throw new IllegalArgumentException(
        s"Spark job tag cannot contain '$SPARK_JOB_TAGS_SEP'.")
    }
    if (tag.isEmpty) {
      throw new IllegalArgumentException(
        "Spark job tag cannot be an empty string.")
    }
  }

  /**
   * 查找给定类所在的JAR包路径，方便用户将其JAR传递给SparkContext。
   *
   * @param cls 应位于JAR内的类
   * @return 包含该类的JAR路径，若未找到则返回None
   */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI格式为 "jar:file:/path/foo.jar!/package/cls.class"，从中提取/path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * 查找包含给定对象所属类的JAR包。在Driver程序中通常调用jarOfObject(this)即可。
   *
   * @param obj 其所属类应在JAR内的实例引用
   * @return 包含该实例所属类的JAR路径，若未找到则返回None
   */
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

  /**
   * 基于可单独传递给SparkContext的参数，创建SparkConf的修改副本。
   * 忽略默认值为null的参数（不会像SparkConf那样抛出异常），简化SparkContext构造函数的编写。
   */
  private[spark] def updatedConf(
      conf: SparkConf,
      master: String,
      appName: String,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()): SparkConf =
  {
    val res = conf.clone()
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome != null) {
      res.setSparkHome(sparkHome)
    }
    if (jars != null && !jars.isEmpty) {
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }

  /** 获取Driver可用的CPU核心数（用于Netty I/O等任务），单参数重载版本 */
  private[spark] def numDriverCores(master: String): Int = {
    numDriverCores(master, null)
  }

  /**
   * 根据master URL和配置，计算Driver可用的CPU核心数。
   * local模式下根据线程数确定，YARN/K8s cluster模式下从配置读取。
   */
  private[spark] def numDriverCores(master: String, conf: SparkConf): Int = {
    // 将线程字符串转为整数，"*"表示使用所有可用处理器
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }
    master match {
      case "local" => 1 // local模式只有1个核心
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads) // local[N]模式
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads) // local[N,M]模式
      case "yarn" | SparkMasterRegex.KUBERNETES_REGEX(_) =>
        if (conf != null && conf.get(SUBMIT_DEPLOY_MODE) == "cluster") {
          conf.getInt(DRIVER_CORES.key, 0) // cluster模式从配置获取Driver核心数
        } else {
          0
        }
      case _ => 0 // 其他情况：Driver未被使用或核心数将在后续确定
    }
  }

  /** 从配置或环境变量中获取Executor内存大小（MB），默认1024MB */
  private[spark] def executorMemoryInMb(conf: SparkConf): Int = {
    conf.getOption(EXECUTOR_MEMORY.key)
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM"))
      .map(warnSparkMem))
      .map(Utils.memoryStringToMb)
      .getOrElse(1024)
  }

  // 警告用户SPARK_MEM环境变量已弃用，应使用spark.executor.memory配置
  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    value
  }

  /**
   * 根据给定的master URL创建任务调度器。
   * 返回调度后端（SchedulerBackend）和任务调度器（TaskScheduler）的二元组。
   * 这是Spark调度系统的核心工厂方法，根据不同的部署模式创建对应的调度器实现。
   */
  private def createTaskScheduler(
      sc: SparkContext,
      master: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // 本地运行时，不重试失败的任务
    val MAX_LOCAL_TASK_FAILURES = 1

    // 确保默认Executor的资源满足一个或多个任务的需求。
    // 此函数用于不设置executor cores配置的集群管理器，其他集群管理器在ResourceProfile中检查。
    def checkResourcesPerTask(executorCores: Int): Unit = {
      val taskCores = sc.conf.get(CPUS_PER_TASK)
      if (!sc.conf.get(SKIP_VALIDATE_CORES_TESTING)) {
        validateTaskCpusLargeEnough(sc.conf, executorCores, taskCores)
      }
      val defaultProf = sc.resourceProfileManager.defaultResourceProfile
      ResourceUtils.warnOnWastedResources(defaultProf, sc.conf, Some(executorCores))
    }

    // 根据master URL模式匹配，创建对应的调度后端和任务调度器
    master match {
      case "local" =>
        // local模式：单线程本地执行
        checkResourcesPerTask(1)
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_REGEX(threads) =>
        // local[N]或local[*]模式：多线程本地执行
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*]使用机器上所有可用核心数；local[N]使用恰好N个线程
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        if (threadCount <= 0) {
          throw new SparkException(s"Asked to run locally with $threadCount threads")
        }
        checkResourcesPerTask(threadCount)
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        // local[N,M]模式：多线程本地执行，允许M次任务失败（用于测试）
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        checkResourcesPerTask(threadCount)
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case SPARK_REGEX(sparkUrl) =>
        // spark://模式：Standalone集群部署
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _) // 支持多Master HA
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_CLUSTER_REGEX(numWorkers, coresPerWorker, memoryPerWorker) =>
        // local-cluster[N,cores,memory]模式：本地模拟Spark集群（用于测试）
        checkResourcesPerTask(coresPerWorker.toInt)
        // 检查请求的Executor内存不超过每个Worker的内存，否则Spark会卡住
        val memoryPerWorkerInt = memoryPerWorker.toInt
        if (sc.executorMemory > memoryPerWorkerInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MiB/worker but requested %d MiB/executor".format(
              memoryPerWorkerInt, sc.executorMemory))
        }

        // 对于本地集群模式，默认禁用主机本地磁盘读取。
        // 因为该模式用于测试，所有Executor运行在同一台主机上，
        // 如果启用主机本地读取，则大多数单元测试都需要显式禁用它才能测试远程拉取。
        sc.conf.setIfMissing(SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED, false)

        val scheduler = new TaskSchedulerImpl(sc)
        val localCluster = LocalSparkCluster(
          numWorkers.toInt, coresPerWorker.toInt, memoryPerWorkerInt, sc.conf)
        val masterUrls = localCluster.start() // 启动本地集群并获取Master URL
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
          localCluster.stop() // 注册关闭回调以停止本地集群
        }
        (backend, scheduler)

      case masterUrl =>
        // 其他所有master URL：通过ServiceLoader加载外部集群管理器（如YARN、K8s等）
        val cm = getClusterManager(masterUrl) match {
          case Some(clusterMgr) => clusterMgr
          case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
        }
        try {
          val scheduler = cm.createTaskScheduler(sc, masterUrl)
          val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
          cm.initialize(scheduler, backend)
          (backend, scheduler)
        } catch {
          case se: SparkException => throw se
          case NonFatal(e) =>
            throw new SparkException("External scheduler cannot be instantiated", e)
        }
    }
  }

  /** 通过ServiceLoader机制查找能处理给定URL的外部集群管理器 */
  private def getClusterManager(url: String): Option[ExternalClusterManager] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoaders =
      ServiceLoader.load(classOf[ExternalClusterManager], loader).asScala.filter(_.canCreate(url))
    if (serviceLoaders.size > 1) {
      throw new SparkException(
        s"Multiple external cluster managers registered for the url $url: $serviceLoaders")
    }
    serviceLoaders.headOption
  }

  /**
   * 若hadoop-cloud模块存在，则默认为所有S3存储桶启用Magic Committer。
   * Magic Committer可以避免S3上的重命名操作，显著提升写入性能。
   */
  private def enableMagicCommitterIfNeeded(conf: SparkConf): Unit = {
    if (Utils.classIsLoadable("org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter") &&
        Utils.classIsLoadable("org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")) {
      // 尝试启用S3 Magic Committer（若未显式设置）
      conf.setIfMissing("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
      if (conf.get("spark.hadoop.fs.s3a.committer.magic.enabled").equals("true")) {
        conf.setIfMissing("spark.hadoop.fs.s3a.committer.name", "magic")
        conf.setIfMissing("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
          "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
        conf.setIfMissing("spark.sql.parquet.output.committer.class",
          "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
        conf.setIfMissing("spark.sql.sources.commitProtocolClass",
          "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      }
    }
  }

  /**
   * 向Driver和Executor的额外Java选项中补充JVM模块系统选项（SPARK-36796）。
   * 在Java 9+的模块系统中，某些反射操作需要显式开放模块访问权限。
   */
  private def supplementJavaModuleOptions(conf: SparkConf): Unit = {
    def supplement(key: String): Unit = {
      val v = s"${JavaModuleOptions.defaultModuleOptions()} ${conf.get(key, "")}".trim()
      conf.set(key, v)
    }
    supplement(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
    supplement(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS)
  }

  /** 向Driver和Executor的额外Java选项中补充IPv6偏好设置 */
  private def supplementJavaIPv6Options(conf: SparkConf): Unit = {
    def supplement(key: String): Unit = {
      val v = s"-Djava.net.preferIPv6Addresses=${Utils.preferIPv6} ${conf.get(key, "")}".trim()
      conf.set(key, v)
    }
    supplement(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
    supplement(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS)
  }
}

/**
 * 用于从master字符串中提取信息的正则表达式集合。
 * Spark支持多种部署模式，每种模式的master URL格式不同，需要对应的正则表达式来解析。
 */
private object SparkMasterRegex {
  // local[N]和local[*]格式的正则表达式
  val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
  // local[N, maxRetries]格式的正则表达式，用于可模拟任务失败的测试
  val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
  // local-cluster[N, cores, memory]格式的正则表达式，用于本地模拟Spark集群
  val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
  // spark://格式的正则表达式，用于连接Standalone集群
  val SPARK_REGEX = """spark://(.*)""".r
  // k8s://格式的正则表达式，用于连接Kubernetes集群
  val KUBERNETES_REGEX = """k8s://(.*)""".r

  /** 判断master URL是否为Kubernetes模式 */
  def isK8s(master: String) : Boolean = isK8s(Option(master))

  def isK8s(master: Option[String]) : Boolean = {
    master match {
      case Some(KUBERNETES_REGEX(_)) => true
      case _ => false
    }
  }
}

/**
 * 封装如何将Hadoop Writable类型转换为Scala类型T的转换器类。
 * 存储了与类型T对应的Writable类（例如Int对应IntWritable）以及执行转换的函数。
 * writableClass的getter接受ClassTag[T]参数，以支持Writable子类到自身的转换场景。
 */
private[spark] class WritableConverter[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable

/** WritableConverter伴生对象，提供常见Scala类型到Hadoop Writable类型的隐式转换器 */
object WritableConverter {

  // 创建简单Writable转换器的辅助方法
  private[spark] def simpleWritableConverter[T, W <: Writable: ClassTag](convert: W => T)
  : WritableConverter[T] = {
    val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
    new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
  }

  // 以下隐式函数原先在SparkContext 1.3之前定义在SparkContext类中，
  // 用户需要 `import SparkContext._` 来启用。现移至此处以让编译器自动发现。
  // 为保持向后兼容，SparkContext中仍保留旧函数并直接转发到这里。

  // 以下隐式声明是为了兼容Scala 2.12而新增的。
  // Scala 2.12弃用了零参数方法的eta扩展，因此不会匹配无参方法作为无参函数类型的隐式。

  implicit val intWritableConverterFn: () => WritableConverter[Int] =
    () => simpleWritableConverter[Int, IntWritable](_.get)

  implicit val longWritableConverterFn: () => WritableConverter[Long] =
    () => simpleWritableConverter[Long, LongWritable](_.get)

  implicit val doubleWritableConverterFn: () => WritableConverter[Double] =
    () => simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit val floatWritableConverterFn: () => WritableConverter[Float] =
    () => simpleWritableConverter[Float, FloatWritable](_.get)

  implicit val booleanWritableConverterFn: () => WritableConverter[Boolean] =
    () => simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit val bytesWritableConverterFn: () => WritableConverter[Array[Byte]] = {
    () => simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
      // getBytes返回的数组可能比实际数据长，需截取有效部分
      Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
    }
  }

  implicit val stringWritableConverterFn: () => WritableConverter[String] =
    () => simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverterFn[T <: Writable : ClassTag]: () => WritableConverter[T] =
    () => new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])

  // 以下隐式转换保留用于向后兼容，功能与上述相同

  implicit def intWritableConverter(): WritableConverter[Int] =
    simpleWritableConverter[Int, IntWritable](_.get)

  implicit def longWritableConverter(): WritableConverter[Long] =
    simpleWritableConverter[Long, LongWritable](_.get)

  implicit def doubleWritableConverter(): WritableConverter[Double] =
    simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit def floatWritableConverter(): WritableConverter[Float] =
    simpleWritableConverter[Float, FloatWritable](_.get)

  implicit def booleanWritableConverter(): WritableConverter[Boolean] =
    simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
    simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
      // getBytes返回的数组可能比实际数据长，需截取有效部分
      Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
    }
  }

  implicit def stringWritableConverter(): WritableConverter[String] =
    simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverter[T <: Writable](): WritableConverter[T] =
    new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])
}

/**
 * 封装如何将Scala类型T转换为Hadoop Writable类型的工厂类。
 * 存储了与类型T对应的Writable类（例如Int对应IntWritable）以及执行转换的函数。
 * Writable类将在SequenceFileRDDFunctions中使用。
 */
private[spark] class WritableFactory[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: T => Writable) extends Serializable

/** WritableFactory伴生对象，提供常见Scala类型到Hadoop Writable类型的隐式工厂 */
object WritableFactory {

  // 创建简单Writable工厂的辅助方法
  private[spark] def simpleWritableFactory[T: ClassTag, W <: Writable : ClassTag](convert: T => W)
    : WritableFactory[T] = {
    val writableClass = implicitly[ClassTag[W]].runtimeClass.asInstanceOf[Class[W]]
    new WritableFactory[T](_ => writableClass, convert)
  }

  // 以下为各基本类型的隐式WritableFactory，自动将Scala类型转换为对应的Hadoop Writable类型

  implicit def intWritableFactory: WritableFactory[Int] =
    simpleWritableFactory(new IntWritable(_))

  implicit def longWritableFactory: WritableFactory[Long] =
    simpleWritableFactory(new LongWritable(_))

  implicit def floatWritableFactory: WritableFactory[Float] =
    simpleWritableFactory(new FloatWritable(_))

  implicit def doubleWritableFactory: WritableFactory[Double] =
    simpleWritableFactory(new DoubleWritable(_))

  implicit def booleanWritableFactory: WritableFactory[Boolean] =
    simpleWritableFactory(new BooleanWritable(_))

  implicit def bytesWritableFactory: WritableFactory[Array[Byte]] =
    simpleWritableFactory(new BytesWritable(_))

  implicit def stringWritableFactory: WritableFactory[String] =
    simpleWritableFactory(new Text(_))

  // Writable子类到自身的转换（直接透传）
  implicit def writableWritableFactory[T <: Writable: ClassTag]: WritableFactory[T] =
    simpleWritableFactory(w => w)

}

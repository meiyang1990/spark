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

import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.LinkedHashSet
import scala.jdk.CollectionConverters._

import org.apache.avro.{Schema, SchemaNormalization}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.internal.config.Network._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * 只读Spark配置接口，提供各种类型参数的获取方法。
 * 支持字符串、时间（秒/毫秒）、大小（字节/KB/MB/GB）、整数、长整数、浮点数、布尔值等类型。
 */
trait ReadOnlySparkConf {
  /** 获取配置参数，未设置时抛出NoSuchElementException */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** 获取配置参数，未设置时返回默认值 */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** 获取预定义配置条目的值（Spark内部API），返回类型由ConfigEntry定义 */
  private[spark] def get[T](entry: ConfigEntry[T]): T

  /** 获取时间参数（秒），未设置时抛异常。无后缀时默认为秒 */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /** 获取时间参数（秒），未设置时使用默认值 */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /** 获取时间参数（毫秒），未设置时抛异常。无后缀时默认为毫秒 */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /** 获取时间参数（毫秒），未设置时使用默认值 */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /** 获取大小参数（字节），未设置时抛异常 */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /** 获取大小参数（字节），未设置时使用字符串默认值 */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /** 获取大小参数（字节），未设置时使用长整数默认值 */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, s"${defaultValue}B"))
  }

  /** 获取大小参数（KB），未设置时抛异常 */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /** 获取大小参数（KB），未设置时使用默认值 */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /** 获取大小参数（MB），未设置时抛异常 */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /** 获取大小参数（MB），未设置时使用默认值 */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /** 获取大小参数（GB），未设置时抛异常 */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /** 获取大小参数（GB），未设置时使用默认值 */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** 获取参数值，以Option形式返回 */
  def getOption(key: String): Option[String]

  /** 获取所有参数，返回键值对数组 */
  def getAll: Array[(String, String)]

  /** 获取整数参数，未设置时使用默认值 */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** 获取长整数参数，未设置时使用默认值 */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** 获取双精度浮点数参数，未设置时使用默认值 */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** 获取布尔参数，未设置时使用默认值 */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** 检查配置中是否包含指定的键 */
  def contains(key: String): Boolean

  /** 检查配置中是否包含指定的ConfigEntry */
  def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /**
   * 值格式转换的包装方法：捕获NumberFormatException和IllegalArgumentException，
   * 在异常消息中附带出错的配置键名以便调试
   */
  protected def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
          .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }

  /** 获取环境变量的包装方法，便于在单元测试中mock */
  private[spark] def getenv(name: String): String = System.getenv(name)
}

/**
 * Spark应用程序的配置类，以键值对形式设置各种Spark参数。
 *
 * 通常使用`new SparkConf()`创建，会自动加载应用中以`spark.*`开头的Java系统属性。
 * 直接在SparkConf对象上设置的参数优先级高于系统属性。
 * 单元测试中可用`new SparkConf(false)`跳过加载外部配置。
 *
 * 所有setter方法支持链式调用，如`new SparkConf().setMaster("local").setAppName("My app")`。
 *
 * @param loadDefaults 是否从Java系统属性中加载配置
 *
 * @note SparkConf传递给Spark后会被克隆且不可再修改。Spark不支持运行时修改配置。
 */
class SparkConf(loadDefaults: Boolean)
    extends ReadOnlySparkConf
    with Cloneable
    with Logging
    with Serializable {

  import SparkConf._

  /** 使用默认参数创建SparkConf（加载系统属性和classpath中的配置） */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  // 延迟初始化ConfigReader，绑定环境变量提供者用于配置值中的变量替换
  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new SparkConfigProvider(settings))
    _reader.bindEnv((key: String) => Option(getenv(key)))
    _reader
  }

  // 如果loadDefaults为true，从系统属性中加载所有spark.*开头的配置
  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  /** 从系统属性中加载所有spark.*开头的配置到当前SparkConf */
  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value, silent)
    }
    this
  }

  /** 设置一个配置键值对 */
  def set(key: String, value: String): SparkConf = {
    set(key, value, false)
  }

  /** 内部set方法：校验key/value非null，可选地记录已废弃配置的警告 */
  private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  /** 通过ConfigEntry设置配置值，使用ConfigEntry的字符串转换器 */
  private[spark] def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  /** 通过OptionalConfigEntry设置配置值 */
  private[spark] def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  /** 设置Master URL，如"local"本地单线程、"local[4]"本地4核、"spark://master:7077"集群模式 */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** 设置应用名称，将显示在Spark Web UI中 */
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  /** 设置需要分发到集群的JAR文件列表 */
  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
    set(JARS, jars.filter(_ != null))
  }

  /** 设置需要分发到集群的JAR文件列表（Java友好版本） */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toImmutableArraySeq)
  }

  /** 设置Executor启动时的环境变量，存储为spark.executorEnv.VAR_NAME格式 */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set("spark.executorEnv." + variable, value)
  }

  /** 批量设置Executor环境变量 */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  /** 批量设置Executor环境变量（Java友好版本） */
  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toImmutableArraySeq)
  }

  /** 设置Worker节点上Spark的安装路径 */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  /** 批量设置多个参数 */
  def setAll(settings: Iterable[(String, String)]): SparkConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** 仅在参数尚未配置时设置（不覆盖已有值） */
  def setIfMissing(key: String, value: String): SparkConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  /** 通过ConfigEntry设置配置值（仅在未配置时） */
  private[spark] def setIfMissing[T](entry: ConfigEntry[T], value: T): SparkConf = {
    if (settings.putIfAbsent(entry.key, entry.stringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  /** 通过OptionalConfigEntry设置配置值（仅在未配置时） */
  private[spark] def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
    if (settings.putIfAbsent(entry.key, entry.rawStringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  /**
   * 使用Kryo序列化并注册指定的类。
   * 多次调用会追加而非覆盖之前注册的类。
   */
  def registerKryoClasses(classes: Array[Class[_]]): SparkConf = {
    val allClassNames = new LinkedHashSet[String]()
    // 获取已注册的类名
    allClassNames ++= get(KRYO_CLASSES_TO_REGISTER).map(_.trim)
      .filter(!_.isEmpty)
    // 追加新的类名
    allClassNames ++= classes.map(_.getName)

    set(KRYO_CLASSES_TO_REGISTER, allClassNames.toSeq)
    // 同时设置序列化器为KryoSerializer
    set(SERIALIZER, classOf[KryoSerializer].getName)
    this
  }

  private final val avroNamespace = "avro.schema."

  /** 注册Avro Schema以便通用记录序列化器减少网络IO。Schema以指纹作为键存储 */
  def registerAvroSchemas(schemas: Schema*): SparkConf = {
    for (schema <- schemas) {
      set(avroNamespace + SchemaNormalization.parsingFingerprint64(schema), schema.toString)
    }
    this
  }

  /** 获取配置中所有注册的Avro Schema，返回Map[指纹ID -> Schema字符串] */
  def getAvroSchema: Map[Long, String] = {
    getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
      .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
      .toMap
  }

  /** 从配置中移除指定的参数 */
  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  /** 通过ConfigEntry移除配置参数 */
  private[spark] def remove(entry: ConfigEntry[_]): SparkConf = {
    remove(entry.key)
  }

  /** 通过ConfigReader获取预定义配置条目的值 */
  private[spark] def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /** 获取配置参数值，同时检查已废弃的配置键名 */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  /** 获取配置参数值并进行变量替换 */
  private[spark] def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute)
  }

  /** 获取所有配置参数，返回键值对数组 */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** 获取所有以指定前缀开头的参数，返回时去除前缀 */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  /** 获取所有以指定前缀开头的参数，并对键应用转换函数f */
  def getAllWithPrefix[K](prefix: String, f: String => K): Array[(K, String)] = {
    getAll.filter { case (k, _) => k.startsWith(prefix) }
      .map { case (k, v) => (f(k), v) }
  }

  /** 获取所有Executor环境变量配置（spark.executorEnv.*前缀） */
  def getExecutorEnv: Seq[(String, String)] = {
    getAllWithPrefix("spark.executorEnv.").toImmutableArraySeq
  }

  /** 获取Spark应用ID，Driver端在TaskScheduler注册后有效，Executor端从启动时就有效 */
  def getAppId: String = get("spark.app.id")

  /** 检查是否包含指定配置键，同时检查该键的所有替代键名 */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  /** 克隆当前SparkConf对象，创建一个新的独立副本 */
  override def clone: SparkConf = {
    val cloned = new SparkConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  /**
   * 校验配置的合法性。检查非法或已废弃的配置项，对非法项抛异常。
   * 非幂等方法——可能会将废弃的配置转换为新的配置项。
   */
  private[spark] def validateSettings(): Unit = {
    // 警告spark.local.dir将被集群管理器覆盖
    if (contains("spark.local.dir")) {
      val msg = "Note that spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in standalone/kubernetes and LOCAL_DIRS" +
        " in YARN)."
      logWarning(msg)
    }

    // 检查Spark 1.1及之前版本使用的废弃配置
    sys.props.get("spark.driver.libraryPath").foreach { value =>
      val warning =
        log"""
          |spark.driver.libraryPath was detected (set to '${MDC(LogKeys.CONFIG, value)}').
          |This is deprecated in Spark 1.2+.
          |
          |Please instead use: ${MDC(LogKeys.CONFIG2, DRIVER_LIBRARY_PATH.key)}
        """.stripMargin
      logWarning(warning)
    }

    // 校验Executor的Java选项：不允许包含-Dspark（应通过SparkConf设置）和-Xmx（应通过spark.executor.memory设置）
    Seq(EXECUTOR_JAVA_OPTIONS.key, "spark.executor.defaultJavaOptions").foreach { executorOptsKey =>
      getOption(executorOptsKey).foreach { javaOpts =>
        if (javaOpts.contains("-Dspark")) {
          throw new SparkException(
            errorClass = "INVALID_SPARK_CONFIG.INVALID_EXECUTOR_SPARK_OPTIONS",
            messageParameters = Map("executorOptsKey" -> executorOptsKey, "javaOpts" -> javaOpts),
            cause = null)
        }
        if (javaOpts.contains("-Xmx")) {
          throw new SparkException(
            errorClass = "INVALID_SPARK_CONFIG.INVALID_EXECUTOR_MEMORY_OPTIONS",
            messageParameters = Map("executorOptsKey" -> executorOptsKey, "javaOpts" -> javaOpts),
            cause = null)
        }
      }
    }

    // 校验内存比例参数必须在0到1之间
    for (key <- Seq(MEMORY_FRACTION.key, MEMORY_STORAGE_FRACTION.key)) {
      val value = getDouble(key, 0.5)
      if (value > 1 || value < 0) {
        throw new IllegalArgumentException(s"$key should be between 0 and 1 (was '$value').")
      }
      SparkException.require(
        value >= 0 && value <= 1,
        errorClass = "INVALID_SPARK_CONFIG.INVALID_MEMORY_FRACTION",
        messageParameters = Map(
          "memoryFractionKey" -> key,
          "memoryFractionValue" -> value.toString))
    }

    // 校验部署模式必须是cluster或client
    if (contains(SUBMIT_DEPLOY_MODE)) {
      get(SUBMIT_DEPLOY_MODE) match {
        case "cluster" | "client" =>
        case _ => throw new SparkException(
          errorClass = "INVALID_SPARK_CONFIG.INVALID_SPARK_SUBMIT_DEPLOY_MODE_KEY",
          messageParameters = Map("sparkSubmitDeployModeKey" -> SUBMIT_DEPLOY_MODE.key),
          cause = null)
      }
    }

    // 校验总核心数是否能被每个Executor的核心数整除，不能整除时警告剩余核心不会被分配
    if (contains(CORES_MAX) && contains(EXECUTOR_CORES)) {
      val totalCores = getInt(CORES_MAX.key, 1)
      val executorCores = get(EXECUTOR_CORES)
      val leftCores = totalCores % executorCores
      if (leftCores != 0) {
        logWarning(log"Total executor cores: " +
          log"${MDC(LogKeys.NUM_EXECUTOR_CORES_TOTAL, totalCores)} " +
          log"is not divisible by cores per executor: " +
          log"${MDC(LogKeys.NUM_EXECUTOR_CORES, executorCores)}, " +
          log"the left cores: " +
          log"${MDC(LogKeys.NUM_EXECUTOR_CORES_REMAINING, leftCores)} " +
          log"will not be allocated")
      }
    }

    // 如果启用了加密，则必须同时启用网络认证
    val encryptionEnabled = get(NETWORK_CRYPTO_ENABLED) || get(SASL_ENCRYPTION_ENABLED)
    SparkException.require(
      !encryptionEnabled || get(NETWORK_AUTH_ENABLED),
      errorClass = "INVALID_SPARK_CONFIG.NETWORK_AUTH_MUST_BE_ENABLED",
      messageParameters = Map("networkAuthEnabledConf" -> NETWORK_AUTH_ENABLED.key))

    // SPARK-22754: 心跳间隔必须小于网络超时时间，否则几乎总是导致ExecutorLostFailure
    val executorTimeoutThresholdMs = get(NETWORK_TIMEOUT) * 1000
    val executorHeartbeatIntervalMs = get(EXECUTOR_HEARTBEAT_INTERVAL)
    SparkException.require(
      executorTimeoutThresholdMs > executorHeartbeatIntervalMs,
      errorClass = "INVALID_SPARK_CONFIG.INVALID_EXECUTOR_HEARTBEAT_INTERVAL",
      messageParameters = Map(
        "networkTimeoutKey" -> NETWORK_TIMEOUT.key,
        "networkTimeoutValue" -> executorTimeoutThresholdMs.toString,
        "executorHeartbeatIntervalKey" -> EXECUTOR_HEARTBEAT_INTERVAL.key,
        "executorHeartbeatIntervalValue" -> executorHeartbeatIntervalMs.toString))
  }

  /** 返回所有配置的调试字符串，每行一个键值对，敏感信息会被脱敏 */
  def toDebugString: String = {
    Utils.redact(this, getAll).sorted.map { case (k, v) => k + "=" + v }.mkString("\n")
  }

}

/**
 * SparkConf伴生对象，管理已废弃配置项的映射和配置键的备用名称。
 */
private[spark] object SparkConf extends Logging {

  /** 已废弃配置键到废弃信息的映射表。当用户配置中出现这些键时会记录警告日志 */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("spark.cache.class", "0.8",
        "The spark.cache.class property is no longer being used! Specify storage levels using " +
        "the RDD.persist() method instead."),
      DeprecatedConfig("spark.yarn.user.classpath.first", "1.3",
        "Please use spark.{driver,executor}.userClassPathFirst instead."),
      DeprecatedConfig("spark.kryoserializer.buffer.mb", "1.4",
        "Please use spark.kryoserializer.buffer instead. The default value for " +
          "spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
          "are no longer accepted. To specify the equivalent now, one may use '64k'."),
      DeprecatedConfig("spark.shuffle.spill", "1.6", "Not used anymore."),
      DeprecatedConfig("spark.rpc", "2.0", "Not used anymore."),
      DeprecatedConfig("spark.scheduler.executorTaskBlacklistTime", "2.1.0",
        "Not used anymore. Please use the new excludedOnFailure options, spark.excludeOnFailure.*"),
      DeprecatedConfig("spark.yarn.am.port", "2.0.0", "Not used anymore"),
      DeprecatedConfig("spark.executor.port", "2.0.0", "Not used anymore"),
      DeprecatedConfig("spark.rpc.numRetries", "2.2.0", "Not used anymore"),
      DeprecatedConfig("spark.rpc.retry.wait", "2.2.0", "Not used anymore"),
      DeprecatedConfig("spark.shuffle.service.index.cache.entries", "2.3.0",
        "Not used anymore. Please use spark.shuffle.service.index.cache.size"),
      DeprecatedConfig("spark.yarn.credentials.file.retention.count", "2.4.0", "Not used anymore."),
      DeprecatedConfig("spark.yarn.credentials.file.retention.days", "2.4.0", "Not used anymore."),
      DeprecatedConfig("spark.yarn.services", "3.0.0", "Feature no longer available."),
      DeprecatedConfig("spark.executor.plugins", "3.0.0",
        "Feature replaced with new plugin API. See Monitoring documentation."),
      DeprecatedConfig("spark.blacklist.enabled", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.enabled"),
      DeprecatedConfig("spark.blacklist.task.maxTaskAttemptsPerExecutor", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor"),
      DeprecatedConfig("spark.blacklist.task.maxTaskAttemptsPerNode", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.task.maxTaskAttemptsPerNode"),
      DeprecatedConfig("spark.blacklist.application.maxFailedTasksPerExecutor", "3.1.0",
        "Not used anymore. Please use " +
          "spark.excludeOnFailure.application.maxFailedTasksPerExecutor"),
      DeprecatedConfig("spark.blacklist.stage.maxFailedTasksPerExecutor", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.stage.maxFailedTasksPerExecutor"),
      DeprecatedConfig("spark.blacklist.application.maxFailedExecutorsPerNode", "3.1.0",
        "Not used anymore. Please use " +
          "spark.excludeOnFailure.application.maxFailedExecutorsPerNode"),
      DeprecatedConfig("spark.blacklist.stage.maxFailedExecutorsPerNode", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.stage.maxFailedExecutorsPerNode"),
      DeprecatedConfig("spark.blacklist.timeout", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.timeout"),
      DeprecatedConfig("spark.blacklist.application.fetchFailure.enabled", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.application.fetchFailure.enabled"),
      DeprecatedConfig("spark.scheduler.blacklist.unschedulableTaskSetTimeout", "3.1.0",
        "Not used anymore. Please use " +
          "spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout"),
      DeprecatedConfig("spark.blacklist.killBlacklistedExecutors", "3.1.0",
        "Not used anymore. Please use spark.excludeOnFailure.killExcludedExecutors"),
      DeprecatedConfig("spark.yarn.blacklist.executor.launch.blacklisting.enabled", "3.1.0",
        "Not used anymore. Please use spark.yarn.executor.launch.excludeOnFailure.enabled"),
      DeprecatedConfig("spark.network.remoteReadNioBufferConversion", "3.5.2",
        "Please open a JIRA ticket to report it if you need to use this configuration."),
      DeprecatedConfig("spark.shuffle.unsafe.file.output.buffer", "4.0.0",
        "Please use spark.shuffle.localDisk.file.output.buffer"),
      DeprecatedConfig("spark.shuffle.server.chunkFetchHandlerThreadsPercent", "4.2.0",
        "Using separate chunkFetchHandlers could be problematic according to the underlying" +
          " netty layer"),
      DeprecatedConfig("spark.shuffle.server.finalizeShuffleMergeThreadsPercent", "4.2.0",
        "Using separate finalizeWorkers could be problematic according to the underlying" +
          " netty layer")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
   * 当前配置键到其历史替代键列表的映射表。
   * 替代键按定义顺序使用。当用户配置中出现已废弃的替代键时会记录警告。
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    EXECUTOR_USER_CLASS_PATH_FIRST.key -> Seq(
      AlternateConfig("spark.files.userClassPathFirst", "1.3")),
    UPDATE_INTERVAL_S.key -> Seq(
      AlternateConfig("spark.history.fs.update.interval.seconds", "1.4"),
      AlternateConfig("spark.history.fs.updateInterval", "1.3"),
      AlternateConfig("spark.history.updateInterval", "1.3")),
    CLEANER_INTERVAL_S.key -> Seq(
      AlternateConfig("spark.history.fs.cleaner.interval.seconds", "1.4")),
    MAX_LOG_AGE_S.key -> Seq(
      AlternateConfig("spark.history.fs.cleaner.maxAge.seconds", "1.4")),
    "spark.yarn.am.waitTime" -> Seq(
      AlternateConfig("spark.yarn.applicationMaster.waitTries", "1.3",
        // Translate old value to a duration, with 10s wait time per try.
        translation = s => s"${s.toLong * 10}s")),
    REDUCER_MAX_SIZE_IN_FLIGHT.key -> Seq(
      AlternateConfig("spark.reducer.maxMbInFlight", "1.4")),
    KRYO_SERIALIZER_BUFFER_SIZE.key -> Seq(
      AlternateConfig("spark.kryoserializer.buffer.mb", "1.4",
        translation = s => s"${(s.toDouble * 1000).toInt}k")),
    KRYO_SERIALIZER_MAX_BUFFER_SIZE.key -> Seq(
      AlternateConfig("spark.kryoserializer.buffer.max.mb", "1.4")),
    SHUFFLE_FILE_BUFFER_SIZE.key -> Seq(
      AlternateConfig("spark.shuffle.file.buffer.kb", "1.4")),
    EXECUTOR_LOGS_ROLLING_MAX_SIZE.key -> Seq(
      AlternateConfig("spark.executor.logs.rolling.size.maxBytes", "1.4")),
    IO_COMPRESSION_SNAPPY_BLOCKSIZE.key -> Seq(
      AlternateConfig("spark.io.compression.snappy.block.size", "1.4")),
    IO_COMPRESSION_LZ4_BLOCKSIZE.key -> Seq(
      AlternateConfig("spark.io.compression.lz4.block.size", "1.4")),
    "spark.streaming.fileStream.minRememberDuration" -> Seq(
      AlternateConfig("spark.streaming.minRememberDuration", "1.5")),
    "spark.yarn.max.executor.failures" -> Seq(
      AlternateConfig("spark.yarn.max.worker.failures", "1.5")),
    MEMORY_OFFHEAP_ENABLED.key -> Seq(
      AlternateConfig("spark.unsafe.offHeap", "1.6")),
    "spark.yarn.jars" -> Seq(
      AlternateConfig("spark.yarn.jar", "2.0")),
    MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM.key -> Seq(
      AlternateConfig("spark.reducer.maxReqSizeShuffleToMem", "2.3"),
      AlternateConfig("spark.maxRemoteBlockSizeFetchToMem", "3.0")),
    LISTENER_BUS_EVENT_QUEUE_CAPACITY.key -> Seq(
      AlternateConfig("spark.scheduler.listenerbus.eventqueue.size", "2.3")),
    DRIVER_MEMORY_OVERHEAD.key -> Seq(
      AlternateConfig("spark.yarn.driver.memoryOverhead", "2.3")),
    EXECUTOR_MEMORY_OVERHEAD.key -> Seq(
      AlternateConfig("spark.yarn.executor.memoryOverhead", "2.3")),
    KEYTAB.key -> Seq(
      AlternateConfig("spark.yarn.keytab", "3.0")),
    PRINCIPAL.key -> Seq(
      AlternateConfig("spark.yarn.principal", "3.0")),
    KERBEROS_RELOGIN_PERIOD.key -> Seq(
      AlternateConfig("spark.yarn.kerberos.relogin.period", "3.0")),
    KERBEROS_FILESYSTEMS_TO_ACCESS.key -> Seq(
      AlternateConfig("spark.yarn.access.namenodes", "2.2"),
      AlternateConfig("spark.yarn.access.hadoopFileSystems", "3.0")),
    "spark.kafka.consumer.cache.capacity" -> Seq(
      AlternateConfig("spark.sql.kafkaConsumerCache.capacity", "3.0")),
    MAX_EXECUTOR_FAILURES.key -> Seq(
      AlternateConfig("spark.yarn.max.executor.failures", "3.5")),
    EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS.key -> Seq(
      AlternateConfig("spark.yarn.executor.failuresValidityInterval", "3.5"))
  )

  /**
   * configsWithAlternatives的反向索引，将废弃的替代键名映射到(新键名, 替代键信息)的二元组。
   * 使查找废弃键名更高效。
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * 判断给定配置键是否需要在Executor启动时传递。
   * 认证相关配置在Executor连接Scheduler时就需要，其他Spark配置可以稍后从Driver继承。
   */
  def isExecutorStartupConf(name: String): Boolean = {
    (name.startsWith("spark.auth") && name != SecurityManager.SPARK_AUTH_SECRET_CONF) ||
    name.startsWith("spark.rpc") ||
    name.startsWith("spark.network") ||
    // We need SSL configs to propagate as they may be needed for RPCs.
    // Passwords are propagated separately though.
    (name.startsWith("spark.ssl") && !name.contains("Password")) ||
    isSparkPortConf(name)
  }

  /** 判断配置键名是否匹配Spark端口配置模式：spark.*.port 或 spark.port.* */
  def isSparkPortConf(name: String): Boolean = {
    (name.startsWith("spark.") && name.endsWith(".port")) || name.startsWith("spark.port.")
  }

  /** 查找给定配置键的已废弃替代键，返回第一个可用的值（必要时进行值转换） */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /** 如果给定的配置键已废弃，记录警告日志 */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        log"The configuration key '${MDC(LogKeys.CONFIG, key)}' has been deprecated " +
          log"as of Spark ${MDC(LogKeys.CONFIG_VERSION, cfg.version)} and " +
          log"may be removed in the future. " +
          log"${MDC(LogKeys.CONFIG_DEPRECATION_MESSAGE, cfg.deprecationMessage)}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        log"The configuration key '${MDC(LogKeys.CONFIG, key)}' " +
          log"has been deprecated as of " +
          log"Spark ${MDC(LogKeys.CONFIG_VERSION, cfg.version)} and " +
          log"may be removed in the future. Please use the new key " +
          log"'${MDC(LogKeys.CONFIG_KEY_UPDATED, newKey)}' instead.")
      return
    }
  }

  /**
   * 已废弃且无替代项的配置键信息。
   * @param key 废弃的配置键
   * @param version 配置被废弃的Spark版本
   * @param deprecationMessage 废弃警告信息
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * 已废弃的替代配置键信息，包含可选的值转换函数。
   * @param key 废弃的配置键
   * @param version 配置被废弃的Spark版本
   * @param translation 将旧配置值转换为新配置值的转换函数（可选）
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)
}

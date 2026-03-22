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

package org.apache.spark.deploy

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.{List => JList}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.{SparkConf, SparkException, SparkUserAppException}
import org.apache.spark.deploy.SparkSubmitAction._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.DYN_ALLOCATION_ENABLED
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

/**
 * 文件说明：解析并封装spark-submit脚本提交作业的参数，支持从命令行、配置文件、环境变量加载配置
 * 环境参数env主要用于单元测试场景
 *
 * @param args 命令行参数序列
 * @param env 环境变量映射，默认使用系统环境变量
 */
private[deploy] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends SparkSubmitArgumentsParser with Logging {
  var maybeMaster: Option[String] = None
  // Global defaults. These should be keep to minimum to avoid confusing behavior.
  def master: String =
    maybeMaster.getOrElse(System.getProperty("spark.test.master", "local[*]"))
  var maybeRemote: Option[String] = None
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var extraPropertiesFiles: Seq[String] = Nil
  private var loadSparkDefaults: Boolean = false
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var packages: String = null
  var repositories: String = null
  var ivyRepoPath: String = null
  var ivySettingsPath: Option[String] = None
  var packagesExclusions: String = null
  var verbose: Boolean = false
  var isPython: Boolean = false
  var pyFiles: String = null
  var isR: Boolean = false
  var action: SparkSubmitAction = null
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null
  var principal: String = null
  var keytab: String = null
  private var dynamicAllocationEnabled: Boolean = false
  // Standalone cluster mode only
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = false // used internally

  override protected def logName: String = classOf[SparkSubmitArguments].getName

  // 从命令行参数解析并设置参数
  parse(args.asJava)

  // 从属性文件加载配置合并到sparkProperties
  mergeDefaultSparkProperties()
  // 移除sparkProperties中不以"spark."开头的无效配置项
  ignoreNonSparkProperties()
  // 结合sparkProperties和环境变量填充所有缺失的参数
  loadEnvironmentArguments()

  // 从配置读取REST API开关
  useRest = sparkProperties.getOrElse("spark.master.rest.enabled", "false").toBoolean

  // 参数合法性校验
  validateArguments()

  /**
   * 从指定路径加载属性到sparkProperties，路径为null时不执行任何操作
   * @param filePath 属性文件路径
   */
  private def loadPropertiesFromFile(filePath: String): Unit = {
    if (filePath != null) {
      if (verbose) {
        logInfo(log"Using properties file: ${MDC(PATH, filePath)}")
      }
      val properties = Utils.getPropertiesFromFile(filePath)
      properties.foreach { case (k, v) =>
        // 不覆盖命令行--conf已经设置的属性
        if (!sparkProperties.contains(k)) {
          sparkProperties(k) = v
        }
      }
      // 属性文件可能包含敏感信息，打印前先脱敏
      if (verbose) {
        Utils.redact(properties).foreach { case (k, v) =>
          logInfo(log"Adding default property: ${MDC(KEY, k)}=${MDC(VALUE, v)}")
        }
      }
    }
  }

  /**
   * 合并默认属性文件和命令行--conf参数，调用时sparkProperties已经填充了--conf的配置
   * 遵循优先级：--conf > 额外属性文件 > 基础属性文件 > spark-defaults.conf
   */
  private def mergeDefaultSparkProperties(): Unit = {
    // 保存--conf传入的属性，这些优先级最高
    val confProperties = sparkProperties.clone()

    // 先加载用户指定的主属性文件
    loadPropertiesFromFile(propertiesFile)

    // 加载额外属性文件，后加载的覆盖先加载的
    extraPropertiesFiles.foreach { filePath =>
      if (filePath != null) {
        if (verbose) {
          logInfo(log"Using properties file: ${MDC(PATH, filePath)}")
        }
        val properties = Utils.getPropertiesFromFile(filePath)
        properties.foreach { case (k, v) =>
          // 不覆盖--conf已经设置的属性
          if (!confProperties.contains(k)) {
            sparkProperties(k) = v
          }
        }
        if (verbose) {
          Utils.redact(properties).foreach { case (k, v) =>
            logInfo(log"Adding default property: ${MDC(KEY, k)}=${MDC(VALUE, v)}")
          }
        }
      }
    }

    // 在以下两种情况加载spark-defaults.conf：
    // 1. 用户没有指定属性文件
    // 2. 用户指定了属性文件，但开启了--load-spark-defaults标志
    if (propertiesFile == null || loadSparkDefaults) {
      loadPropertiesFromFile(Utils.getDefaultPropertiesFile(env))
    }
  }

  /**
   * 从sparkProperties中移除不以"spark."开头的无效配置项
   */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.keys.foreach { k =>
    if (!k.startsWith("spark.")) {
      sparkProperties -= k
      logWarning(log"Ignoring non-Spark config property: ${MDC(CONFIG, k)}")
    }
  }
}

  /**
   * 从环境变量、Spark属性等来源加载参数，填充所有未设置的参数
   */
  private def loadEnvironmentArguments(): Unit = {
    // 优先级：命令行 > spark属性 > 环境变量
    maybeMaster = maybeMaster
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
    maybeRemote = maybeRemote
      .orElse(sparkProperties.get("spark.remote"))
      .orElse(env.get("SPARK_REMOTE"))

    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get(config.DRIVER_CLASS_PATH.key))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get(config.DRIVER_JAVA_OPTIONS.key))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get(config.DRIVER_LIBRARY_PATH.key))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get(config.DRIVER_MEMORY.key))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get(config.DRIVER_CORES.key))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get(config.EXECUTOR_MEMORY.key))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get(config.EXECUTOR_CORES.key))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get(config.CORES_MAX.key))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get(config.JARS.key)).orNull
    files = Option(files).orElse(sparkProperties.get(config.FILES.key)).orNull
    archives = Option(archives).orElse(sparkProperties.get(config.ARCHIVES.key)).orNull
    pyFiles = Option(pyFiles).orElse(sparkProperties.get(config.SUBMIT_PYTHON_FILES.key)).orNull
    ivyRepoPath = sparkProperties.get(config.JAR_IVY_REPO_PATH.key).orNull
    ivySettingsPath = sparkProperties.get(config.JAR_IVY_SETTING_PATH.key)
    packages = Option(packages).orElse(sparkProperties.get(config.JAR_PACKAGES.key)).orNull
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get(config.JAR_PACKAGES_EXCLUSIONS.key)).orNull
    repositories = Option(repositories)
      .orElse(sparkProperties.get(config.JAR_REPOSITORIES.key)).orNull
    deployMode = Option(deployMode)
      .orElse(sparkProperties.get(config.SUBMIT_DEPLOY_MODE.key))
      .orElse(env.get("DEPLOY_MODE"))
      .orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get(config.EXECUTOR_INSTANCES.key).orNull)
    queue = Option(queue).orElse(sparkProperties.get("spark.yarn.queue")).orNull
    keytab = Option(keytab)
      .orElse(sparkProperties.get(config.KEYTAB.key))
      .orElse(sparkProperties.get("spark.yarn.keytab"))
      .orNull
    principal = Option(principal)
      .orElse(sparkProperties.get(config.PRINCIPAL.key))
      .orElse(sparkProperties.get("spark.yarn.principal"))
      .orNull
    dynamicAllocationEnabled =
      sparkProperties.get(DYN_ALLOCATION_ENABLED.key).exists("true".equalsIgnoreCase)

    // YARN模式兼容旧环境变量SPARK_YARN_APP_NAME设置应用名
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // 如果没设置应用名，用主类名或者主资源文件名作为默认应用名
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = new File(primaryResource).getName()
    }

    // 默认动作是提交作业
    action = Option(action).getOrElse(SUBMIT)
  }

  /**
   * 校验必填参数是否存在，在加载完所有默认值后调用，根据不同动作分别校验
   */
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
      case PRINT_VERSION =>
    }
  }

  /**
   * 校验提交作业场景参数合法性
   */
  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (maybeRemote.isDefined && (maybeMaster.isDefined || deployMode != null)) {
      error("Remote cannot be specified with master and/or deploy mode.")
    }
    if (primaryResource == null) {
      error("Must specify a primary resource (JAR or Python or R file)")
    }
    if (driverMemory != null
        && Try(JavaUtils.byteStringAsBytes(driverMemory)).getOrElse(-1L) <= 0) {
      error("Driver memory must be a positive number")
    }
    if (executorMemory != null
        && Try(JavaUtils.byteStringAsBytes(executorMemory)).getOrElse(-1L) <= 0) {
      error("Executor memory must be a positive number")
    }
    if (driverCores != null && Try(driverCores.toInt).getOrElse(-1) <= 0) {
      error("Driver cores must be a positive number")
    }
    if (executorCores != null && Try(executorCores.toInt).getOrElse(-1) <= 0) {
      error("Executor cores must be a positive number")
    }
    if (totalExecutorCores != null && Try(totalExecutorCores.toInt).getOrElse(-1) <= 0) {
      error("Total executor cores must be a positive number")
    }
    if (!dynamicAllocationEnabled &&
      numExecutors != null && Try(numExecutors.toInt).getOrElse(-1) <= 0) {
      error("Number of executors must be a positive number")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        error(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }

    if (proxyUser != null && principal != null) {
      error("Only one of --proxy-user or --principal can be provided.")
    }
  }

  /**
   * 校验kill提交ID参数合法性
   */
  private def validateKillArguments(): Unit = {
    if (submissionToKill == null) {
      error("Please specify a submission to kill.")
    }
  }

  /**
   * 校验状态查询提交ID参数合法性
   */
  private def validateStatusRequestArguments(): Unit = {
    if (submissionToRequestStatusFor == null) {
      error("Please specify a submission to request status for.")
    }
  }

  /**
   * 判断是否为Standalone集群模式
   * @return true如果是Standalone集群模式返回true
   */
  def isStandaloneCluster: Boolean = {
    master.startsWith("spark://") && deployMode == "cluster"
  }

  override def toString: String = {
    s"""Parsed arguments:
    |  master                  $master
    |  remote                  ${maybeRemote.orNull}
    |  deployMode              $deployMode
    |  executorMemory          $executorMemory
    |  executorCores           $executorCores
    |  totalExecutorCores      $totalExecutorCores
    |  propertiesFile          $propertiesFile
    |  extraPropertiesFiles    [${extraPropertiesFiles.mkString(", ")}]
    |  driverMemory            $driverMemory
    |  driverCores             $driverCores
    |  driverExtraClassPath    $driverExtraClassPath
    |  driverExtraLibraryPath  $driverExtraLibraryPath
    |  driverExtraJavaOptions  $driverExtraJavaOptions
    |  supervise               $supervise
    |  queue                   $queue
    |  numExecutors            $numExecutors
    |  files                   $files
    |  pyFiles                 $pyFiles
    |  archives                $archives
    |  mainClass               $mainClass
    |  primaryResource         $primaryResource
    |  name                    $name
    |  childArgs               [${childArgs.mkString(" ")}]
    |  jars                    $jars
    |  packages                $packages
    |  packagesExclusions      $packagesExclusions
    |  repositories            $repositories
    |  verbose                 $verbose
    |
    |Spark properties used, including those specified through
    | --conf and those from the properties files:
    |${Utils.redact(sparkProperties).sorted.mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /**
   * 处理解析到的命令行选项和值，重写父类方法
   * @param opt 选项名
   * @param value 选项值
   * @return 是否继续解析，返回false
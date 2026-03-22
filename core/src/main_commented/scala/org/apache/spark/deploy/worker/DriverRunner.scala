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

package org.apache.spark.deploy.worker

import java.io._
import java.net.URI
import java.nio.file.Files
import java.nio.file.StandardOpenOption

import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.StandaloneResourceUtils.prepareResourcesFile
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{DRIVER_RESOURCES_FILE, SPARK_DRIVER_PREFIX}
import org.apache.spark.internal.config.UI.UI_REVERSE_PROXY
import org.apache.spark.internal.config.Worker.WORKER_DRIVER_TERMINATE_TIMEOUT
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, Utils}

/**
 * 文件: DriverRunner.scala
 * 所属模块: Spark Core 部署模块
 * 核心职责: 在Standalone模式的Worker节点上管理单个Driver进程的完整生命周期，包括启动、监控、失败重启和终止，负责Driver运行目录准备、依赖Jar下载和进程启动等工作。
 * 使用场景: 仅用于Spark Standalone集群部署模式，由Worker节点启动，负责管控提交到该Worker运行的Driver。
 */

/**
 * 管理单个Driver进程的执行，包含失败自动重启能力，仅在Standalone集群部署模式下使用。
 * @param conf Spark配置对象
 * @param driverId Driver的唯一标识ID
 * @param workDir Worker工作根目录
 * @param sparkHome Spark安装根目录
 * @param driverDesc Driver描述信息，包含启动命令、资源需求等
 * @param worker Worker节点的RpcEndpoint引用，用于向Worker发送状态更新
 * @param workerUrl Worker节点的Rpc服务地址
 * @param workerWebUiUrl Worker节点Web UI地址
 * @param securityManager 安全管理器
 * @param resources 分配给该Driver的资源信息
 */
private[deploy] class DriverRunner(
    conf: SparkConf,
    val driverId: String,
    val workDir: File,
    val sparkHome: File,
    val driverDesc: DriverDescription,
    val worker: RpcEndpointRef,
    val workerUrl: String,
    val workerWebUiUrl: String,
    val securityManager: SecurityManager,
    val resources: Map[String, ResourceInformation] = Map.empty)
  extends Logging {

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // 驱动执行完成后填充最终状态
  @volatile private[worker] var finalState: Option[DriverState] = None
  @volatile private[worker] var finalException: Option[Exception] = None

  // 终止Driver进程时等待的超时时间，从配置读取
  private val driverTerminateTimeoutMs = conf.get(WORKER_DRIVER_TERMINATE_TIMEOUT)

  // 解耦时钟用于测试注入
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  // 解耦睡眠器用于测试注入
  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
      Thread.sleep(1000)
      !killed
    }
  }

  /** 启动一个独立线程来运行和管理Driver进程 */
  private[worker] def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run(): Unit = {
        var shutdownHook: AnyRef = null
        try {
          // 注册JVM关闭钩子，Worker退出时杀死Driver进程
          shutdownHook = ShutdownHookManager.addShutdownHook { () =>
            logInfo(log"Worker shutting down, killing driver ${MDC(DRIVER_ID, driverId)}")
            kill()
          }

          // 准备Driver运行环境并启动Driver，获取退出码
          val exitCode = prepareAndRunDriver()

          // 根据是否被强制杀死和退出码设置Driver最终状态
          finalState = if (exitCode == 0) {
            Some(DriverState.FINISHED)
          } else if (killed) {
            Some(DriverState.KILLED)
          } else {
            Some(DriverState.FAILED)
          }
        } catch {
          case e: Exception =>
            kill()
            finalState = Some(DriverState.ERROR)
            finalException = Some(e)
        } finally {
          // 移除注册的关闭钩子避免内存泄漏
          if (shutdownHook != null) {
            ShutdownHookManager.removeShutdownHook(shutdownHook)
          }
        }

        // 向Worker发送Driver状态变更通知
        worker.send(DriverStateChanged(driverId, finalState.get, finalException))
      }
    }.start()
  }

  /** 终止当前Driver进程，如果Driver尚未启动则阻止其启动 */
  private[worker] def kill(): Unit = {
    logInfo("Killing driver process!")
    killed = true
    synchronized {
      process.foreach { p =>
        // 尝试终止Driver进程，等待超时后强制杀死
        val exitCode = Utils.terminateProcess(p, driverTerminateTimeoutMs)
        if (exitCode.isEmpty) {
          logWarning(log"Failed to terminate driver process: ${MDC(PROCESS, p)} " +
              log". This process will likely be orphaned.")
        }
      }
    }
  }

  /**
   * 为当前Driver创建专属工作目录
   * 如果创建目录失败抛出异常
   * @return 创建完成的Driver工作目录对象
   */
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !Utils.createDirectory(driverDir)) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * 下载用户提交的Driver Jar包到工作目录，返回本地路径
   * 如果下载过程出错抛出异常
   * @param driverDir Driver工作目录
   * @return 下载完成的Jar包本地绝对路径
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarFileName = new URI(driverDesc.jarUrl).getPath.split("/").last
    val localJarFile = new File(driverDir, jarFileName)
    if (!localJarFile.exists()) { // 同一节点多个Worker可能已提前下载
      logInfo(log"Copying user jar ${MDC(JAR_URL, driverDesc.jarUrl)}" +
        log" to ${MDC(FILE_NAME, localJarFile)}")
      Utils.fetchFile(
        driverDesc.jarUrl,
        driverDir,
        conf,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)
      if (!localJarFile.exists()) { // 验证下载是否成功
        throw new IOException(
          s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
      }
    }
    localJarFile.getAbsolutePath
  }

  /**
   * 准备Driver运行环境并启动Driver进程
   * @return Driver进程退出码
   */
  private[worker] def prepareAndRunDriver(): Int = {
    val driverDir = createWorkingDirectory()
    val localJarFilename = downloadUserJar(driverDir)
    // 生成Driver资源配置文件
    val resourceFileOpt = prepareResourcesFile(SPARK_DRIVER_PREFIX, resources, driverDir)

    // 替换启动命令中的占位变量
    def substituteVariables(argument: String): String = argument match {
      case "{{WORKER_URL}}" => workerUrl
      case "{{USER_JAR}}" => localJarFilename
      case other => other
    }

    // 添加Driver资源文件配置到Java选项，供Driver启动时加载资源
    val javaOpts = driverDesc.command.javaOpts ++ resourceFileOpt.map(f =>
      Seq(s"-D${DRIVER_RESOURCES_FILE.key}=${f.getAbsolutePath}")).getOrElse(Seq.empty)
    // TODO: 如果支持提交多个Jar，需要在这里添加处理逻辑
    // 构建Driver进程启动器，替换占位变量
    val builder = CommandUtils.buildProcessBuilder(driverDesc.command.copy(javaOpts = javaOpts),
      securityManager, driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)

    // 将Driver日志的Web UI地址添加到进程环境变量
    val reverseProxy = conf.get(UI_REVERSE_PROXY)
    val workerUrlRef = UIUtils.makeHref(reverseProxy, driverId, workerWebUiUrl)
    builder.environment.put("SPARK_DRIVER_LOG_URL_STDOUT",
      s"$workerUrlRef/logPage/?driverId=$driverId&logType=stdout")
    builder.environment.put("SPARK_DRIVER_LOG_URL_STDERR",
      s"$workerUrlRef/logPage/?driverId=$driverId&logType=stderr")

    runDriver(builder, driverDir, driverDesc.supervise)
  }

  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    // 设置工作目录
    builder.directory(baseDir)
    // 进程初始化：重定向标准输出和错误到日志文件
    def initialize(process: Process): Unit = {
      // 重定向标准输出到stdout文件
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      // 重定向标准错误到stderr文件，先写入启动命令头信息
      val stderr = new File(baseDir, "stderr")
      val redactedCommand = Utils.redactCommandLineArgs(conf, builder.command.asScala.toSeq)
        .mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(redactedCommand, "=".repeat(40))
      Files.writeString(stderr.toPath, header, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    // 带重试机制运行Driver命令（开启监控时失败自动重启）
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  /**
   * 带重试退避机制执行Driver命令，开启监控时Driver失败自动重启
   * @param command 待执行的命令包装对象
   * @param initialize 进程初始化方法
   * @param supervise 是否开启Driver监控，开启后失败自动重启
   * @return 最终退出码
   */
  private[worker] def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int = {
    var exitCode = -1
    // 重试之间的等待时间（秒），用于指数退避
    var waitSeconds = 1
    // 运行超过该时长（秒）则认为启动成功，重置退避等待时间
    val successfulRunDuration = 5
    var keepTrying = !killed

    // 脱敏处理启动命令用于日志打印
    val redactedCommand = Utils.redactCommandLineArgs(conf, command.command)
      .mkString("\"", "\" \"", "\"")
    while (keepTrying) {
      logInfo(log"Launch Command: ${MDC(COMMAND, redactedCommand)}")

      synchronized {
        if (killed) { return exitCode }
        // 启动Driver进程
        process = Some(command.start())
        // 执行初始化：重定向输出流
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      // 等待进程退出获取退出码
      exitCode = process.get.waitFor()

      // 判断是否需要重试重启Driver
      keepTrying = supervise && exitCode != 0 && !killed
      if (keepTrying) {
        // 如果进程成功运行了足够长时间，重置退避时间
        if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000L) {
          waitSeconds = 1
        }
        logInfo(log"Command exited with status ${MDC(EXIT_CODE, exitCode)}," +
          log" re-launching after ${MDC(TIME_UNITS, waitSeconds)} s.")
        // 等待退避时间
        sleeper.sleep(waitSeconds)
        // 指数退避：下次等待时间翻倍
        waitSeconds = waitSeconds * 2
      }
    }

    exitCode
  }
}

/**
 * 睡眠接口，解耦睡眠逻辑方便测试
 */
private[deploy] trait Sleeper {
  def sleep(seconds: Int): Unit
}

/**
 * ProcessBuilder包装接口，因为ProcessBuilder是final类无法mock，通过该接口抽象方便测试
 */
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

/**
 * ProcessBuilderLike工厂对象
 */
private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala.toSeq
  }
}
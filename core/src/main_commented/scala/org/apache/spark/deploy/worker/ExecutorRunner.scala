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
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.deploy.StandaloneResourceUtils.prepareResourcesFile
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.SPARK_EXECUTOR_PREFIX
import org.apache.spark.internal.config.UI._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

/**
 * 管理单个Executor进程的执行生命周期，仅在Spark Standalone部署模式下使用。
 * 负责启动、监控Executor进程，并在进程退出后向Worker上报状态变化。
 */
private[deploy] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: RpcEndpointRef,
    val workerId: String,
    val webUiScheme: String,
    val host: String,
    val webUiPort: Int,
    val publicAddress: String,
    val sparkHome: File,
    val executorDir: File,
    val workerUrl: String,
    conf: SparkConf,
    val appLocalDirs: Seq[String],
    @volatile var state: ExecutorState.Value,
    val rpId: Int,
    val resources: Map[String, ResourceInformation] = Map.empty)
  extends Logging {

  private val fullId = appId + "/" + execId
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  // 终止Executor进程时的等待超时时间，单位毫秒
  private val EXECUTOR_TERMINATE_TIMEOUT_MS = 10 * 1000

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  private var shutdownHook: AnyRef = null

  /**
   * 启动ExecutorRunner，创建新线程启动并监控Executor进程
   */
  private[worker] def start(): Unit = {
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run(): Unit = { fetchAndRunExecutor() }
    }
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      // It's possible that we arrive here before calling `fetchAndRunExecutor`, then `state` will
      // be `ExecutorState.LAUNCHING`. In this case, we should set `state` to `FAILED`.
      if (state == ExecutorState.LAUNCHING || state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess("Worker shutting down") }
  }

  /**
   * 终止Executor进程，等待退出并通知Worker更新资源状态
   * @param message 导致Executor死亡的异常消息
   */
  private def killProcess(message: String): Unit = {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      // 尝试终止Executor进程，带超时等待
      exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning(log"Failed to terminate process: ${MDC(PROCESS, process)}" +
          log". This process will likely be orphaned.")
      }
    }
    try {
      // 向Worker发送Executor状态变化通知
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), exitCode))
    } catch {
      case e: IllegalStateException => logWarning(log"${MDC(ERROR, e.getMessage())}", e)
    }
  }

  /**
   * 停止当前ExecutorRunner，终止它启动的Executor进程
   */
  private[worker] def kill(): Unit = {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /**
   * 替换启动命令中的占位变量，如EXECUTOR_ID、CORES等
   * @param argument 原始包含占位符的参数字符串
   * @return 替换占位符后的实际参数
   */
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case "{{RESOURCE_PROFILE_ID}}" => rpId.toString
    case other => other
  }

  /**
   * 准备资源并启动Executor进程，监控其运行直到退出
   */
  private def fetchAndRunExecutor(): Unit = {
    try {
      // 生成资源分配信息文件
      val resourceFileOpt = prepareResourcesFile(SPARK_EXECUTOR_PREFIX, resources, executorDir)
      // Launch the process
      // 拼接启动参数，加入资源文件路径
      val arguments = appDesc.command.arguments ++ resourceFileOpt.map(f =>
        Seq("--resourcesFile", f.getAbsolutePath)).getOrElse(Seq.empty)
      // 替换Java选项中的应用ID和ExecutorID占位符
      val subsOpts = appDesc.command.javaOpts.map {
        Utils.substituteAppNExecIds(_, appId, execId.toString)
      }
      // 更新命令对象中的参数和Java选项
      val subsCommand = appDesc.command.copy(arguments = arguments, javaOpts = subsOpts)
      // 构建进程启动器
      val builder = CommandUtils.buildProcessBuilder(subsCommand, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      // 脱敏处理命令行参数，避免敏感信息泄露
      val redactedCommand = Utils.redactCommandLineArgs(conf, command.asScala.toSeq)
        .mkString("\"", "\" \"", "\"")
      logInfo(log"Launch command: ${MDC(COMMAND, redactedCommand)}")

      // 设置进程工作目录为Executor专属目录
      builder.directory(executorDir)
      // 设置环境变量：指定应用本地目录
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      // 避免在Spark Shell场景下生成多余的Scala父进程
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      // 构建Web UI日志访问地址
      val baseUrl =
        if (conf.get(UI_REVERSE_PROXY)) {
          conf.get(UI_REVERSE_PROXY_URL.key, "").stripSuffix("/") +
            s"/proxy/$workerId/logPage/?appId=$appId&executorId=$execId&logType="
        } else {
          s"$webUiScheme$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
        }
      // 将stderr和stdout的访问地址放入环境变量
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      // 启动Executor进程
      process = builder.start()
      // 写入日志文件头，记录启动命令
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        redactedCommand, "=".repeat(40))

      // Redirect its stdout and stderr to files
      // 将Executor标准输出重定向到文件，并启动日志追加器
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf, true)

      // 将Executor标准错误重定向到文件，写入文件头并启动日志追加器
      val stderr = new File(executorDir, "stderr")
      Files.writeString(stderr.toPath, header)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf, true)

      // 更新状态为运行中，通知Worker
      state = ExecutorState.RUNNING
      worker.send(ExecutorStateChanged(appId, execId, state, None, None))
      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      // 阻塞等待Executor进程退出，获取退出码
      val exitCode = process.waitFor()
      // 更新状态为已退出，通知Worker
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException =>
        logInfo(log"Runner thread for executor ${MDC(EXECUTOR_ID, fullId)} interrupted")
        state = ExecutorState.KILLED
        killProcess(s"Runner thread for executor $fullId interrupted")
      case e: Exception =>
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(s"Error running executor: $e")
    }
  }
}
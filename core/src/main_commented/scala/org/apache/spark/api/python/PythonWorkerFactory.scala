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

package org.apache.spark.api.python

import java.io.{DataInputStream, DataOutputStream, EOFException, File, InputStream}
import java.net.{InetAddress, InetSocketAddress, SocketException, StandardProtocolFamily, UnixDomainSocketAddress}
import java.net.SocketTimeoutException
import java.nio.channels._
import java.util.Arrays
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.Python.PYTHON_FACTORY_IDLE_WORKER_MAX_POOL_SIZE
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util.{RedirectThread, Utils}

/**
 * 封装Python工作进程网络通信通道的实体类
 * @param channel Python工作进程的Socket通信通道
 */
case class PythonWorker(channel: SocketChannel) {

  private[this] var selectorOpt: Option[Selector] = None
  private[this] var selectionKeyOpt: Option[SelectionKey] = None

  def selector: Selector = selectorOpt.orNull
  def selectionKey: SelectionKey = selectionKeyOpt.orNull

  /** 关闭当前选择器并取消注册的选择键 */
  private def closeSelector(): Unit = {
    selectionKeyOpt.foreach(_.cancel())
    selectorOpt.foreach(_.close())
  }

  /**
   * 刷新选择器，重新注册通道到新的选择器，用于复用空闲工作进程 */
  def refresh(): this.type = synchronized {
    closeSelector()
    if (channel.isBlocking) {
      selectorOpt = None
      selectionKeyOpt = None
    } else {
      val selector = Selector.open()
      selectorOpt = Some(selector)
      selectionKeyOpt =
        Some(channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE))
    }
    this
  }

  /** 停止工作进程，关闭选择器和通信通道 */
  def stop(): Unit = synchronized {
    closeSelector()
    Option(channel).foreach(_.close())
  }
}

/**
 * Python工作进程工厂，负责管理Python工作进程的创建、复用和销毁，支持守护进程模式复用，降低进程启动开销
 * 为PySpark在Executor端创建Python执行进程，提供给Python UDF执行能力
 * @param pythonExec Python解释器可执行文件路径
 * @param workerModule Python工作进程模块名称
 * @param daemonModule Python守护进程模块名称
 * @param envVars 启动Python进程需要的环境变量
 * @param useDaemonEnabled 是否启用守护进程模式
 */
private[spark] class PythonWorkerFactory(
    pythonExec: String,
    workerModule: String,
    daemonModule: String,
    envVars: Map[String, String],
    val useDaemonEnabled: Boolean)
  extends Logging { self =>

  /** 简化构造器，使用默认守护进程模块 */
  def this(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      useDaemonEnabled: Boolean) =
    this(pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule,
      envVars, useDaemonEnabled)

  import PythonWorkerFactory._

  // Because forking processes from Java is expensive, we prefer to launch a single Python daemon,
  // pyspark/daemon.py (by default) and tell it to fork new workers for our tasks. This daemon
  // currently only works on UNIX-based systems now because it uses signals for child management,
  // so we can also fall back to launching workers, pyspark/worker.py (by default) directly.
  /** 是否启用守护进程模式，Windows不支持fork所以强制关闭 */
  private val useDaemon = {
    // This flag is ignored on Windows as it's unable to fork.
    !Utils.isWindows && useDaemonEnabled
  }

  private val conf = SparkEnv.get.conf
  private val authHelper = new SocketAuthHelper(conf)
  /** 是否使用Unix域套接字进行进程间通信 */
  private val isUnixDomainSock = authHelper.isUnixDomainSock

  @GuardedBy("self")
  private var daemon: Process = null
  val daemonHost = InetAddress.getLoopbackAddress()
  @GuardedBy("self")
  private var daemonPort: Int = 0
  @GuardedBy("self")
  /** 存储守护进程模式下，当前活跃工作进程与对应进程句柄映射 */
  private val daemonWorkers = new mutable.WeakHashMap[PythonWorker, ProcessHandle]()
  @GuardedBy("self")
  private var daemonSockPath: String = _
  @GuardedBy("self")
  // Visible for testing
  private[spark] val idleWorkers = new mutable.Queue[PythonWorker]()
  @GuardedBy("self")
  /** 空闲工作进程池最大容量，配置限制避免占用过多资源 */
  private val maxIdleWorkerPoolSize =
    conf.get(PYTHON_FACTORY_IDLE_WORKER_MAX_POOL_SIZE)
  @GuardedBy("self")
  /** 最后一次活动时间戳，用于清理超时空闲工作进程 */
  private var lastActivityNs = 0L
  /** 启动后台线程定期清理超时空闲工作进程 */
  new MonitorThread().start()

  @GuardedBy("self")
  /** 存储直接启动模式下，工作进程与对应进程对象映射 */
  private val simpleWorkers = new mutable.WeakHashMap[PythonWorker, Process]()

  /** 合并后的最终PYTHONPATH环境变量，包含Spark Python依赖 */
  private val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  /**
   * 创建或复用一个Python工作进程，优先从空闲队列复用
   * @return (PythonWorker实例, 进程句柄Option)
   */
  def create(): (PythonWorker, Option[ProcessHandle]) = {
    if (useDaemon) {
      self.synchronized {
        // 从空闲队列取出存活的工作进程复用
        while (idleWorkers.nonEmpty) {
          val worker = idleWorkers.dequeue()
          daemonWorkers.get(worker).foreach { workerHandle =>
            if (workerHandle.isAlive()) {
              try {
                return (worker.refresh(), Some(workerHandle))
              } catch {
                case _: CancelledKeyException => /* pass */
              }
            }
          }
          logWarning(log"Worker ${MDC(WORKER, worker)} " +
            log"process from idle queue is dead, discarding.")
          stopWorker(worker)
        }
      }
      // 空闲队列无可用，通过守护进程新建工作进程
      createThroughDaemon()
    } else {
      // 不使用守护进程，直接启动新工作进程
      createSimpleWorker(blockingMode = false)
    }
  }

  /**
   * 通过已启动的Python守护进程创建新工作进程，复用守护进程fork新进程，避免Java层面fork开销
   * 仅在非Windows系统可用
   */
  private def createThroughDaemon(): (PythonWorker, Option[ProcessHandle]) = {

    def createWorker(): (PythonWorker, Option[ProcessHandle]) = {
      val socketChannel = if (isUnixDomainSock) {
        SocketChannel.open(UnixDomainSocketAddress.of(daemonSockPath))
      } else {
        SocketChannel.open(new InetSocketAddress(daemonHost, daemonPort))
      }

      val serverSelector = Selector.open()
      try {
        val timeOutMs = 60 * 1000
        socketChannel.configureBlocking(false)
        socketChannel.register(serverSelector, SelectionKey.OP_READ)
        if (serverSelector.select(timeOutMs) == 0) {
          throw new SocketTimeoutException(
            s"Timed out while waiting for the Python worker to connect back after $timeOutMs ms"
          )
        }
      } finally {
        serverSelector.close()
      }
      socketChannel.configureBlocking(true)

      // 读取工作进程PID
      val pid = new DataInputStream(Channels.newInputStream(socketChannel)).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }
      val processHandle = ProcessHandle.of(pid).orElseThrow(
        () => new IllegalStateException("Python daemon failed to launch worker.")
      )
      // 完成身份认证验证连接合法性
      authHelper.authToServer(socketChannel)
      socketChannel.configureBlocking(false)
      val worker = PythonWorker(socketChannel)
      daemonWorkers.put(worker, processHandle)
      (worker.refresh(), Some(processHandle))
    }

    self.synchronized {
      // 守护进程未启动则先启动
      startDaemon()

      // 尝试连接，失败一次则重启守护进程重试
      try {
        createWorker()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createWorker()
        case exc: SocketTimeoutException =>
          logWarning(exc.toString)
          logWarning("Lost connection to Python daemon, attempting to restart")
          stopDaemon()
          startDaemon()
          createWorker()
      }
    }
  }

  /**
   * 直接启动Python工作进程，不通过守护进程，每个任务一个进程，Windows只能用这种方式
   * @param blockingMode 是否使用阻塞IO模式
   * @return (PythonWorker实例, 进程句柄Option)
   */
  private[spark] def createSimpleWorker(
      blockingMode: Boolean): (PythonWorker, Option[ProcessHandle]) = {
    var serverSocketChannel: ServerSocketChannel = null
    lazy val sockPath = new File(
      authHelper.sockDir,
      s".${UUID.randomUUID()}.sock")
    try {
      if (isUnixDomainSock) {
        serverSocketChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
        sockPath.deleteOnExit()
        serverSocketChannel.bind(UnixDomainSocketAddress.of(sockPath.getPath))
      } else {
        serverSocketChannel = ServerSocketChannel.open()
        serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1)
      }

      // 创建Python工作进程，设置环境变量
      val pb = new ProcessBuilder(Arrays.asList(pythonExec, "-m", workerModule))
      val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
      if (jobArtifactUUID != "default") {
        val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
        f.mkdir()
        pb.directory(f)
      }
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
      workerEnv.put("PYTHONPATH", pythonPath)
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      // 设置Python进程不缓冲输出，保证日志实时输出
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      if (isUnixDomainSock) {
        workerEnv.put("PYTHON_WORKER_FACTORY_SOCK_PATH", sockPath.getPath)
        workerEnv.put("PYTHON_UNIX_DOMAIN_ENABLED", "True")
      } else {
        workerEnv.put("PYTHON_WORKER_FACTORY_PORT", serverSocketChannel.socket().getLocalPort
          .toString)
        workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      }
      if (Utils.preferIPv6) {
        workerEnv.put("SPARK_PREFER_IPV6", "True")
      }
      // 启动Python工作进程
      val workerProcess = pb.start()

      // 将Python进程标准输出和错误重定向到当前进程标准错误
      redirectStreamsToStderr(workerProcess.getInputStream, workerProcess.getErrorStream)

      // 等待工作进程反向连接，验证认证信息
      try {
        // Wait up to 10 seconds for client to connect.
        serverSocketChannel.configureBlocking(false)
        val serverSelector = Selector.open()
        serverSelector.register(serverSelector, SelectionKey.OP_ACCEPT)
        val socketChannel =
          if (serverSelector.select(10 * 1000) > 0) { // Wait up to 10 seconds.
            serverSocketChannel.accept()
          } else {
            throw new SocketTimeoutException(
              "Timed out while waiting for the Python worker to connect back")
          }
        // 验证客户端认证信息
        authHelper.authClient(socketChannel)
        // 读取工作进程PID
        val pid = new DataInputStream(Channels.newInputStream(socketChannel)).readInt()
        if (pid < 0) {
          throw new IllegalStateException("Python failed to launch worker with code " + pid)
        }
        if (!blockingMode) {
          socketChannel.configureBlocking(false)
        }
        val worker = PythonWorker(socketChannel)
        self.synchronized {
          simpleWorkers.put(worker, workerProcess)
        }
        (worker.refresh(), ProcessHandle.of(pid).toScala)
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker failed to connect back.", e)
      }
    } finally {
      if (serverSocketChannel != null) {
        serverSocketChannel.close()
        if (isUnixDomainSock) sockPath.delete()
      }
    }
  }

  /** 启动Python守护进程，负责后续fork工作进程 */
  private def startDaemon(): Unit = {
    self.synchronized {
      // 守护进程已经在运行，直接返回
      if (daemon != null) {
        return
      }

      try {
        // 创建Python守护进程构建参数
        val command = Arrays.asList(pythonExec, "-m", daemonModule, workerModule)
        val pb = new ProcessBuilder(command)
        val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
        if (jobArtifactUUID != "default") {
          val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
          f.mkdir()
          pb.directory(f)
        }
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        if (isUnixDomainSock) {
          workerEnv.put(
            "PYTHON_WORKER_FACTORY_SOCK_DIR",
            authHelper.sockDir)
          workerEnv.put("PYTHON_UNIX_DOMAIN_ENABLED", "True")
        } else {
          workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        }
        if (Utils.preferIPv6) {
          workerEnv.put("SPARK_PREFER_IPV6", "True")
        }
        // 设置不缓冲输出，保证日志实时输出
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        // 启动守护进程
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        try {
          // 读取守护进程返回的监听地址/端口
          if (isUnixDomainSock) {
            daemonSockPath = PythonWorkerUtils.readUTF(in)
          } else {
            daemonPort = in.readInt()
          }
        } catch {
          case _: EOFException if daemon.isAlive =>
            throw SparkCoreErrors.eofExceptionWhileReadPortNumberError(
              daemonModule)
          case _: EOFException =>
            throw SparkCoreErrors.
              eofExceptionWhileReadPortNumberError(daemonModule, Some(daemon.exitValue))
        }

        // 验证端口/套接字路径合法性，排除守护进程启动输出干扰
        val isMalformedPort = !isUnixDomainSock && (daemonPort < 1 || daemonPort > 0xffff)
        val isMalformedSockPath = isUnixDomainSock && !new File(daemonSockPath).exists()
        val errorMsg =
          if (isUnixDomainSock) daemonSockPath else f"$daemonPort (0x$daemonPort%08x)"
        if (isMalformedPort || isMalformedSockPath) {
          val exceptionMessage = f"""
            |Bad data in $daemonModule's standard output. Invalid
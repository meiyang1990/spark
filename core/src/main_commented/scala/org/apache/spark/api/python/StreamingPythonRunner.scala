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

/**
 * PySpark流处理模块的Python Worker启动器，负责为Python流处理函数初始化并管理Python工作进程
 * 核心职责：创建Python工作进程、传递初始化信息、维护通信通道、停止清理进程
 */
package org.apache.spark.api.python

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkEnv, SparkPythonException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PYTHON_WORKER_MODULE, PYTHON_WORKER_RESPONSE, SESSION_ID}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.{PYTHON_AUTH_SOCKET_TIMEOUT, PYTHON_UNIX_DOMAIN_SOCKET_ENABLED}


/**
 * StreamingPythonRunner工厂对象，用于创建StreamingPythonRunner实例
 */
private[spark] object StreamingPythonRunner {
  def apply(
      func: PythonFunction,
      connectUrl: String,
      sessionId: String,
      workerModule: String
  ): StreamingPythonRunner = {
    new StreamingPythonRunner(func, connectUrl, sessionId, workerModule)
  }
}

/**
 * StreamingPythonRunner实现类，负责管理Python流处理工作进程的生命周期
 * 为Python流处理函数创建独立Python工作进程，初始化通信通道，并负责停止和清理工作进程
 * @param func 用户定义的Python流处理函数
 * @param connectUrl Spark Connect连接地址
 * @param sessionId 当前流处理会话ID
 * @param workerModule Python工作进程入口模块名
 */
private[spark] class StreamingPythonRunner(
    func: PythonFunction,
    connectUrl: String,
    sessionId: String,
    workerModule: String) extends Logging {
  private val conf = SparkEnv.get.conf
  private val isUnixDomainSock = conf.get(PYTHON_UNIX_DOMAIN_SOCKET_ENABLED)
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)

  protected val envVars: java.util.Map[String, String] = func.envVars
  protected val pythonExec: String = func.pythonExec
  protected var pythonWorker: Option[PythonWorker] = None
  protected var pythonWorkerFactory: Option[PythonWorkerFactory] = None
  protected val pythonVer: String = func.pythonVer

  /**
   * 初始化Python流处理工作进程，建立通信通道并发送必要的初始化信息
   * @return 输出流和输入流组成的元组，用于后续与Python工作进程通信
   */
  def init(): (DataOutputStream, DataInputStream) = {
    logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Sending necessary information to the " +
      log"Python worker")
    val env = SparkEnv.get

    // 获取本地磁盘目录并设置环境变量
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir)

    // 配置相关环境变量
    envVars.put("SPARK_AUTH_SOCKET_TIMEOUT", authSocketTimeout.toString)
    envVars.put("SPARK_BUFFER_SIZE", bufferSize.toString)
    if (!connectUrl.isEmpty) {
      envVars.put("SPARK_CONNECT_LOCAL_URL", connectUrl)
    }
    envVars.put("SPARK_PYTHON_RUNTIME", "PYTHON_WORKER")

    // 创建Python工作进程工厂并启动工作进程
    val workerFactory =
      new PythonWorkerFactory(pythonExec, workerModule, envVars.asScala.toMap, false)
    val (worker: PythonWorker, _) = workerFactory.createSimpleWorker(blockingMode = true)
    pythonWorker = Some(worker)
    pythonWorkerFactory = Some(workerFactory)

    // 基于Socket通道创建IO流
    val socketChannel = pythonWorker.get.channel
    val stream = new BufferedOutputStream(Channels.newOutputStream(socketChannel), bufferSize)
    val dataIn = new DataInputStream(
      new BufferedInputStream(Channels.newInputStream(socketChannel), bufferSize))
    val dataOut = new DataOutputStream(stream)

    // 非Unix域套接字场景下延长初始化超时时间
    val originalTimeout = if (!isUnixDomainSock) {
      val timeout = socketChannel.socket().getSoTimeout()
      // 初始化阶段设置超时为5分钟
      socketChannel.socket().setSoTimeout(5 * 60 * 1000)
      Some(timeout)
    } else {
      None
    }

    val resFromPython = try {
      // 发送Python版本信息
      PythonWorkerUtils.writePythonVersion(pythonVer, dataOut)

      // 发送会话ID
      if (!sessionId.isEmpty) {
        PythonRDD.writeUTF(sessionId, dataOut)
      }

      // 发送用户Python函数到工作进程
      PythonWorkerUtils.writePythonFunction(func, dataOut)
      dataOut.flush()

      logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Reading initialization response from " +
        log"Python runner.")
      // 读取Python端初始化响应码
      dataIn.readInt()
    } catch {
      case e: java.net.SocketTimeoutException =>
        throw new StreamingPythonRunnerInitializationTimeoutException(e.getMessage)
      case e: Exception =>
        throw new StreamingPythonRunnerInitializationCommunicationException(e.getMessage)
    }

    // 恢复原始socket超时设置
    originalTimeout.foreach(v => socketChannel.socket().setSoTimeout(v))

    // 响应码非0表示初始化失败，读取错误信息并抛出异常
    if (resFromPython != 0) {
      val errMessage = PythonWorkerUtils.readUTF(dataIn)
      throw new StreamingPythonRunnerInitializationException(resFromPython, errMessage)
    }
    logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Runner initialization succeeded " +
      log"(returned ${MDC(PYTHON_WORKER_RESPONSE, resFromPython)}).")

    (dataOut, dataIn)
  }

  /**
   * 流处理Python工作进程初始化通信异常
   * @param errMessage 错误信息
   */
  class StreamingPythonRunnerInitializationCommunicationException(errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_COMMUNICATION_FAILURE",
      messageParameters = Map("msg" -> errMessage))

  /**
   * 流处理Python工作进程初始化超时异常
   * @param errMessage 错误信息
   */
  class StreamingPythonRunnerInitializationTimeoutException(errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_TIMEOUT_FAILURE",
      messageParameters = Map("msg" -> errMessage))

  /**
   * 流处理Python工作进程初始化失败异常
   * @param resFromPython Python端返回的错误码
   * @param errMessage 错误信息
   */
  class StreamingPythonRunnerInitializationException(resFromPython: Int, errMessage: String)
    extends SparkPythonException(
      errorClass = "STREAMING_PYTHON_RUNNER_INITIALIZATION_FAILURE",
      messageParameters = Map(
        "resFromPython" -> resFromPython.toString,
        "msg" -> errMessage))

  /**
   * 停止Python工作进程，清理资源
   */
  def stop(): Unit = {
    logInfo(log"[session: ${MDC(SESSION_ID, sessionId)}] Stopping streaming runner," +
      log" module: ${MDC(PYTHON_WORKER_MODULE, workerModule)}.")

    try {
      // 停止工作进程和工厂
      pythonWorkerFactory.foreach { factory =>
        pythonWorker.foreach { worker =>
          factory.stopWorker(worker)
          factory.stop()
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception when trying to kill worker", e)
    }
  }

  /**
   * 查询Python工作进程是否已经停止
   * @return 若工作进程和工厂已初始化，返回Some(是否停止)；否则返回None
   */
  def isWorkerStopped(): Option[Boolean] = {
    pythonWorkerFactory.flatMap { factory =>
      pythonWorker.map { worker =>
        factory.isWorkerStopped(worker)
      }
    }
  }
}
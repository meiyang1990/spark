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

import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashSet
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.Logger

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.Network.RPC_ASK_TIMEOUT
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.util.{SparkExitCode, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * 向Driver转发消息的代理端点
 * 
 * 当前提交失败不支持重试。在高可用模式下，客户端会向所有Master提交请求，由能处理的Master接收请求
 */
private class ClientEndpoint(
    override val rpcEnv: RpcEnv,
    driverArgs: ClientArguments,
    masterEndpoints: Seq[RpcEndpointRef],
    conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging {

  // 用于按指定时间发送消息的调度执行器
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("client-forward-message")
  // 为`Future`方法提供隐式执行上下文
  private val forwardMessageExecutionContext =
    ExecutionContext.fromExecutor(forwardMessageThread,
      t => t match {
        case ie: InterruptedException => // 正常退出
        case e: Throwable =>
          logError(log"${MDC(ERROR, e.getMessage)}", e)
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
      })

   private val lostMasters = new HashSet[RpcAddress]
   private var activeMasterEndpoint: RpcEndpointRef = null
   // 是否等待Driver应用执行完成后再退出客户端
   private val waitAppCompletion = conf.get(config.STANDALONE_SUBMIT_WAIT_APP_COMPLETION)
   // Driver状态轮询间隔（单位：毫秒）
   private val REPORT_DRIVER_STATUS_INTERVAL = 10000
   private var submittedDriverID = ""
   private var driverStatusReported = false

  /**
   * 从系统属性或Spark配置获取指定属性，优先使用系统属性
   * @param key 属性键
   * @param conf Spark配置对象
   * @return 属性值Option
   */
  private def getProperty(key: String, conf: SparkConf): Option[String] = {
    sys.props.get(key).orElse(conf.getOption(key))
  }

  override def onStart(): Unit = {
    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = config.DRIVER_CLASS_PATH.key
        // 解析用户配置的Driver类路径
        val classPathEntries = getProperty(classPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val libraryPathConf = config.DRIVER_LIBRARY_PATH.key
        // 解析用户配置的Driver本地库路径
        val libraryPathEntries = getProperty(libraryPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val extraJavaOptsConf = config.DRIVER_JAVA_OPTIONS.key
        // 解析用户额外的Java启动参数
        val extraJavaOpts = getProperty(extraJavaOptsConf, conf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)

        // 拼接Spark默认和用户自定义的Java参数
        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        // 构造Driver启动命令，使用Worker端的DriverWrapper包装用户主类
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)
        // 解析Driver资源需求
        val driverResourceReqs = ResourceUtils.parseResourceRequirements(conf,
          config.SPARK_DRIVER_PREFIX)
        // 构造Driver描述信息提交给Master
        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command,
          driverResourceReqs)
        asyncSendToMasterAndForwardReply[SubmitDriverResponse](
          RequestSubmitDriver(driverDescription))

      case "kill" =>
        // 处理杀死Driver的请求
        val driverId = driverArgs.driverId
        submittedDriverID = driverId
        asyncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
    logInfo("... waiting before polling master for driver state")
    // 启动定时任务定期轮询Driver状态
    forwardMessageThread.scheduleAtFixedRate(() => Utils.tryLogNonFatalError {
      monitorDriverStatus()
    }, 5000, REPORT_DRIVER_STATUS_INTERVAL, TimeUnit.MILLISECONDS)
  }

  /**
   * 异步发送消息给Master，并将回复转发给当前端点自身
   */
  private def asyncSendToMasterAndForwardReply[T: ClassTag](message: Any): Unit = {
    for (masterEndpoint <- masterEndpoints) {
      masterEndpoint.ask[T](message).onComplete {
        case Success(v) => self.send(v)
        case Failure(e) =>
          logWarning(log"Error sending messages to master " +
            log"${MDC(MASTER_URL, masterEndpoint)}", e)
      }(forwardMessageExecutionContext)
    }
  }

  /**
   * 向Master请求当前提交Driver的状态
   */
  private def monitorDriverStatus(): Unit = {
    if (submittedDriverID != "") {
      asyncSendToMasterAndForwardReply[DriverStatusResponse](RequestDriverStatus(submittedDriverID))
    }
  }

  /**
   * 处理并报告Driver状态，如果配置了不等待应用完成则直接退出JVM；否则仅在开启debug日志时报告状态
   */
  def reportDriverStatus(
      found: Boolean,
      state: Option[DriverState],
      workerId: Option[String],
      workerHostPort: Option[String],
      exception: Option[Exception]): Unit = {
    if (found) {
      // 使用driverStatusReported避免在等待完成模式下重复打印日志
      if (!driverStatusReported) {
        driverStatusReported = true
        logInfo(log"State of ${MDC(DRIVER_ID, submittedDriverID)}" +
          log" is ${MDC(DRIVER_STATE, state.get)}")
        // 如果Driver正在运行，输出运行节点信息
        (workerId, workerHostPort, state) match {
          case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
            logInfo(log"Driver running on ${MDC(HOST, hostPort)} (${MDC(WORKER_ID, id)})")
          case _ =>
        }
      }
      // 如果存在异常，直接退出
      exception match {
        case Some(e) =>
          logError("Exception from cluster", e)
          System.exit(-1)
        case _ =>
          // Driver已进入终止状态，退出客户端
          state.get match {
            case DriverState.FINISHED | DriverState.FAILED |
                 DriverState.ERROR | DriverState.KILLED =>
              logInfo(log"State of driver ${MDC(DRIVER_ID, submittedDriverID)}" +
                log" is ${MDC(DRIVER_STATE, state.get)}, exiting spark-submit JVM.")
              System.exit(0)
            case _ =>
              // 配置不等待完成则直接退出
              if (!waitAppCompletion) {
                logInfo("spark-submit not configured to wait for completion, " +
                  " exiting spark-submit JVM.")
                System.exit(0)
              } else {
                // 等待完成模式下继续轮询
                logDebug(log"State of driver ${MDC(DRIVER_ID, submittedDriverID)}" +
                  log" is ${MDC(DRIVER_STATE, state.get)}, " +
                  log"continue monitoring driver status.")
              }
          }
      }
    } else if (exception.exists(e => Utils.responseFromBackup(e.getMessage))) {
      // 忽略备份Master的响应
      logDebug(s"The status response is reported from a backup spark instance. So, ignored.")
    } else {
        // Master找不到该Driver，错误退出
        logError(log"ERROR: Cluster master did not recognize ${MDC(DRIVER_ID, submittedDriverID)}")
        System.exit(-1)
    }
  }
  override def receive: PartialFunction[Any, Unit] = {

    case SubmitDriverResponse(master, success, driverId, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
        submittedDriverID = driverId.get
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }

    case KillDriverResponse(master, driverId, success, message) =>
      logInfo(message)
      if (success) {
        activeMasterEndpoint = master
      } else if (!Utils.responseFromBackup(message)) {
        System.exit(-1)
      }

    case DriverStatusResponse(found, state, workerId, workerHostPort, exception) =>
      reportDriverStatus(found, state, workerId, workerHostPort, exception)
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(log"Error connecting to master ${MDC(HOST_PORT, remoteAddress)}.")
      lostMasters += remoteAddress
      // Note that this heuristic does not account for the fact that a Master can recover within
      // the lifetime of this client. Thus, once a Master is lost it is lost to us forever. This
      // is not currently a concern, however, because this client does not retry submissions.
      // 所有Master都不可用，错误退出
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (!lostMasters.contains(remoteAddress)) {
      logError(log"Error connecting to master (${MDC(HOST_PORT, remoteAddress)}).", cause)
      lostMasters += remoteAddress
      // 所有Master都不可用，错误退出
      if (lostMasters.size >= masterEndpoints.size) {
        logError("No master is available, exiting.")
        System.exit(-1)
      }
    }
  }

  override def onError(cause: Throwable): Unit = {
    logError("Error processing messages, exiting.", cause)
    System.exit(-1)
  }

  override def onStop(): Unit = {
    forwardMessageThread.shutdownNow()
  }
}

/**
 * Standalone集群中启动和终止Driver的可执行入口工具
 */
object Client {
  /**
   * Client入口主方法
   * @param args 命令行参数
   */
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }
    // scalastyle:on println
    new ClientApp().start(args, new SparkConf())
  }
}

/**
 * Spark Standalone集群提交Driver应用的客户端应用入口
 * 负责创建RPC环境，向Master提交Driver请求并监听状态
 */
private[spark] class ClientApp extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    // 解析命令行参数
    val driverArgs = new ClientArguments(args)

    // 如果未配置RPC请求超时，设置默认10秒
    if (!conf.contains(RPC_ASK_TIMEOUT)) {
      conf.set(RPC_ASK_TIMEOUT, "10s")
    }
    // 根据参数设置日志级别
    LogManager.getRootLogger.asInstanceOf[Logger].setLevel(driverArgs.logLevel)

    // 创建客户端RPC环境，绑定本地随机端口
    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))

    // 为每个Master地址创建端点引用
    val masterEndpoints = driverArgs.masters.map(RpcAddress.fromSparkURL).
      map(rpcEnv.setupEndpointRef(_, Master.ENDPOINT_NAME))
    // 注册Client端点到本地RPC环境
    rpcEnv.setupEndpoint("client",
      new ClientEndpoint(rpcEnv, driverArgs, masterEndpoints.toImmutableArraySeq, conf))

    // 阻塞等待终止，保持客户端运行以监听Driver状态
    rpcEnv.awaitTermination()
  }

}
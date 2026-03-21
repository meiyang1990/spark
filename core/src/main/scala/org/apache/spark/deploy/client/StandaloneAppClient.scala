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

package org.apache.spark.deploy.client

import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc._
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * 【学习笔记】StandaloneAppClient - Spark 应用程序与 Standalone 集群 Master 通信的客户端。
 *
 * <p>核心职责：
 * 1. 作为应用端代理，负责向 Spark Master 注册应用程序。
 * 2. 维护与 Master 的长连接，监听集群事件（如 Executor 添加/移除、Master 故障转移）。
 * 3. 封装对集群资源（Executor）的申请和释放请求。
 *
 * <p>工作流程：
 * - 启动时：通过 RpcEndpoint 在 Spark 集群中建立连接，尝试异步连接 Master。
 * - 注册成功：Master 返回 appId，通过回调通知应用程序。
 * - 集群交互：支持动态调整 Executor 资源，通过 sendToMaster 或 ask 机制与 Master 交互。
 *
 * @param masterUrls 集群 Master 的地址列表，格式为 spark://host:port。
 */
private[spark] class StandaloneAppClient(
    rpcEnv: RpcEnv,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: StandaloneAppClientListener,
    conf: SparkConf)
  extends Logging {

  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))

  // 注册超时限制（20秒）与最大重试次数
  private val REGISTRATION_TIMEOUT_SECONDS = 20
  private val REGISTRATION_RETRIES = 3

  // RPC 客户端引用、应用唯一 ID 及注册状态标记
  private val endpoint = new AtomicReference[RpcEndpointRef]
  private val appId = new AtomicReference[String]
  private val registered = new AtomicBoolean(false)

  // ClientEndpoint 是实际负责 RPC 交互的 RpcEndpoint 实现类
  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
    with Logging {

    // 当前连接的 Master 的引用
    private var master: Option[RpcEndpointRef] = None
    // 连接状态标记，避免重复触发回调
    private var alreadyDisconnected = false
    private val alreadyDead = new AtomicBoolean(false)
    // 异步注册任务的 Future，用于在超时或成功时取消
    private val registerMasterFutures = new AtomicReference[Array[JFuture[_]]]
    // 定时重试器
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    // 用于并发向多个 Master 进行注册的线程池
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length
    )

    // 用于周期性调度重试任务的单线程池
    private val registrationRetryThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    // 端点启动时立即发起对 Master 的首次注册
    override def onStart(): Unit = {
      try {
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          this.stop()
      }
    }

    /**
     * 并发向所有已知的 Master 发送注册申请，异步执行。
     */
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      for (masterAddress <- masterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {
              return
            }
            logInfo(
              log"Connecting to master ${MDC(LogKeys.MASTER_URL, masterAddress.toSparkURL)}...")
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(log"Failed to connect to master " +
              log"${MDC(MASTER_URL, masterAddress)}", e)
          }
        })
      }
    }

    /**
     * 周期性注册逻辑：每隔 REGISTRATION_TIMEOUT_SECONDS 秒重试，直到达到 REGISTRATION_RETRIES 次数。
     */
    private def registerWithMaster(nthRetry: Int): Unit = {
      registerMasterFutures.set(tryRegisterAllMasters())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) {
            // 注册成功则取消所有悬挂的重试任务
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) {
            // 重试耗尽，标记应用死亡
            markDead("All masters are unresponsive! Giving up.")
          } else {
            // 取消当前批次，发起下一次重试
            registerMasterFutures.get.foreach(_.cancel(true))
            registerWithMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }

    /**
     * Send a message to the current master. If we have not yet registered successfully with any
     * master, the message will be dropped.
     */
    private def sendToMaster(message: Any): Unit = {
      master match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(
          log"Drop ${MDC(MESSAGE, message)} because has not yet connected to master")
      }
    }

    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    // 消息处理入口：接收来自 Master 的集群状态通知
    override def receive: PartialFunction[Any, Unit] = {
      // 注册成功的回调
      case RegisteredApplication(appId_, masterRef) =>
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        listener.connected(appId.get)

      // 应用被移除
      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        this.stop()

      // 有新的 Executor 加入集群
      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = s"$appId/$id"
        logInfo(log"Executor added: ${MDC(LogKeys.EXECUTOR_ID, fullId)} on " +
          log"${MDC(LogKeys.WORKER_ID, workerId)} (${MDC(LogKeys.HOST_PORT, hostPort)}) " +
          log"with ${MDC(LogKeys.NUM_CORES, cores)} core(s)")
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      // Executor 状态更新回调（如终止或被清理）
      case ExecutorUpdated(id, state, message, exitStatus, workerHost) =>
        val fullId = s"$appId/$id"
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo(log"Executor updated: ${MDC(LogKeys.EXECUTOR_ID, fullId)} is now " +
          log"${MDC(LogKeys.EXECUTOR_STATE, state)}${MDC(LogKeys.MESSAGE, messageText)}")
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerHost)
        } else if (state == ExecutorState.DECOMMISSIONED) {
          listener.executorDecommissioned(fullId,
            ExecutorDecommissionInfo(message.getOrElse(""), workerHost))
        }

      // Worker 节点被移除
      case WorkerRemoved(id, host, message) =>
        logInfo(log"Master removed worker ${MDC(LogKeys.WORKER_ID, id)}: " +
          log"${MDC(LogKeys.MESSAGE, message)}")
        listener.workerRemoved(id, host, message)

      // 处理 Master 故障转移后的新 Master 切换
      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo(log"Master has changed, new master is at " +
          log"${MDC(LogKeys.MASTER_URL, masterRef.address.toSparkURL)}")
        master = Some(masterRef)
        alreadyDisconnected = false
        // 显式确认已切换到新 Master
        masterRef.send(MasterChangeAcknowledged(appId.get))
    }

    // 接收带返回值的请求，如停止应用或调整资源
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // 停止应用请求
      case StopAppClient =>
        markDead("Application has been stopped.")
        sendToMaster(UnregisterApplication(appId.get))
        context.reply(true)
        this.stop()

      // 请求申请 Executor
      case r: RequestExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      // 请求释放/杀死 Executor
      case k: KillExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    /**
     * 异步发起对 Master 的询问，并在回复到达时通过回调通知调用方。
     */
    private def askAndReplyAsync[T](
        endpointRef: RpcEndpointRef,
        context: RpcCallContext,
        msg: T): Unit = {
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // Cancelled
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    // 监控网络断开事件，通知应用已与 Master 断连
    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(
          log"Connection to ${MDC(MASTER_URL, address)} failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    // 网络错误回调
    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(log"Could not connect to ${MDC(MASTER_URL, address)}: " +
          log"${MDC(ERROR, cause)}")
      }
    }

    /**
     * 触发“连接断开”监听事件。
     */
    def markDisconnected(): Unit = {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    /**
     * 触发“应用死亡”监听事件（如 Master 显式移除或重试耗尽）。
     */
    def markDead(reason: String): Unit = {
      if (!alreadyDead.get) {
        listener.dead(reason)
        alreadyDead.set(true)
      }
    }

    // 停止服务时清理线程资源和等待中的任务
    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.get.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }

  // 启动 RPC Endpoint
  def start(): Unit = {
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }

  // 停止客户端，向 Master 发送销毁请求
  def stop(): Unit = {
    if (endpoint.get != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.get.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      endpoint.set(null)
    }
  }

  /**
   * 申请指定数量的 Executor，包含已在排队和运行中的 Executor。
   * @return 是否已收到 Master 的确认。
   */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    requestTotalExecutors(Map(appDescription.defaultProfile -> requestedTotal))
  }

  /**
   * 根据资源配置申请 Executor，允许为不同 Profile 定义期望数量。
   * @return 是否已收到 Master 的确认。
   */
  def requestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](RequestExecutors(appId.get, resourceProfileToTotalExecs))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * 通知 Master 销毁指定的 Executor 列表。
   * @return 是否已收到 Master 的确认。
   */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (endpoint.get != null && appId.get != null) {
      endpoint.get.ask[Boolean](KillExecutors(appId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

}

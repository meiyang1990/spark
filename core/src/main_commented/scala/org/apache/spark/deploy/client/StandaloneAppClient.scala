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
 * 文件说明: Standalone模式下，Spark应用与集群Master通信的客户端组件
 * 核心职责: 负责向Master注册应用、维护连接状态、接收集群事件并回调通知Spark调度器，处理Executor资源的申请与释放
 */
private[spark] class StandaloneAppClient(
    rpcEnv: RpcEnv,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: StandaloneAppClientListener,
    conf: SparkConf)
  extends Logging {

  // 将Master地址字符串转换为RPC地址对象数组
  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))

  // 注册超时时间（秒）
  private val REGISTRATION_TIMEOUT_SECONDS = 20
  // 注册最大重试次数
  private val REGISTRATION_RETRIES = 3

  // 保存客户端RPC端点引用
  private val endpoint = new AtomicReference[RpcEndpointRef]
  // 保存应用ID
  private val appId = new AtomicReference[String]
  // 标记应用是否注册成功
  private val registered = new AtomicBoolean(false)

  /**
   * 实际负责与Master进行RPC通信的端点类，处理所有来自Master的消息和事件
   */
  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
    with Logging {

    // 当前连接的Master端点引用
    private var master: Option[RpcEndpointRef] = None
    // 标记是否已经通知过断连，避免重复回调
    private var alreadyDisconnected = false
    // 标记应用是否已经死亡，避免重复回调
    private val alreadyDead = new AtomicBoolean(false)
    // 保存并发注册多个Master的任务Future，用于超时取消
    private val registerMasterFutures = new AtomicReference[Array[JFuture[_]]]
    // 保存定时重试任务的Future，用于停止时取消
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    // 用于并发向多个Master发起注册的线程池
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      masterRpcAddresses.length
    )

    // 用于执行注册重试定时任务的单线程调度池
    private val registrationRetryThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    /**
     * RPC端点启动后，立即发起第一次注册尝试
     */
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
     * 并发向所有配置的Master发起注册请求，异步执行
     * @return 所有注册任务的Future数组
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
            // 建立到该Master的端点引用
            val masterRef = rpcEnv.setupEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            // 发送应用注册请求
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // 任务被取消，无需处理
            case NonFatal(e) => logWarning(log"Failed to connect to master " +
              log"${MDC(MASTER_URL, masterAddress)}", e)
          }
        })
      }
    }

    /**
     * 定时重试注册逻辑，每次超时后发起下一轮重试，直到达到最大重试次数
     * @param nthRetry 当前是第几次重试
     */
    private def registerWithMaster(nthRetry: Int): Unit = {
      // 发起当前轮次的所有Master注册
      registerMasterFutures.set(tryRegisterAllMasters())
      // 定时检查注册结果
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) {
            // 注册成功，取消所有未完成的注册任务，关闭线程池
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) {
            // 达到最大重试次数，所有Master都无响应，标记应用死亡
            markDead("All masters are unresponsive! Giving up.")
          } else {
            // 取消当前轮次任务，发起下一轮重试
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

    // 判断地址是否为配置的Master地址之一
    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    /**
     * 处理来自Master的单向通知消息
     */
    override def receive: PartialFunction[Any, Unit] = {
      // 应用注册成功，Master返回应用ID
      case RegisteredApplication(appId_, masterRef) =>
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        // 通知监听器连接建立完成
        listener.connected(appId.get)

      // 应用被Master移除
      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        this.stop()

      // Master分配了新的Executor，通知监听器
      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = s"$appId/$id"
        logInfo(log"Executor added: ${MDC(LogKeys.EXECUTOR_ID, fullId)} on " +
          log"${MDC(LogKeys.WORKER_ID, workerId)} (${MDC(LogKeys.HOST_PORT, hostPort)}) " +
          log"with ${MDC(LogKeys.NUM_CORES, cores)} core(s)")
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      // Master通知Executor状态更新
      case ExecutorUpdated(id, state, message, exitStatus, workerHost) =>
        val fullId = s"$appId/$id"
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo(log"Executor updated: ${MDC(LogKeys.EXECUTOR_ID, fullId)} is now " +
          log"${MDC(LogKeys.EXECUTOR_STATE, state)}${MDC(LogKeys.MESSAGE, messageText)}")
        // Executor已经终止，通知监听器移除
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerHost)
        // Executor被停用，通知监听器进行去commission处理
        } else if (state == ExecutorState.DECOMMISSIONED) {
          listener.executorDecommissioned(fullId,
            ExecutorDecommissionInfo(message.getOrElse(""), workerHost))
        }

      // Master通知Worker节点被移除
      case WorkerRemoved(id, host, message) =>
        logInfo(log"Master removed worker ${MDC(LogKeys.WORKER_ID, id)}: " +
          log"${MDC(LogKeys.MESSAGE, message)}")
        listener.workerRemoved(id, host, message)

      // Master发生故障转移，切换到新的Master
      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo(log"Master has changed, new master is at " +
          log"${MDC(LogKeys.MASTER_URL, masterRef.address.toSparkURL)}")
        master = Some(masterRef)
        alreadyDisconnected = false
        // 向新Master确认切换完成
        masterRef.send(MasterChangeAcknowledged(appId.get))
    }

    /**
     * 处理需要回复的请求消息
     */
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      // 停止客户端请求
      case StopAppClient =>
        markDead("Application has been stopped.")
        // 向Master发送取消注册请求
        sendToMaster(UnregisterApplication(appId.get))
        context.reply(true)
        this.stop()

      // 申请Executor资源请求
      case r: RequestExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      // 杀死指定Executor请求
      case k: KillExecutors =>
        master match {
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    /**
     * 异步向Master发送请求，并在收到回复后回传给调用上下文
     * @param endpointRef Master端点引用
     * @param context RPC调用上下文，用于回复结果
     * @param msg 请求消息
     */
    private def askAndReplyAsync[T](
        endpointRef: RpcEndpointRef,
        context: RpcCallContext,
        msg: T): Unit = {
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // 取消，无需处理
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    /**
     * 与Master连接断开，通知监听器
     */
    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(
          log"Connection to ${MDC(MASTER_URL, address)} failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    /**
     * 网络错误处理，记录日志
     */
    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(log"Could not connect to ${MDC(MASTER_URL, address)}: " +
          log"${MDC(ERROR, cause)}")
      }
    }

    /**
     * 标记连接断开，通知监听器
     */
    def markDisconnected(): Unit = {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    /**
     * 标记应用死亡，通知监听器，终止客户端
     * @param reason 死亡原因
     */
    def markDead(reason: String): Unit = {
      if (!alreadyDead.get) {
        listener.dead(reason)
        alreadyDead.set(true)
      }
    }

    /**
     * 停止端点，清理所有线程资源和任务
     */
    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.get.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }

  /**
   * 启动App客户端，注册RPC端点
   */
  def start(): Unit = {
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }

  /**
   * 停止App客户端，向Master发送停止请求，清理资源
   */
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
   * 请求调整总Executor数量，使用默认资源配置
   * @param requestedTotal 期望的总Executor数量（包含已运行的）
   * @return 异步Future，包含Master是否确认请求
   */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    requestTotalExecutors(Map(appDescription.defaultProfile -> requestedTotal))
  }

  /**
   * 请求根据不同资源配置调整总Executor数量
   * @param resourceProfileToTotalExecs 各资源配置对应的期望总Executor数量
   * @return 异步Future，包含Master是否确认请求
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
   * 请求杀死指定的一批Executor
   * @param executorIds 要杀死的ExecutorID列表
   * @return 异步Future，包含Master是否确认请求
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
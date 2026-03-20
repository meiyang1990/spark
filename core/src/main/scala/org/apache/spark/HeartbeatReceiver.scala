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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{HashMap, Map}
import scala.concurrent.Future

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.Network
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
 * Executor 向 Driver 发送的心跳消息。
 * 该消息被多个内部组件共享，用于传达活跃度或正在运行任务的执行信息。
 * 同时会清除超过 spark.network.timeout 未发送心跳的主机。
 * spark.executor.heartbeatInterval 应显著小于 spark.network.timeout。
 */
private[spark] case class Heartbeat(
    executorId: String,
    // taskId -> accumulator updates
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId,
    // (stageId, stageAttemptId) -> executor metric peaks
    executorUpdates: Map[(Int, Int), ExecutorMetrics])

/**
 * SparkContext 用来通知 HeartbeatReceiver：SparkContext.taskScheduler 已创建的事件。
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * 位于 Driver 端，接收来自 Executor 的心跳。
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with IsolatedThreadSafeRpcEndpoint with Logging {

  def this(sc: SparkContext) = {
    this(sc, new SystemClock)
  }

  sc.listenerBus.addToManagementQueue(this)

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  private val executorLastSeen = new HashMap[String, Long]

  private val executorTimeoutMs = sc.conf.get(
    config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT
  ).getOrElse(Utils.timeStringAsMs(s"${sc.conf.get(Network.NETWORK_TIMEOUT)}s"))

  private val checkTimeoutIntervalMs = sc.conf.get(Network.NETWORK_TIMEOUT_INTERVAL)

  private val executorHeartbeatIntervalMs = sc.conf.get(config.EXECUTOR_HEARTBEAT_INTERVAL)

  if (sc.conf.get(config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT).isEmpty) {
    require(checkTimeoutIntervalMs <= executorTimeoutMs,
      s"${Network.NETWORK_TIMEOUT_INTERVAL.key} should be less than or " +
        s"equal to ${Network.NETWORK_TIMEOUT.key}.")
  } else {
    require(checkTimeoutIntervalMs <= executorTimeoutMs,
      s"${Network.NETWORK_TIMEOUT_INTERVAL.key} should be less than or " +
        s"equal to ${config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key}.")
  }
  require(executorHeartbeatIntervalMs <= executorTimeoutMs,
    s"${config.EXECUTOR_HEARTBEAT_INTERVAL.key} should be less than or " +
      s"equal to ${config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key}")

  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" 用于执行一些快速操作。在其中运行的操作不应长时间阻塞线程。
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  override def onStart(): Unit = {
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { Option(self).foreach(_.ask[Boolean](ExpireDeadHosts)) },
      0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  // 处理各类 RPC 消息并回复
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // 本地发送和接收的消息
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      context.reply(true)
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)

    // 来自 Executor 的心跳消息
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId, executorUpdates) =>
      var reregisterBlockManager = !sc.isStopped
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId)) {
          executorLastSeen(executorId) = clock.getTimeMillis()
          // 在事件循环线程中处理心跳，避免阻塞 RPC 线程
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId, executorUpdates)
              reregisterBlockManager &= unknownExecutor
              val response = HeartbeatResponse(reregisterBlockManager)
              context.reply(response)
            }
          })
        } else {
          // 收到已移除 Executor 的飞行中心跳，非错误情况，不输出警告（SPARK-4134）
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager))
        }
      } else {
        // Executor 会在发送第一次心跳前等待数秒，此情况极少发生
        logWarning(log"Dropping ${MDC(HEARTBEAT, heartbeat)} " +
          log"because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager))
      }
  }

  /**
   * 发送 ExecutorRegistered 到事件循环以注册新 Executor。仅用于测试。
   *
   * @return 如果 HeartbeatReceiver 已停止则返回 None。否则返回一个 Future 表示操作是否成功。
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /** 如果心跳接收器未停止，则通知它 Executor 已注册 */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * 发送 ExecutorRemoved 到事件循环以移除 Executor。仅用于测试。
   *
   * @return 如果 HeartbeatReceiver 已停止则返回 None。否则返回一个 Future 表示操作是否成功。
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * 如果心跳接收器未停止，通知它 Executor 已被移除以避免多余的错误日志。
   *
   * 注意：必须在 Executor 实际移除之后才能执行此操作，以避免以下竞态条件：
   * 如果过早地从数据结构中移除 Executor 的元数据，在 Executor 实际移除之前
   * 可能收到飞行中的心跳，这会导致后续仍将 Executor 标记为死主机并输出大量错误日志。
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }

  // 检查并过期长时间未发送心跳的 Executor
  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(log"Removing executor ${MDC(EXECUTOR_ID, executorId)} " +
          log"with no recent heartbeats: " +
          log"${MDC(TIME_UNITS, now - lastSeenMs)} ms exceeds timeout " +
          log"${MDC(EXECUTOR_TIMEOUT, executorTimeoutMs)} ms")
        // 异步 kill Executor 以避免阻塞当前线程
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // 使用 killAndReplaceExecutor 而非 killExecutor，以便获取替代 Executor（SPARK-8119）
            sc.killAndReplaceExecutor(executorId)
            // SPARK-27348: 对于非优雅关闭的 Executor，手动从 CoarseGrainedSchedulerBackend 移除
            // 以确保：1) 显式移除信息而非等待断连消息 2) 调用 executorLost() 使分配到该 Executor 的任务失败
            sc.schedulerBackend match {
              case backend: CoarseGrainedSchedulerBackend =>
                backend.driverEndpoint.send(RemoveExecutor(executorId,
                  ExecutorProcessLost(
                    s"Executor heartbeat timed out after ${now - lastSeenMs} ms")))
              case _: LocalSchedulerBackend =>
              case other => throw new UnsupportedOperationException(
                s"Unknown scheduler backend: ${other.getClass}")
            }
          }
        })
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}

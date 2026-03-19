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
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    // taskId -> accumulator updates
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId,
    // (stageId, stageAttemptId) -> executor metric peaks
    executorUpdates: Map[(Int, Int), ExecutorMetrics])

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
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

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
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
   * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
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

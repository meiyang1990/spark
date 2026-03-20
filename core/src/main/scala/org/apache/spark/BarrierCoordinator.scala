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

import java.util.{ArrayList, Collections, TimerTask}
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}
import java.util.function.Consumer

import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerStageCompleted}
import org.apache.spark.util.ThreadUtils

/**
 * 屏障阶段尝试的唯一标识符。
 * 每个屏障阶段尝试在同一时间最多只能有一个活跃的 barrier() 调用，
 * 因此可以使用 (stageId, stageAttemptId) 来标识发起 barrier() 调用的阶段尝试。
 */
private case class ContextBarrierId(stageId: Int, stageAttemptId: Int) {
  override def toString: String = s"Stage $stageId (Attempt $stageAttemptId)"
}

/**
 * 屏障协调器，处理来自 BarrierTaskContext 的所有全局同步请求。
 * 
 * 每个全局同步请求由 `BarrierTaskContext.barrier()` 生成，
 * 并通过 stageId + stageAttemptId + barrierEpoch 唯一标识。
 * 当一组 `barrier()` 调用的所有请求都收到后，回复所有阻塞的全局同步请求。
 * 如果协调器在配置的时间内无法收集到足够的全局同步请求，
 * 则使所有请求失败并返回带有超时消息的异常。
 */
private[spark] class BarrierCoordinator(
    timeoutInSecs: Long,
    listenerBus: LiveListenerBus,
    override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  // TODO SPARK-25030 在提交给 SparkSubmit 的 mainClass 中创建 Timer() 会导致无法获取结果，需要修复该问题。
  private lazy val timer = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "BarrierCoordinator barrier epoch increment timer")
  private val timerFutures = Collections.synchronizedList(new ArrayList[ScheduledFuture[_]])

  // 监听 StageCompleted 事件，清理对应的 ContextBarrierState
  private val listener = new SparkListener {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageInfo = stageCompleted.stageInfo
      val barrierId = ContextBarrierId(stageInfo.stageId, stageInfo.attemptNumber())
      // 清理已完成阶段尝试的 ContextBarrierState
      cleanupBarrierStage(barrierId)
    }
  }

  // 记录所有发起 barrier() 调用的活跃阶段尝试及其对应的内部状态
  private val states = new ConcurrentHashMap[ContextBarrierId, ContextBarrierState]

  override def onStart(): Unit = {
    super.onStart()
    listenerBus.addToStatusQueue(listener)
  }

  override def onStop(): Unit = {
    try {
      states.forEachValue(1, clearStateConsumer)
      states.clear()
      listenerBus.removeListener(listener)
    } finally {
      timerFutures.asScala.foreach(_.cancel(false))
      timerFutures.clear()
      ThreadUtils.shutdown(timer)
      super.onStop()
    }
  }

  /**
   * barrier() 调用的当前状态。
   * 当新的阶段尝试发出 barrier() 调用时创建状态，在阶段完成时回收。
   *
   * @param barrierId 发起 barrier() 调用的屏障阶段标识符
   * @param numTasks 屏障阶段的任务数量，该阶段的所有 barrier() 调用需要收集 `numTasks` 个请求才能成功
   */
  private class ContextBarrierState(
      val barrierId: ContextBarrierId,
      val numTasks: Int) {

    // 一个屏障阶段尝试可能包含多次 barrier() 调用，`barrierEpoch` 用于标识每次 barrier() 调用。
    // 当 barrier() 调用成功时应递增，当 barrier() 调用因超时失败时应重置。
    private var barrierEpoch: Int = 0

    // 已发起阻塞 runBarrier() 调用的屏障任务的 RPCCallContext 数组
    private val requesters: ArrayBuffer[RpcCallContext] = new ArrayBuffer[RpcCallContext](numTasks)

    // 已发起阻塞 runBarrier() 调用的各屏障任务发送的消息。
    // 同步完成后，这些消息将回复给所有任务。
    private val messages = Array.ofDim[String](numTasks)

    // 从此次屏障同步内的任务收集的请求方法。
    // 所有任务应确保在同一屏障同步阶段调用相同的方法。
    // 换句话说，对于合法的屏障同步，requestMethods 的大小应始终为 1。
    // 否则，如果 requestMethods 的大小大于 1，则屏障同步将失败。
    private val requestMethods = new HashSet[RequestMethod.Value]

    // 确保 barrier() 调用可能超时的定时任务
    private var timerTask: TimerTask = null

    // 为 barrier() 调用初始化定时任务
    private def initTimerTask(state: ContextBarrierState): Unit = {
      timerTask = new TimerTask {
        override def run(): Unit = state.synchronized {
          // 当前 barrier() 调用超时，使所有同步请求失败
          requesters.foreach(_.sendFailure(new SparkException("The coordinator didn't get all " +
            s"barrier sync requests for barrier epoch $barrierEpoch from $barrierId within " +
            s"$timeoutInSecs second(s).")))
          cleanupBarrierStage(barrierId)
        }
      }
    }

    // 取消当前活跃的定时任务并释放资源
    private def cancelTimerTask(): Unit = {
      timerFutures.asScala.foreach(_.cancel(false))
      timerFutures.clear()
    }

    // 处理全局同步请求。
    // 如果在配置的时间内收集到足够的请求，则 barrier() 调用成功；
    // 否则使所有挂起的请求失败。
    def handleRequest(requester: RpcCallContext, request: RequestToSync): Unit = synchronized {
      val taskId = request.taskAttemptId
      val epoch = request.barrierEpoch
      val curReqMethod = request.requestMethod
      requestMethods.add(curReqMethod)
      if (requestMethods.size > 1) {
        val error = new SparkException(s"Different barrier sync types found for the " +
          s"sync $barrierId: ${requestMethods.mkString(", ")}. Please use the " +
          s"same barrier sync type within a single sync.")
        (requesters :+ requester).foreach(_.sendFailure(error))
        clear()
        return
      }

      // 要求从 BarrierTaskContext 正确设置任务数量
      require(request.numTasks == numTasks, s"Number of tasks of $barrierId is " +
        s"${request.numTasks} from Task $taskId, previously it was $numTasks.")

      // 检查来自屏障任务的 epoch 是否匹配当前 barrierEpoch
      logInfo(log"Current barrier epoch for ${MDC(BARRIER_ID, barrierId)}" +
        log" is ${MDC(BARRIER_EPOCH, barrierEpoch)}.")
      if (epoch != barrierEpoch) {
        requester.sendFailure(new SparkException(s"The request to sync of $barrierId with " +
          s"barrier epoch $barrierEpoch has already finished. Maybe task $taskId is not " +
          "properly killed."))
      } else {
        // 如果这是 barrier() 调用收到的第一条同步消息，则启动定时器以确保同步可能超时
        if (requesters.isEmpty) {
          initTimerTask(this)
          val timerFuture = timer.schedule(timerTask, timeoutInSecs, TimeUnit.SECONDS)
          timerFutures.add(timerFuture)
        }
        // 将请求者添加到待回复的 RPCCallContexts 数组中
        requesters += requester
        messages(request.partitionId) = request.message
        logInfo(log"Barrier sync epoch ${MDC(BARRIER_EPOCH, barrierEpoch)}" +
          log" from ${MDC(BARRIER_ID, barrierId)} received update from Task" +
          log" ${MDC(TASK_ID, taskId)}, current progress:" +
          log" ${MDC(REQUESTER_SIZE, requesters.size)}/${MDC(NUM_REQUEST_SYNC_TASK, numTasks)}.")
        if (requesters.size == numTasks) {
          requesters.foreach(_.reply(messages.clone()))
          // 当前 barrier() 调用成功完成，清理 ContextBarrierState 并递增 barrier epoch
          logInfo(log"Barrier sync epoch ${MDC(BARRIER_EPOCH, barrierEpoch)}" +
            log" from ${MDC(BARRIER_ID, barrierId)} received all updates from" +
            log" tasks, finished successfully.")
          barrierEpoch += 1
          requesters.clear()
          requestMethods.clear()
          cancelTimerTask()
        }
      }
    }

    // 清理屏障阶段尝试的内部状态
    def clear(): Unit = synchronized {
      // 全局同步失败，预期阶段将重试另一次尝试，
      // 来自当前阶段尝试的所有同步消息都应失败
      barrierEpoch = -1
      requesters.clear()
      cancelTimerTask()
    }
  }

  // 清理对应特定阶段尝试的 [[ContextBarrierState]]
  private def cleanupBarrierStage(barrierId: ContextBarrierId): Unit = {
    val barrierState = states.remove(barrierId)
    if (barrierState != null) {
      barrierState.clear()
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case request @ RequestToSync(numTasks, stageId, stageAttemptId, _, _, _, _, _) =>
      // 获取或初始化对应阶段尝试的 ContextBarrierState
      val barrierId = ContextBarrierId(stageId, stageAttemptId)
      states.computeIfAbsent(barrierId,
        (key: ContextBarrierId) => new ContextBarrierState(key, numTasks))
      val barrierState = states.get(barrierId)

      barrierState.handleRequest(context, request)
  }

  private val clearStateConsumer = new Consumer[ContextBarrierState] {
    override def accept(state: ContextBarrierState) = state.clear()
  }
}

private[spark] sealed trait BarrierCoordinatorMessage extends Serializable

/**
 * 来自 BarrierTaskContext 的全局同步请求消息。
 * 通过 stageId + stageAttemptId + barrierEpoch 唯一标识。
 *
 * @param numTasks 协调器需要收到的全局同步请求总数
 * @param stageId 当前 Stage ID
 * @param stageAttemptId 当前 Stage 尝试 ID
 * @param taskAttemptId 当前任务唯一 ID
 * @param barrierEpoch barrier() 调用的 ID，一个任务可能包含多次 barrier() 调用
 * @param partitionId 任务所分配的分区 ID
 * @param message 从 BarrierTaskContext 发送的消息
 * @param requestMethod 触发协调器的 BarrierTaskContext 方法（BARRIER 或 ALL_GATHER）
 */
private[spark] case class RequestToSync(
  numTasks: Int,
  stageId: Int,
  stageAttemptId: Int,
  taskAttemptId: Long,
  barrierEpoch: Int,
  partitionId: Int,
  message: String,
  requestMethod: RequestMethod.Value) extends BarrierCoordinatorMessage

private[spark] object RequestMethod extends Enumeration {
  val BARRIER, ALL_GATHER = Value
}

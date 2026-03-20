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

import java.io.Closeable
import java.util.{Properties, TimerTask}
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success => ScalaSuccess, Try}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._

/**
 * :: Experimental ::
 * Barrier Stage 中任务专用的 TaskContext，提供额外的上下文信息和同步工具。
 * 在 barrier 任务中通过 BarrierTaskContext.get() 获取当前上下文。
 */
@Experimental
@Since("2.4.0")
class BarrierTaskContext private[spark] (
    taskContext: TaskContext) extends TaskContext with Logging {

  import BarrierTaskContext._

  // 获取 Driver 端 BarrierCoordinator 的 RPC 引用，所有 barrier() 调用都发送到该端点
  private val barrierCoordinator: RpcEndpointRef = {
    val env = SparkEnv.get
    RpcUtils.makeDriverRef("barrierSync", env.conf, env.rpcEnv)
  }

  // 本地 barrier 纪元计数器，每次 barrier() 调用后递增，需与 Driver 端保持一致
  private var barrierEpoch = 0

  /** 记录 barrier 同步的进度日志 */
  private def logProgressInfo(msg: MessageWithContext, startTime: Option[Long]): Unit = {
    val waitMsg = startTime.fold(log"")(st => log", waited " +
      log"for ${MDC(TOTAL_TIME, System.currentTimeMillis() - st)} ms,")
    logInfo(log"Task ${MDC(TASK_ATTEMPT_ID, taskAttemptId())}" +
      log" from Stage ${MDC(STAGE_ID, stageId())}" +
      log"(Attempt ${MDC(STAGE_ATTEMPT_ID, stageAttemptNumber())}) " +
      msg + waitMsg +
      log" current barrier epoch is ${MDC(BARRIER_EPOCH, barrierEpoch)}.")
  }

  /**
   * 执行全局 barrier 同步的核心方法。
   * 向 BarrierCoordinator 发送同步请求，等待所有 barrier 任务到达同步点。
   * 每秒检查一次任务是否被 kill，被 kill 则中止 RPC。
   */
  private def runBarrier(message: String, requestMethod: RequestMethod.Value): Array[String] = {
    logProgressInfo(log"has entered the global sync", None)
    logTrace("Current callSite: " + Utils.getCallSite())

    val startTime = System.currentTimeMillis()
    val timerTask = new TimerTask {
      override def run(): Unit = {
        logProgressInfo(
          log"waiting under the global sync since ${MDC(TIME, startTime)}",
          Some(startTime)
        )
      }
    }
    // 每分钟打印一次等待日志
    val timerFuture = timer.scheduleAtFixedRate(timerTask, 1, 1, TimeUnit.MINUTES)

    try {
      // 发送可中止的 RPC 请求，设置超长超时（365天），实际超时由 BarrierCoordinator 控制
      val abortableRpcFuture = barrierCoordinator.askAbortable[Array[String]](
        message = RequestToSync(numPartitions(), stageId(), stageAttemptNumber(), taskAttemptId(),
          barrierEpoch, partitionId(), message, requestMethod),
        timeout = new RpcTimeout(365.days, "barrierTimeout"))

      // 每秒轮询一次：检查 RPC 是否完成，同时检查任务是否被 kill
      while (!abortableRpcFuture.future.isCompleted) {
        try {
          Thread.sleep(1000)
        } catch {
          case _: InterruptedException => // 任务被 Driver kill
        } finally {
          // 检查任务是否被中断，被中断则中止 RPC
          Try(taskContext.killTaskIfInterrupted()) match {
            case ScalaSuccess(_) => // 任务仍在正常运行
            case Failure(e) => abortableRpcFuture.abort(e)
          }
        }
      }
      // 获取所有 barrier 任务汇聚的消息数组
      val messages = abortableRpcFuture.future.value.get.get

      barrierEpoch += 1
      logProgressInfo(log"finished global sync successfully", Some(startTime))
      messages
    } catch {
      case e: SparkException =>
        logProgressInfo(log"failed to perform global sync", Some(startTime))
        throw e
    } finally {
      timerFuture.cancel(false)
      timer.purge()
    }
  }

  /**
   * :: Experimental ::
   * 设置全局同步屏障，阻塞直到同一 Stage 中所有任务都到达此屏障。
   * 类似 MPI 的 MPI_Barrier 函数。
   *
   * 注意：在 barrier stage 中，每个任务在所有可能的代码分支中必须有相同数量的 barrier() 调用，
   * 否则可能导致任务挂起或超时。
   */
  @Experimental
  @Since("2.4.0")
  def barrier(): Unit = runBarrier("", RequestMethod.BARRIER)

  /**
   * :: Experimental ::
   * 阻塞直到所有任务到达此同步点，每个任务传入一条消息，
   * 返回所有任务传入的消息列表（全局收集）。
   */
  @Experimental
  @Since("3.0.0")
  def allGather(message: String): Array[String] = runBarrier(message, RequestMethod.ALL_GATHER)

  /**
   * :: Experimental ::
   * 获取此 barrier stage 中所有任务的 BarrierTaskInfo，按分区 ID 排序。
   */
  @Experimental
  @Since("2.4.0")
  def getTaskInfos(): Array[BarrierTaskInfo] = {
    val addressesStr = Option(taskContext.getLocalProperty("addresses")).getOrElse("")
    addressesStr.split(",").map(_.trim()).map(new BarrierTaskInfo(_))
  }

  // ===== 以下方法均委托给内部持有的 taskContext =====

  override def isCompleted(): Boolean = taskContext.isCompleted()

  override def isFailed(): Boolean = taskContext.isFailed()

  override def isInterrupted(): Boolean = taskContext.isInterrupted()

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    taskContext.addTaskCompletionListener(listener)
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    taskContext.addTaskFailureListener(listener)
    this
  }

  override def stageId(): Int = taskContext.stageId()

  override def stageAttemptNumber(): Int = taskContext.stageAttemptNumber()

  override def partitionId(): Int = taskContext.partitionId()

  override def numPartitions(): Int = taskContext.numPartitions()

  override def attemptNumber(): Int = taskContext.attemptNumber()

  override def taskAttemptId(): Long = taskContext.taskAttemptId()

  override def getLocalProperty(key: String): String = taskContext.getLocalProperty(key)

  override def taskMetrics(): TaskMetrics = taskContext.taskMetrics()

  override def getMetricsSources(sourceName: String): Seq[Source] = {
    taskContext.getMetricsSources(sourceName)
  }

  override def cpus(): Int = taskContext.cpus()

  override def resources(): Map[String, ResourceInformation] = taskContext.resources()

  override def resourcesJMap(): java.util.Map[String, ResourceInformation] = {
    resources().asJava
  }

  override private[spark] def killTaskIfInterrupted(): Unit = taskContext.killTaskIfInterrupted()

  override private[spark] def getKillReason(): Option[String] = taskContext.getKillReason()

  override private[spark] def taskMemoryManager(): TaskMemoryManager = {
    taskContext.taskMemoryManager()
  }

  override private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskContext.registerAccumulator(a)
  }

  override private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
    taskContext.setFetchFailed(fetchFailed)
  }

  override private[spark] def markInterrupted(reason: String): Unit = {
    taskContext.markInterrupted(reason)
  }

  override private[spark] def markTaskFailed(error: Throwable): Unit = {
    taskContext.markTaskFailed(error)
  }

  override private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = {
    taskContext.markTaskCompleted(error)
  }

  override private[spark] def getTaskFailure: Option[Throwable] = {
    taskContext.getTaskFailure
  }

  override private[spark] def fetchFailed: Option[FetchFailedException] = {
    taskContext.fetchFailed
  }

  override private[spark] def getLocalProperties: Properties = taskContext.getLocalProperties

  override private[spark] def interruptible(): Boolean = taskContext.interruptible()

  override private[spark] def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String)
    : Unit = {
    taskContext.pendingInterrupt(threadToInterrupt, reason)
  }

  override private[spark] def createResourceUninterruptibly[T <: Closeable](resourceBuilder: => T)
    : T = {
    taskContext.createResourceUninterruptibly(resourceBuilder)
  }
}

/** BarrierTaskContext 伴生对象，提供获取当前上下文的静态方法和共享定时器 */
@Experimental
@Since("2.4.0")
object BarrierTaskContext {
  /**
   * :: Experimental ::
   * 获取当前活跃的 BarrierTaskContext。在 barrier 任务的用户函数中调用。
   */
  @Experimental
  @Since("2.4.0")
  def get(): BarrierTaskContext = TaskContext.get().asInstanceOf[BarrierTaskContext]

  // barrier() 调用中用于定期打印等待日志的共享定时器
  private val timer = {
    val executor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      "Barrier task timer for barrier() calls.")
    assert(executor.isInstanceOf[ScheduledThreadPoolExecutor])
    executor.asInstanceOf[ScheduledThreadPoolExecutor]
  }

}

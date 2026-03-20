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
import java.util.{Properties, Stack}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.LISTENER
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._


/**
 * A [[TaskContext]] implementation.
 *
 * A small note on thread safety. The interrupted & fetchFailed fields are volatile, this makes
 * sure that updates are always visible across threads. The complete & failed flags and their
 * callbacks are protected by locking on the context instance. For instance, this ensures
 * that you cannot add a completion listener in one thread while we are completing in another
 * thread. Other state is immutable, however the exposed `TaskMetrics` & `MetricsSystem` objects are
 * not thread safe.
 */
private[spark] class TaskContextImpl(
    override val stageId: Int,
    override val stageAttemptNumber: Int,
    override val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val numPartitions: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty,
    override val cpus: Int = SparkEnv.get.conf.get(config.CPUS_PER_TASK),
    override val resources: Map[String, ResourceInformation] = Map.empty)
  extends TaskContext
  with Logging {

  /**
   * 任务完成回调函数栈。
   * 使用 Stack 使得监听器按注册的逆序执行。调用时从栈顶弹出。
   */
  @transient private val onCompleteCallbacks = new Stack[TaskCompletionListener]

  // 任务失败回调函数栈
  @transient private val onFailureCallbacks = new Stack[TaskFailureListener]

  /**
   * 当前正在执行完成/失败回调的线程（如果有的话）。
   * invokeListeners() 用它保证回调串行执行，避免并发调用。
   */
  @transient @volatile private var listenerInvocationThread: Option[Thread] = None

  // 如果任务被 kill，记录 kill 原因
  @volatile private var reasonIfKilled: Option[String] = None

  // 挂起的中断请求，被不可中断的资源创建阻塞时暂存。受 TaskContext.synchronized 保护
  private var pendingInterruptRequest: Option[(Option[Thread], String)] = None

  // 任务是否可被中断。受 TaskContext.synchronized 保护
  private var _interruptible = true

  // 任务是否已完成
  private var completed: Boolean = false

  // 如果任务失败，记录导致失败的异常
  private var failureCauseOpt: Option[Throwable] = None

  // Fetch 失败时保存异常，防止用户代码吞掉该异常（SPARK-19276）
  @volatile private var _fetchFailedException: Option[FetchFailedException] = None

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    val needToCallListener = synchronized {
      // If there is already a thread invoking listeners, adding the new listener to
      // `onCompleteCallbacks` will cause that thread to execute the new listener, and the call to
      // `invokeTaskCompletionListeners()` below will be a no-op.
      //
      // If there is no such thread, the call to `invokeTaskCompletionListeners()` below will
      // execute all listeners, including the new listener.
      onCompleteCallbacks.push(listener)
      completed
    }
    if (needToCallListener) {
      invokeTaskCompletionListeners(None)
    }
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    synchronized {
      onFailureCallbacks.push(listener)
      failureCauseOpt
    }.foreach(invokeTaskFailureListeners)
    this
  }

  override def resourcesJMap(): java.util.Map[String, ResourceInformation] = {
    resources.asJava
  }

  private[spark] override def markTaskFailed(error: Throwable): Unit = {
    synchronized {
      if (failureCauseOpt.isDefined) return
      failureCauseOpt = Some(error)
    }
    invokeTaskFailureListeners(error)
  }

  // 标记任务完成并触发所有完成回调（幂等：重复调用不会再次触发）
  private[spark] override def markTaskCompleted(error: Option[Throwable]): Unit = {
    synchronized {
      if (completed) return
      completed = true
    }
    invokeTaskCompletionListeners(error)
  }

  private[spark] override def getTaskFailure: Option[Throwable] = {
    failureCauseOpt
  }

  private def invokeTaskCompletionListeners(error: Option[Throwable]): Unit = {
    // It is safe to access the reference to `onCompleteCallbacks` without holding the TaskContext
    // lock. `invokeListeners()` acquires the lock before accessing the contents.
    invokeListeners(onCompleteCallbacks, "TaskCompletionListener", error) {
      _.onTaskCompletion(this)
    }
  }

  private def invokeTaskFailureListeners(error: Throwable): Unit = {
    // It is safe to access the reference to `onFailureCallbacks` without holding the TaskContext
    // lock. `invokeListeners()` acquires the lock before accessing the contents.
    invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error)) {
      _.onTaskFailure(this, error)
    }
  }

  /**
   * 通用的监听器调用方法。需满足两个约束：
   * 1. 监听器必须串行执行（TaskContext API 保证）
   * 2. 监听器可能创建新线程调用 TaskContext 方法，不能在持锁时调用监听器（避免死锁）
   *
   * 通过确保任意时刻最多一个线程在执行监听器来同时满足上述约束。
   */
  private def invokeListeners[T](
      listeners: Stack[T],
      name: String,
      error: Option[Throwable])(
      callback: T => Unit): Unit = {
    synchronized {
      if (listenerInvocationThread.nonEmpty) {
        // 已有其他线程在执行监听器，当前线程直接返回
        return
      } else {
        // 注册当前线程为监听器执行线程，阻止其他线程并发执行
        listenerInvocationThread = Some(Thread.currentThread())
      }
    }

    // 从栈中取出下一个监听器或注销当前线程
    def getNextListenerOrDeregisterThread(): Option[T] = synchronized {
      if (listeners.empty()) {
        listenerInvocationThread = None
        None
      } else {
        Some(listeners.pop())
      }
    }

    val listenerExceptions = new ArrayBuffer[Throwable](2)
    var listenerOption: Option[T] = None
    while ({listenerOption = getNextListenerOrDeregisterThread(); listenerOption.nonEmpty}) {
      val listener = listenerOption.get
      try {
        callback(listener)
      } catch {
        case e: Throwable =>
          // 监听器执行失败时：临时清除 listenerInvocationThread 并调用 markTaskFailed
          // 这会触发失败回调链，完成后继续执行剩余的监听器
          // 涵盖多种并发场景（任务体成功/失败 × 完成监听器失败 × 失败监听器失败等组合）
          try {
            listenerInvocationThread = None
            markTaskFailed(e)
          } catch {
            case t: Throwable => e.addSuppressed(t)
          } finally {
            synchronized {
              if (listenerInvocationThread.isEmpty) {
                listenerInvocationThread = Some(Thread.currentThread())
              }
            }
          }
          listenerExceptions += e
          logError(log"Error in ${MDC(LISTENER, name)}", e)
      }
    }
    // 如果有监听器抛出异常，汇总后抛出
    if (listenerExceptions.nonEmpty) {
      val exception = new TaskCompletionListenerException(
        listenerExceptions.map(_.getMessage).toSeq, error)
      listenerExceptions.foreach(exception.addSuppressed)
      throw exception
    }
  }

  private[spark] override def markInterrupted(reason: String): Unit = {
    reasonIfKilled = Some(reason)
  }

  private[spark] override def killTaskIfInterrupted(): Unit = {
    val reason = reasonIfKilled
    if (reason.isDefined) {
      throw new TaskKilledException(reason.get)
    }
  }

  private[spark] override def getKillReason(): Option[String] = {
    reasonIfKilled
  }

  @GuardedBy("this")
  override def isCompleted(): Boolean = synchronized(completed)

  override def isFailed(): Boolean = synchronized(failureCauseOpt.isDefined)

  override def isInterrupted(): Boolean = reasonIfKilled.isDefined

  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

  private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
    this._fetchFailedException = Option(fetchFailed)
  }

  private[spark] override def fetchFailed: Option[FetchFailedException] = _fetchFailedException

  private[spark] override def getLocalProperties: Properties = localProperties


  override def interruptible(): Boolean = TaskContext.synchronized(_interruptible)

  override def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String): Unit = {
    TaskContext.synchronized {
      pendingInterruptRequest = Some((threadToInterrupt, reason))
    }
  }

  /**
   * 在不可中断的上下文中创建资源。
   * 创建前先检查是否有挂起的中断请求，若有则立即抛出异常。
   * 创建期间将 _interruptible 设为 false，阻止中断请求生效。
   * 创建完成后注册完成回调自动关闭资源，并恢复可中断状态。
   */
  def createResourceUninterruptibly[T <: Closeable](resourceBuilder: => T): T = {

    // 检查并执行挂起的中断请求
    @inline def interruptIfRequired(): Unit = {
      pendingInterruptRequest.foreach { case (threadToInterrupt, reason) =>
        markInterrupted(reason)
        threadToInterrupt.foreach(_.interrupt())
      }
      killTaskIfInterrupted()
    }

    TaskContext.synchronized {
      interruptIfRequired()
      _interruptible = false
    }
    try {
      val resource = resourceBuilder
      addTaskCompletionListener[Unit](_ => resource.close())
      resource
    } finally {
      TaskContext.synchronized {
        _interruptible = true
        interruptIfRequired()
      }
    }
  }
}

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
import java.util.Properties

import org.apache.spark.annotation.{DeveloperApi, Evolving, Since}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.scheduler.Task
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}


/** TaskContext伴生对象，通过ThreadLocal管理当前线程的任务上下文 */
object TaskContext {
  /** 获取当前活跃的TaskContext，可在用户函数中调用以获取任务的上下文信息 */
  def get(): TaskContext = taskContext.get

  /**
   * 获取当前活跃TaskContext的分区ID。
   * 如果没有活跃的TaskContext（如本地执行时），返回0。
   */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }

  /** 在指定TaskContext下执行任务，确保执行完毕后清除ThreadLocal中的上下文 */
  def withTaskContext[T](context: TaskContext)(task: => T): T = {
    try {
      TaskContext.setTaskContext(context)
      task
    } finally {
      TaskContext.unset()
    }
  }

  // 使用ThreadLocal保存当前线程的TaskContext，确保每个任务线程有独立的上下文
  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  /** 设置当前线程的TaskContext（Spark内部使用） */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /** 清除当前线程的TaskContext（Spark内部使用） */
  protected[spark] def unset(): Unit = taskContext.remove()

  /** 创建一个空的TaskContext，仅用于测试 */
  private[spark] def empty(): TaskContextImpl = {
    new TaskContextImpl(0, 0, 0, 0, 0, 1,
      null, new Properties, null, TaskMetrics.empty, 1)
  }
}


/**
 * 任务的上下文信息，可在任务执行期间读取或修改。
 * 通过org.apache.spark.TaskContext.get()获取当前运行任务的TaskContext。
 */
abstract class TaskContext extends Serializable {
  // 注意：TaskContext不能定义get方法，否则会阻止Scala编译器基于伴生对象生成静态get方法

  /** 任务是否已完成 */
  def isCompleted(): Boolean

  /** 任务是否已失败 */
  def isFailed(): Boolean

  /** 任务是否已被中断（取消） */
  def isInterrupted(): Boolean

  /**
   * 添加任务完成监听器（Java友好版本）。
   * 在所有情况下都会被调用——成功、失败或取消。
   * 对已完成的任务添加监听器会导致该监听器立即被调用。
   * 同一线程注册的两个监听器在任务完成后按注册的逆序调用。
   * 监听器抛出的异常会导致任务失败。
   */
  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  /**
   * 添加任务完成监听器（Scala闭包版本）。
   * 在所有情况下都会被调用——成功、失败或取消。
   */
  def addTaskCompletionListener[U](f: (TaskContext) => U): TaskContext = {
    addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    })
  }

  /**
   * 添加任务失败监听器。在任务失败时调用（包括完成监听器失败的情况）。
   * 对已失败的任务添加监听器会导致该监听器立即被调用。
   */
  def addTaskFailureListener(listener: TaskFailureListener): TaskContext

  /**
   * 添加任务失败监听器（Scala闭包版本）。
   */
  def addTaskFailureListener(f: (TaskContext, Throwable) => Unit): TaskContext = {
    addTaskFailureListener(new TaskFailureListener {
      override def onTaskFailure(context: TaskContext, error: Throwable): Unit = f(context, error)
    })
  }

  /**
   * 使用此上下文运行任务，确保失败和完成监听器被正确触发。
   * 如果任务在启动前就已被标记为中断，先杀死任务再执行。
   * 异常处理：先触发失败回调，再触发完成回调，最后重新抛出异常。
   */
  private[spark] def runTaskWithListeners[T](task: Task[T]): T = {
    try {
      // SPARK-44818: 可能在kill最初被调用时taskThread还未初始化。
      // 在实际执行任务之前检查并中断。
      killTaskIfInterrupted()
      task.runTask(this)
    } catch {
      case e: Throwable =>
        // 捕获所有异常，运行失败和完成回调后重新抛出
        try {
          markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        try {
          markTaskCompleted(Some(e))
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        throw e
    } finally {
      // 调用任务完成回调。如果markTaskCompleted已经被调用过则为空操作。
      markTaskCompleted(None)
    }
  }

  /** 该任务所属的Stage ID */
  def stageId(): Int

  /** 该任务所属Stage的尝试次数，首次尝试为0 */
  def stageAttemptNumber(): Int

  /** 该任务计算的RDD分区ID */
  def partitionId(): Int

  /** 该任务所属Stage的总分区数 */
  def numPartitions(): Int

  /** 该任务的尝试次数，首次尝试为0 */
  def attemptNumber(): Int

  /** 该任务尝试的唯一ID（同一SparkContext内不会重复），类似Hadoop的TaskAttemptID */
  def taskAttemptId(): Long

  /** 获取Driver端设置的本地属性值，不存在时返回null */
  def getLocalProperty(key: String): String

  /** 分配给该任务的CPU核心数 */
  @Since("3.3.0")
  def cpus(): Int

  /** 分配给该任务的资源信息Map，键为资源名称 */
  @Evolving
  def resources(): Map[String, ResourceInformation]

  /** 分配给该任务的资源信息Map（Java版本） */
  @Evolving
  def resourcesJMap(): java.util.Map[String, ResourceInformation]

  /** 获取任务指标 */
  @DeveloperApi
  def taskMetrics(): TaskMetrics

  /** 获取与运行该任务的实例关联的指定名称的所有指标源 */
  @DeveloperApi
  def getMetricsSources(sourceName: String): Seq[Source]

  /** 如果任务被中断，抛出TaskKilledException并附带中断原因 */
  private[spark] def killTaskIfInterrupted(): Unit

  /** 如果任务被中断则返回中断原因，否则返回None */
  private[spark] def getKillReason(): Option[String]

  /** 获取该任务的托管内存管理器 */
  private[spark] def taskMemoryManager(): TaskMemoryManager

  /** 注册属于该任务的累加器。累加器在Executor端反序列化时必须调用此方法 */
  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit

  /** 记录任务因远程主机的Fetch失败而失败，触发Driver端的fetch-failure处理 */
  private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit

  /** 标记任务为中断（即取消） */
  private[spark] def markInterrupted(reason: String): Unit

  /** 标记任务为失败并触发失败监听器 */
  private[spark] def markTaskFailed(error: Throwable): Unit

  /** 标记任务为完成并触发完成监听器 */
  private[spark] def markTaskCompleted(error: Option[Throwable]): Unit

  /** 如果任务失败，返回导致失败的异常 */
  private[spark] def getTaskFailure: Option[Throwable] = None

  /** 返回任务中存储的FetchFailedException（如果有） */
  private[spark] def fetchFailed: Option[FetchFailedException]

  /** 获取Driver端设置的本地属性集合 */
  private[spark] def getLocalProperties: Properties

  /** 当前任务是否允许被中断 */
  private[spark] def interruptible(): Boolean

  /** 挂起中断请求，直到任务能够在不可中断地创建资源后进行中断 */
  private[spark] def pendingInterrupt(threadToInterrupt: Option[Thread], reason: String): Unit

  /**
   * 不可中断地创建可关闭资源。任务在此状态下不允许被中断，
   * 直到资源创建完成。用于保护资源初始化过程不被打断。
   */
  private[spark] def createResourceUninterruptibly[T <: Closeable](resourceBuilder: => T): T
}

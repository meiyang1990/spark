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

import java.util.concurrent.TimeUnit

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaFutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.JobWaiter
import org.apache.spark.util.ThreadUtils


/**
 * Action 结果的 Future 接口，支持取消操作。
 * 扩展了 Scala 标准 Future 接口，增加了取消功能。
 */
trait FutureAction[T] extends Future[T] {

  /** 取消此 Action 的执行（可附带取消原因） */
  def cancel(reason: Option[String]): Unit

  /** 取消此 Action 的执行 */
  def cancel(): Unit = cancel(None)

  /**
   * 阻塞直到此 Action 完成。
   * @param atMost 最大等待时间
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
   * 等待并返回此 Action 的结果。
   * @param atMost 最大等待时间
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /** Action 完成时（无论成功或失败）调用指定的回调函数 */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit

  /** 返回此 Action 是否已完成 */
  override def isCompleted: Boolean

  /** 返回此 Action 是否已被取消 */
  def isCancelled: Boolean

  /**
   * 此 Future 的当前值。
   * 未完成返回 None；完成返回 Some(Success(t)) 或 Some(Failure(error))
   */
  override def value: Option[Try[T]]

  /** 阻塞并返回此 Job 的结果 */
  @throws(classOf[SparkException])
  def get(): T = ThreadUtils.awaitResult(this, Duration.Inf)

  /**
   * 返回底层异步操作已运行的 Job ID 列表。
   * 某些操作可能运行多个 Job，因此多次调用可能返回不同的列表。
   */
  def jobIds: Seq[Int]

}

/**
 * 触发单个 Job 的 Action 对应的 FutureAction 实现。
 * 适用于 count、collect、reduce 等操作。
 */
@DeveloperApi
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  @volatile private var _cancelled: Boolean = false

  /** 设置取消标志并通知 JobWaiter 取消 */
  override def cancel(reason: Option[String]): Unit = {
    _cancelled = true
    jobWaiter.cancel(reason)
  }

  /** 委托给 JobWaiter 的 completionFuture 等待完成 */
  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    jobWaiter.completionFuture.ready(atMost)
    this
  }

  /** 等待完成并返回结果 */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    jobWaiter.completionFuture.ready(atMost)
    assert(value.isDefined, "Future has not completed properly")
    value.get.get
  }

  /** Job 完成时触发回调 */
  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    jobWaiter.completionFuture onComplete {_ => func(value.get)}
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def isCancelled: Boolean = _cancelled

  /** 将 JobWaiter 的结果转换为用户期望的结果类型 */
  override def value: Option[Try[T]] =
    jobWaiter.completionFuture.value.map {res => res.map(_ => resultFunc)}

  def jobIds: Seq[Int] = Seq(jobWaiter.jobId)

  override def transform[S](f: (Try[T]) => Try[S])(implicit e: ExecutionContext): Future[S] =
    jobWaiter.completionFuture.transform((u: Try[Unit]) => f(u.map(_ => resultFunc)))

  override def transformWith[S](f: (Try[T]) => Future[S])(implicit e: ExecutionContext): Future[S] =
    jobWaiter.completionFuture.transformWith((u: Try[Unit]) => f(u.map(_ => resultFunc)))
}


/**
 * 传递给 ComplexFutureAction 的 "run" 函数用来提交子 Job 的句柄。
 * 封装了 SparkContext 的 submitJob 功能以支持取消。
 */
@DeveloperApi
trait JobSubmitter {
  /** 提交一个 Job 并返回持有结果的 FutureAction */
  def submitJob[T, U, R](
    rdd: RDD[T],
    processPartition: Iterator[T] => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit,
    resultFunc: => R): FutureAction[R]
}


/**
 * 可能触发多个 Spark Job 的 Action 对应的 FutureAction 实现。
 * 适用于 take、takeSample 等操作。
 * 取消通过设置 cancelled 标志并取消所有待执行的子 Job 实现。
 */
@DeveloperApi
class ComplexFutureAction[T](run : JobSubmitter => Future[T])
  extends FutureAction[T] { self =>

  @volatile private var _cancelled = false

  // 持有所有已提交的子 FutureAction 列表
  @volatile private var subActions: List[FutureAction[_]] = Nil

  // 用 Promise 将 run 函数的 Future 结果桥接给外部
  private val p = Promise[T]().completeWith(run(jobSubmitter))

  /** 取消所有子 Action 并标记 Promise 失败 */
  override def cancel(reason: Option[String]): Unit = synchronized {
    _cancelled = true
    p.tryFailure(new SparkException("Action has been cancelled"))
    subActions.foreach(_.cancel(reason))
  }

  /** 创建 JobSubmitter 实例，在 synchronized 块中提交子 Job（保证原子性检查取消状态） */
  private def jobSubmitter = new JobSubmitter {
    def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): FutureAction[R] = self.synchronized {
      // 提交前检查是否已取消，检查和提交必须原子执行
      if (!isCancelled) {
        val job = rdd.context.submitJob(
          rdd,
          processPartition,
          partitions,
          resultHandler,
          resultFunc)
        subActions = job :: subActions
        job
      } else {
        throw new SparkException("Action has been cancelled")
      }
    }
  }

  override def isCancelled: Boolean = _cancelled

  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value

  /** 聚合所有子 Action 的 Job ID */
  def jobIds: Seq[Int] = subActions.flatMap(_.jobIds)

  override def transform[S](f: (Try[T]) => Try[S])(implicit e: ExecutionContext): Future[S] =
    p.future.transform(f)

  override def transformWith[S](f: (Try[T]) => Future[S])(implicit e: ExecutionContext): Future[S] =
    p.future.transformWith(f)
}


/**
 * 将 Scala FutureAction 适配为 Java java.util.concurrent.Future 接口的包装器。
 * 负责类型转换和 Java Future 语义适配（如 isDone 包含取消状态）。
 */
private[spark]
class JavaFutureActionWrapper[S, T](futureAction: FutureAction[S], converter: S => T)
  extends JavaFutureAction[T] {

  override def isCancelled: Boolean = futureAction.isCancelled

  /** 按 java.util.Future 语义：完成、异常或取消均返回 true */
  override def isDone: Boolean = {
    futureAction.isCancelled || futureAction.isCompleted
  }

  override def jobIds(): java.util.List[java.lang.Integer] = {
    java.util.List.of(futureAction.jobIds.map(Integer.valueOf): _*)
  }

  /** 等待结果完成并执行类型转换，取消时抛 CancellationException，失败时包装为 ExecutionException */
  private def getImpl(timeout: Duration): T = {
    ThreadUtils.awaitReady(futureAction, timeout)
    futureAction.value.get match {
      case scala.util.Success(value) => converter(value)
      case scala.util.Failure(exception) =>
        if (isCancelled) {
          throw new CancellationException("Job cancelled").initCause(exception)
        } else {
          throw new ExecutionException("Exception thrown by job", exception)
        }
    }
  }

  override def get(): T = getImpl(Duration.Inf)

  override def get(timeout: Long, unit: TimeUnit): T =
    getImpl(Duration.fromNanos(unit.toNanos(timeout)))

  /** 按 Java Future 语义：已完成时返回 false，否则异步取消并返回 true */
  override def cancel(mayInterruptIfRunning: Boolean): Boolean = synchronized {
    if (isDone) {
      false
    } else {
      futureAction.cancel()
      true
    }
  }

}

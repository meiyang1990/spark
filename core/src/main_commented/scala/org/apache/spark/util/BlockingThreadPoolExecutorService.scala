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

package org.apache.spark.util

import java.util
import java.util.concurrent._

import com.google.common.util.concurrent.Futures

// scalastyle:off
/**
 * 带阻塞限流功能的线程池执行器服务，通过信号量控制任务提交速率，避免队列无限溢出
 * 
 * 核心逻辑：新任务提交需要获取信号量许可，任务完成后释放许可，从而限制队列最大积压任务数
 * <p>
 * 注意：不支持[[invoke*]]系列方法，仅可使用[[submit]]或[[execute]]方法提交任务
 * <p>
 * 实现灵感来源于 Apache S4 项目的同名实现
 */
// scalastyle:on
/**
 * 带阻塞限流的线程池执行器，Spark核心内部使用，用于控制任务积压数量
 * 
 * @param nThreads 线程池固定线程数量
 * @param workQueueSize 工作队列最大可额外容纳的任务数量
 * @param threadFactory 线程工厂，用于创建新线程
 */
private[spark] class BlockingThreadPoolExecutorService(
    nThreads: Int, workQueueSize: Int, threadFactory: ThreadFactory)
  extends ExecutorService {

  // 信号量，总许可数 = 线程数 + 队列容量，控制最大同时处理+排队任务数
  private val permits = new Semaphore(nThreads + workQueueSize)

  // 阻塞工作队列，容量与信号量总许可数一致
  private val workQuque = new LinkedBlockingQueue[Runnable](nThreads + workQueueSize)

  // 实际执行任务的底层线程池，固定大小线程池
  private val delegate = new ThreadPoolExecutor(
    nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, workQuque, threadFactory)

  override def shutdown(): Unit = delegate.shutdown()

  override def shutdownNow(): util.List[Runnable] = delegate.shutdownNow()

  override def isShutdown: Boolean = delegate.isShutdown

  override def isTerminated: Boolean = delegate.isTerminated

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    delegate.awaitTermination(timeout, unit)

  /**
   * 提交Callable任务到线程池，需要先获取信号量许可
   * @param task 待执行的Callable任务
   * @return 任务执行结果Future
   */
  override def submit[T](task: Callable[T]): Future[T] = {
    try permits.acquire() catch {
      case e: InterruptedException =>
        // 恢复中断状态，返回失败Future
        Thread.currentThread.interrupt()
        return Futures.immediateFailedFuture(e)
    }
    // 包装任务，确保执行完成后释放许可
    delegate.submit(new CallableWithPermitRelease(task))
  }

  /**
   * 提交Runnable任务到线程池，指定返回结果
   * @param task 待执行的Runnable任务
   * @param result 任务执行完成后返回的结果对象
   * @return 任务执行结果Future
   */
  override def submit[T](task: Runnable, result: T): Future[T] = {
    try permits.acquire() catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        return Futures.immediateFailedFuture(e)
    }
    delegate.submit(new RunnableWithPermitRelease(task), result)
  }

  /**
   * 提交Runnable任务到线程池
   * @param task 待执行的Runnable任务
   * @return 任务执行结果Future
   */
  override def submit(task: Runnable): Future[_] = {
    try permits.acquire() catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
        return Futures.immediateFailedFuture(e)
    }
    delegate.submit(new RunnableWithPermitRelease(task))
  }

  /**
   * 执行Runnable任务，符合Executor接口规范
   * @param command 待执行的Runnable任务
   */
  override def execute(command: Runnable): Unit = {
    try permits.acquire() catch {
      case _: InterruptedException =>
        Thread.currentThread.interrupt()
    }
    delegate.execute(new RunnableWithPermitRelease(command))
  }

  override def invokeAll[T](
      tasks: util.Collection[_ <: Callable[T]]): util.List[Future[T]] =
    throw new UnsupportedOperationException("Not implemented")

  override def invokeAll[T](
      tasks: util.Collection[_ <: Callable[T]],
      timeout: Long, unit: TimeUnit): util.List[Future[T]] =
    throw new UnsupportedOperationException("Not implemented")

  override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]]): T =
    throw new UnsupportedOperationException("Not implemented")

  override def invokeAny[T](
      tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): T =
    throw new UnsupportedOperationException("Not implemented")

  /**
   * Runnable任务包装器，任务执行完成后自动释放信号量许可
   */
  private class RunnableWithPermitRelease(delegate: Runnable) extends Runnable {
    override def run(): Unit = try delegate.run() finally permits.release()
  }

  /**
   * Callable任务包装器，任务执行完成后自动释放信号量许可
   */
  private class CallableWithPermitRelease[T](delegate: Callable[T]) extends Callable[T] {
    override def call(): T = try delegate.call() finally permits.release()
  }
}
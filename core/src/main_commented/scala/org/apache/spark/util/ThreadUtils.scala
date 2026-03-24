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

import java.util.concurrent._
import java.util.concurrent.{Future => JFuture}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{Awaitable, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.SparkException

/**
 * 线程工具类，提供Spark内部常用的线程池创建、线程管理、异常处理、并发工具等能力
 * 为Spark核心模块统一封装常用的线程操作，方便创建命名规范、符合Daemon线程规范的线程池
 */
private[spark] object ThreadUtils {

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(sameThreadExecutorService())

  /**
   * 创建一个同线程执行器服务，所有任务都在调用线程中直接执行
   * Inspired by Guava MoreExecutors.sameThreadExecutor; 为了避免Guava版本问题在这里内联转换为Scala实现
   * @return 同线程执行器服务实例
   */
  // Inspired by Guava MoreExecutors.sameThreadExecutor; inlined and converted
  // to Scala here to avoid Guava version issues
  def sameThreadExecutorService(): ExecutorService = new AbstractExecutorService {
    private val lock = new ReentrantLock()
    private val termination = lock.newCondition()
    private var runningTasks = 0
    private var serviceIsShutdown = false

    override def shutdown(): Unit = {
      lock.lock()
      try {
        serviceIsShutdown = true
      } finally {
        lock.unlock()
      }
    }

    override def shutdownNow(): java.util.List[Runnable] = {
      shutdown()
      java.util.Collections.emptyList()
    }

    override def isShutdown: Boolean = {
      lock.lock()
      try {
        serviceIsShutdown
      } finally {
        lock.unlock()
      }
    }

    override def isTerminated: Boolean = {
      lock.lock()
      try {
        serviceIsShutdown && runningTasks == 0
      } finally {
        lock.unlock()
      }
    }

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
      var nanos = unit.toNanos(timeout)
      lock.lock()
      try {
        while (nanos > 0 && !isTerminated()) {
          nanos = termination.awaitNanos(nanos)
        }
        isTerminated()
      } finally {
        lock.unlock()
      }
    }

    override def execute(command: Runnable): Unit = {
      lock.lock()
      try {
        if (isShutdown()) throw new RejectedExecutionException("Executor already shutdown")
        runningTasks += 1
      } finally {
        lock.unlock()
      }
      try {
        command.run()
      } finally {
        lock.lock()
        try {
          runningTasks -= 1
          if (isTerminated()) termination.signalAll()
        } finally {
          lock.unlock()
        }
      }
    }
  }

  /**
   * 获取同线程执行上下文，所有任务都在调用execute/submit的线程中执行
   * 调用者需要确保这里运行的任务短小且不会阻塞
   * @return 同线程执行上下文实例
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * 创建命名线程工厂，生成的线程为守护线程，名称使用给定前缀加序号格式化
   * @param prefix 线程名称前缀
   * @return 命名守护线程工厂实例
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * 创建守护线程缓存线程池，线程名称格式为前缀-ID，ID为递增唯一整数
   * 包装了JDK的newCachedThreadPool，统一设置命名和守护线程属性
   * @param prefix 线程名称前缀
   * @return 缓存线程池实例
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * 创建带最大线程数限制的守护线程缓存线程池
   * 线程名称格式为前缀-ID，ID为递增唯一整数
   * @param prefix 线程名称前缀
   * @param maxThreadNumber 最大线程数
   * @param keepAliveSeconds 线程空闲存活时间，默认60秒
   * @return 缓存线程池实例
   */
  def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  /**
   * 创建固定大小的守护线程池，线程名称格式为前缀-ID，ID为递增唯一整数
   * 包装了JDK的newFixedThreadPool，统一设置命名和守护线程属性
   * @param nThreads 线程池固定线程数
   * @param prefix 线程名称前缀
   * @return 固定大小线程池实例
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * 创建单线程守护线程执行器
   * 包装了JDK的newFixedThreadPool单线程版本，统一设置命名和守护线程属性
   * @param threadName 线程名称
   * @return 单线程执行器实例
   */
  def newDaemonSingleThreadExecutor(threadName: String): ThreadPoolExecutor = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newFixedThreadPool(1, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * 创建支持自定义拒绝执行处理器的单线程守护执行器
   * @param threadName 线程名称
   * @param taskQueueCapacity 任务队列容量
   * @param rejectedExecutionHandler 拒绝执行处理器
   * @return 单线程执行器实例
   */
  def newDaemonSingleThreadExecutorWithRejectedExecutionHandler(
      threadName: String,
      taskQueueCapacity: Int,
      rejectedExecutionHandler: RejectedExecutionHandler): ThreadPoolExecutor = {

    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()

    new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue[Runnable](taskQueueCapacity),
      threadFactory,
      rejectedExecutionHandler)
  }

  /**
   * 创建带界工作队列的阻塞守护线程池，队列满时提交任务会被阻塞
   * @param nThreads 线程池线程数量
   * @param workQueueSize 工作队列容量
   * @param prefix 线程名称前缀，格式为前缀-ID，ID递增唯一
   * @return 阻塞线程池执行器服务
   */
  def newDaemonBlockingThreadPoolExecutorService(
      nThreads: Int, workQueueSize: Int, prefix: String): ExecutorService = {
    val threadFactory = namedThreadFactory(prefix)
    new BlockingThreadPoolExecutorService(nThreads, workQueueSize, threadFactory)
  }

  /**
   * 创建单线程守护调度线程执行器，支持定时和延迟任务
   * 开启了取消任务后自动从队列移除的策略，避免已取消任务占用队列空间
   * @param threadName 线程名称
   * @return 单线程调度执行器实例
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // 默认情况下，已取消的任务不会自动从工作队列移除，需要手动开启该功能
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * 创建单线程非守护调度线程执行器，支持定时和延迟任务
   * 开启了取消任务后自动从队列移除的策略，避免已取消任务占用队列空间
   * @param threadName 线程名称
   * @return 单线程调度执行器实例
   */
  def newSingleThreadScheduledExecutor(threadName: String): ScheduledThreadPoolExecutor = {
    val threadFactory = new ThreadFactoryBuilder().setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // 默认情况下，已取消的任务不会自动从工作队列移除，需要手动开启该功能
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * 创建指定线程数的守护调度线程池，支持定时和延迟任务
   * 开启了取消任务后自动从队列移除的策略，避免已取消任务占用队列空间
   * @param threadNamePrefix 线程名称前缀
   * @param numThreads 池内线程数量
   * @return 多线程调度执行器实例
   */
  def newDaemonThreadPoolScheduledExecutor(threadNamePrefix: String, numThreads: Int)
      : ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(s"$threadNamePrefix-%d")
      .build()
    val executor = new ScheduledThreadPoolExecutor(numThreads, threadFactory)
    // 默认情况下，已取消的任务不会自动从工作队列移除，需要手动开启该功能
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * 在新线程中执行代码并返回结果，新线程抛出的异常会被修正栈追踪后抛到调用线程
   * 异常栈会合并调用线程和新线程的栈，去掉当前工具方法的内部调用，让异常更清晰
   * @param threadName 新线程名称
   * @param isDaemon 是否为守护线程，默认true
   * @param body 需要执行的代码块
   * @tparam T 返回结果类型
   * @return 代码块执行结果
   */
  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()

    exception match {
      case Some(realException) =>
        throw wrapCallerStacktrace(realException, dropStacks = 2)
      case None =>
        result
    }
  }

  /**
   * 合并异常栈追踪，将新线程异常合并调用线程的栈信息，去掉当前工具方法的内部调用
   * 让异常信息更清晰，方便定位问题
   * @param realException 新线程抛出的原始异常
   * @param combineMessage 占位栈的提示信息
   * @param dropStacks 需要从当前调用栈中丢弃的栈帧数
   * @tparam T 异常类型
   * @return 修正栈追踪后的异常
   */
  def wrapCallerStacktrace[T <: Throwable](
       realException: T,
       combineMessage: String =
         s"run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")}",
       dropStacks: Int = 1): T = {
    require(dropStacks >= 0, "dropStacks must be zero or positive")
    val simpleName = this.getClass.getSimpleName
    // 移除进入当前辅助方法之前的栈调用部分，只保留调用者侧的栈信息
    // Remove the part of the stack that shows method calls into this helper method
    // This means drop everything from the top until the stack element
    // ThreadUtils.wrapCallerStack(), and then drop that as well (hence the `drop(1)`).
    // Large dropStacks allows caller to drop more stacks.
    val baseStackTrace = Thread.currentThread().getStackTrace
      .dropWhile(!_.getClassName.contains(simpleName))
      .drop(dropStacks)

    // 移除新线程栈中来自当前辅助方法的调用部分
    // Remove the part of the new thread stack that shows methods call from this helper method
    val extraStackTrace = realException.getStackTrace
      .takeWhile(!_.getClassName.contains(simpleName))

    // 合并两个栈，插入一个占位符说明使用了辅助线程执行
    // Combine the two stack traces, with a place holder just specifying that there
    // was a helper method used, without any further details of the helper
    val placeHolderStackElem = new StackTraceElement(s"... $combineMessage ..", " ", "", -1)
    val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

    // 更新异常栈并返回，供调用线程重新抛出
    // Update the stack trace and rethrow the exception in the caller thread
    realException.setStackTrace(finalStackTrace)
    realException
  }

  /**
   * 创建指定最大并行度和名称前缀的ForkJoinPool
   * 自定义线程工厂生成符合命名规范的线程
   * @param prefix 线程名称前缀
   * @param maxThreadNumber 最大并行度
   * @return ForkJoinPool实例
   */
  def newForkJoinPool(prefix: String, maxThreadNumber: Int): ForkJoinPool = {
    // 自定义工厂设置线程名称
    // Custom factory to set thread names
    val factory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
      override def newThread(pool: ForkJoinPool) =
        new ForkJoinWorkerThread(pool) {
          setName(prefix + "-" + super.getName)
        }
    }
    new ForkJoinPool(maxThreadNumber, factory,
      null, // handler
      false // asyncMode
    )
  }

  // scalastyle:off awaitresult
  /**
   * Spark内部推荐使用的等待结果方法，替代scala原生Await.result
   * 封装并重新抛出底层异常，确保当前线程栈出现在日志中；同时避免ForkJoinPool在等待时复用线程执行其他任务，防止ThreadLocal泄漏
   * 由于Spark大量使用ThreadLocal，该方法可以避免很多难以调试的ThreadLocal泄漏问题
   * @param awaitable 可等待的对象
   * @param atMost 最大等待时长
   * @tparam T 结果类型
   * @return 等待得到的结果
   * @throws SparkException 等待过程中发生非超时异常时抛出
   */
  @throws(classOf[SparkException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    SparkThreadUtils.awaitResult(awaitable, atMost)
  }
  // scalastyle:on awaitresult

  /**
   * 等待Java Future结果的方法，异常处理逻辑同scala版本awaitResult
   * @param future Java Future对象
   * @param atMost 最大等待时长
   * @tparam T 结果类型
   * @return 等待得到的结果
   * @throws SparkException 等待过程中发生非超时异常时抛出
   */
  @throws(classOf[SparkException])
  def awaitResult[T](future: JFuture[T], atMost: Duration): T = {
    try {
      atMost match {
        case Duration.Inf => future.get()
        case _ => future.get(atMost._1, atMost._2)
      }
    } catch {
      case e: SparkFatalException =>
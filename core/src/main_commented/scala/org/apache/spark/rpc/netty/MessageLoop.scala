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

package org.apache.spark.rpc.netty

import java.util.concurrent._

import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.ERROR
import org.apache.spark.internal.config.EXECUTOR_ID
import org.apache.spark.internal.config.Network._
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcEndpoint}
import org.apache.spark.util.ThreadUtils

/**
 * 文件概述: Netty-based RPC 消息循环实现，为Dispatcher提供消息投递到Endpoint的执行能力
 * 核心功能: 定义不同类型的消息循环，基于线程池实现RPC消息的异步处理，支持共享线程池和专属线程池两种模式
 */

/**
 * 消息循环抽象基类，为Dispatcher提供向RPC端点投递处理消息的基础执行框架
 * 核心职责: 维护待处理消息的收件人队列，提供统一的消息处理循环，管理线程池生命周期
 * @param dispatcher 所属的RPC消息分发器，用于消息处理回调
 */
private sealed abstract class MessageLoop(dispatcher: Dispatcher) extends Logging {

  // 存储有待处理消息的收件箱队列，由消息循环线程池处理
  private val active = new LinkedBlockingQueue[Inbox]()

  // 消息循环执行任务，会在线程池的所有工作线程上运行
  protected val receiveLoopRunnable = new Runnable() {
    override def run(): Unit = receiveLoop()
  }

  // 子类实现具体类型的线程池
  protected val threadpool: ExecutorService

  // 标记消息循环是否已停止
  private var stopped = false

  /**
   * 投递新消息到指定端点的收件箱
   * @param endpointName 目标RPC端点名称
   * @param message 待处理消息
   */
  def post(endpointName: String, message: InboxMessage): Unit

  /**
   * 注销指定端点，清理相关资源
   * @param name 要注销的端点名称
   */
  def unregister(name: String): Unit

  /**
   * 停止消息循环，关闭线程池并等待所有任务完成
   */
  def stop(): Unit = {
    synchronized {
      if (!stopped) {
        // 注入毒丸消息，通知所有工作线程退出
        setActive(MessageLoop.PoisonPill)
        threadpool.shutdown()
        stopped = true
      }
    }
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * 将收件箱添加到待处理队列
   * @param inbox 待处理的收件箱
   */
  protected final def setActive(inbox: Inbox): Unit = active.offer(inbox)

  /**
   * 消息循环主逻辑，持续从队列取出收件箱处理消息
   */
  private def receiveLoop(): Unit = {
    try {
      while (true) {
        try {
          // 阻塞获取待处理收件箱
          val inbox = active.take()
          if (inbox == MessageLoop.PoisonPill) {
            // 放回毒丸，让其他线程也能收到退出信号
            setActive(MessageLoop.PoisonPill)
            return
          }
          // 处理收件箱中所有待处理消息
          inbox.process(dispatcher)
        } catch {
          // 捕获非致命异常，记录日志后继续循环
          case NonFatal(e) => logError(log"${MDC(ERROR, e.getMessage)}", e)
        }
      }
    } catch {
      // 线程中断，直接退出循环
      case _: InterruptedException => // exit
      // 抛出其他未知异常，重新提交任务保证消息投递不中断
      case t: Throwable =>
        try {
          // 重新提交循环任务，保证异常未导致JVM退出时消息处理仍能继续
          threadpool.execute(receiveLoopRunnable)
        } finally {
          throw t
        }
    }
  }
}

/**
 * MessageLoop 伴生对象，定义毒丸消息标记
 */
private object MessageLoop {
  /** 毒丸收件箱，用于通知消息循环停止处理消息 */
  val PoisonPill = new Inbox(null, null)
}

/**
 * 共享型消息循环，多个RPC端点共享同一个线程池处理消息
 * 适用场景: 大多数轻量级RPC端点，共享线程池资源降低开销
 * @param conf Spark配置对象
 * @param dispatcher 所属的消息分发器
 * @param numUsableCores 可用CPU核心数，用于计算线程池大小
 */
private class SharedMessageLoop(
    conf: SparkConf,
    dispatcher: Dispatcher,
    numUsableCores: Int)
  extends MessageLoop(dispatcher) {

  // 存储端点名称到对应收件箱的映射
  private val endpoints = new ConcurrentHashMap[String, Inbox]()

  /**
   * 根据配置计算消息循环线程池的线程数量
   * @param conf Spark配置对象
   * @return 线程池大小
   */
  private def getNumOfThreads(conf: SparkConf): Int = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()

    val modNumThreads = conf.get(RPC_NETTY_DISPATCHER_NUM_THREADS)
      .getOrElse(math.max(2, availableCores))

    // 支持Driver/Executor分别配置线程数
    conf.get(EXECUTOR_ID).map { id =>
      val role = if (id == SparkContext.DRIVER_IDENTIFIER) "driver" else "executor"
      conf.getInt(s"spark.$role.rpc.netty.dispatcher.numThreads", modNumThreads)
    }.getOrElse(modNumThreads)
  }

  /** 消息分发使用的固定大小守护线程池 */
  override protected val threadpool: ThreadPoolExecutor = {
    val numThreads = getNumOfThreads(conf)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    // 启动所有工作线程，进入消息循环
    for (i <- 0 until numThreads) {
      pool.execute(receiveLoopRunnable)
    }
    pool
  }

  override def post(endpointName: String, message: InboxMessage): Unit = {
    val inbox = endpoints.get(endpointName)
    inbox.post(message)
    setActive(inbox)
  }

  override def unregister(name: String): Unit = {
    val inbox = endpoints.remove(name)
    if (inbox != null) {
      inbox.stop()
      // 标记收件箱为活跃，处理OnStop停止消息
      setActive(inbox)
    }
  }

  /**
   * 注册新的RPC端点到共享消息循环
   * @param name 端点名称
   * @param endpoint 要注册的RPC端点
   */
  def register(name: String, endpoint: RpcEndpoint): Unit = {
    val inbox = new Inbox(name, endpoint)
    endpoints.put(name, inbox)
    // 标记收件箱为活跃，处理OnStart启动消息
    setActive(inbox)
  }
}

/**
 * 专属型消息循环，为单个隔离RPC端点独占使用线程池
 * 适用场景: 需要独立线程隔离的重量级RPC端点，避免阻塞其他端点消息处理
 * @param name 端点名称
 * @param endpoint 对应的隔离RPC端点
 * @param dispatcher 所属的消息分发器
 */
private class DedicatedMessageLoop(
    name: String,
    endpoint: IsolatedRpcEndpoint,
    dispatcher: Dispatcher)
  extends MessageLoop(dispatcher) {

  // 当前专属端点的收件箱
  private val inbox = new Inbox(name, endpoint)

  // 根据端点要求的线程数创建对应线程池
  override protected val threadpool = if (endpoint.threadCount() > 1) {
    ThreadUtils.newDaemonCachedThreadPool(s"dispatcher-$name", endpoint.threadCount())
  } else {
    ThreadUtils.newDaemonSingleThreadExecutor(s"dispatcher-$name")
  }

  // 启动对应数量的工作线程进入消息循环
  (1 to endpoint.threadCount()).foreach { _ =>
    /**
     * 注意不能使用ExecutorService#submit:
     * submit方法会将未捕获异常包裹在Future中，无法被UncaughtExceptionHandler正确处理
     * 因此直接使用execute执行任务
     * */
    threadpool.execute(receiveLoopRunnable)
  }

  // 标记收件箱为活跃，处理OnStart启动消息
  setActive(inbox)

  override def post(endpointName: String, message: InboxMessage): Unit = {
    require(endpointName == name)
    inbox.post(message)
    setActive(inbox)
  }

  override def unregister(endpointName: String): Unit = synchronized {
    require(endpointName == name)
    inbox.stop()
    // 标记收件箱为活跃，处理OnStop停止消息
    setActive(inbox)
    // 注入毒丸停止消息循环
    setActive(MessageLoop.PoisonPill)
    threadpool.shutdown()
  }
}
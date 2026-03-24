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

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

/**
 * 文件说明: Spark RPC 网络通信层，基于Netty实现的消息收件箱，负责存储和分发消息给对应RpcEndpoint
 * 核心职责：为每个RpcEndpoint维护一个消息队列，保证消息线程安全地投递和处理，支持并发控制
 */

private[netty] sealed trait InboxMessage

/** 单向消息，不需要响应回复 */
private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

/** 请求响应消息，需要返回处理结果给发送方 */
private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

/** 端点启动消息，是第一个被处理的消息 */
private[netty] case object OnStart extends InboxMessage

/** 端点停止消息，是最后一个被处理的消息 */
private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 * 收件箱实现，为单个RpcEndpoint存储消息，并线程安全地将消息投递到端点处理
 * @param endpointName 对应端点名称，用于日志标识
 * @param endpoint 绑定的目标RpcEndpoint实例
 */
private[netty] class Inbox(val endpointName: String, val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  // 存储待处理消息的双向链表
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  // 标记收件箱是否已停止，停止后不再接受新消息
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  // 是否允许多线程并发处理消息，非线程安全端点初始不允许，启动后改为允许
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  // 当前正在处理消息的活跃线程数
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  // 初始化时添加OnStart消息，保证它是第一个被处理的消息
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
   * 处理收件箱中存储的消息，由调度器分发线程调用
   * @param dispatcher 消息调度器，用于处理停止时移除端点引用
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    // 先获取消息并检查并发条件
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    // 循环持续处理消息，直到队列为空或不满足并发条件退出
    while (true) {
      // 安全调用，异常会传递给端点onError处理
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              // 处理需要回复的消息，调用receiveAndReply获取处理函数并执行
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case e: Throwable =>
                // 处理异常，发送失败响应给调用方后重新抛出异常，交给onError处理
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            // 处理单向消息，只接收不需要回复
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            // 调用端点启动方法
            endpoint.onStart()
            // 如果端点不是线程安全的，启用并发处理，允许后续多线程处理消息
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            // 停止处理，检查此时应该只有一个活跃线程处理停止消息
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            // 从调度器移除端点引用
            dispatcher.removeRpcEndpointRef(endpoint)
            // 调用端点停止方法
            endpoint.onStop()
            // 确保OnStop是最后一条消息
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            // 通知端点远程地址连接建立
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            // 通知端点远程地址断开连接
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            // 通知端点网络错误
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      // 处理完当前消息，获取下一条消息并检查退出条件
      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // 如果不允许并发且当前不是唯一工作线程，退出处理
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          // 没有更多消息，减少活跃线程数后退出
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  /**
   * 投递新消息到收件箱，线程安全
   * @param message 待投递的消息
   */
  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      // 已停止的收件箱直接丢弃消息
      onDrop(message)
    } else {
      // 添加到消息队列等待处理
      messages.add(message)
      false
    }
  }

  /**
   * 停止收件箱，添加OnStop消息作为最后一条消息
   */
  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      // 禁用并发，保证停止时只有一个线程处理OnStop，安全释放资源
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  /**
   * 检查收件箱消息队列是否为空
   * @return 空返回true，否则false
   */
  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   * 处理被丢弃的消息，默认打警告日志，测试类可覆盖该方法
   * @param message 被丢弃的消息
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(log"Drop ${MDC(MESSAGE, message)} " +
      log"because endpoint ${MDC(END_POINT, endpointName)} is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   * 安全执行用户动作，捕获异常并分发给端点的onError处理
   * @param endpoint 目标端点
   * @param action 待执行的消息处理动作
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    def dealWithFatalError(fatal: Throwable): Unit = {
      // 处理致命错误，减少活跃线程数后抛出异常终止线程
      inbox.synchronized {
        assert(numActiveThreads > 0, "The number of active threads should be positive.")
        // Should reduce the number of active threads before throw the error.
        numActiveThreads -= 1
      }
      logError(log"An error happened while processing message in the inbox for" +
        log" ${MDC(END_POINT, endpointName)}", fatal)
      throw fatal
    }

    try action catch {
      case NonFatal(e) =>
        // 非致命异常，调用端点onError处理
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            // onError处理也抛出非致命异常，仅打日志忽略
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
          case fatal: Throwable =>
            // 致命错误交给专门方法处理
            dealWithFatalError(fatal)
        }
      case fatal: Throwable =>
        // 外层抛出的致命错误直接处理
        dealWithFatalError(fatal)
    }
  }

  // exposed only for testing
  /** 获取当前活跃线程数，仅用于测试 */
  def getNumActiveThreads: Int = {
    inbox.synchronized {
      inbox.numActiveThreads
    }
  }
}
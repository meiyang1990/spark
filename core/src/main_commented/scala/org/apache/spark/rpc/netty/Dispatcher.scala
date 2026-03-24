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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, CountDownLatch}
import javax.annotation.concurrent.GuardedBy

import scala.concurrent.Promise
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._

/**
 * 文件说明: RPC消息分发器，负责将RPC消息路由到对应的RPC端点(Endpoint)处理，是Spark RPC系统的消息路由中心
 *
 * @param nettyEnv 对应Netty实现的RPC环境实例
 * @param numUsableCores 分配给当前进程的CPU核心数，用于调整线程池大小；如果为0则自动使用主机可用CPU数
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {

  /** 存储端点名称到对应消息循环的映射 */
  private val endpoints: ConcurrentMap[String, MessageLoop] =
    new ConcurrentHashMap[String, MessageLoop]
  /** 存储端点实例到对应端点引用的映射，用于根据端点实例查找引用 */
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  /** 关闭同步栅栏，用于等待分发器完全停止 */
  private val shutdownLatch = new CountDownLatch(1)
  /** 共享消息循环实例，用于处理所有非隔离端点共享使用 */
  private lazy val sharedLoop = new SharedMessageLoop(nettyEnv.conf, this, numUsableCores)

  /**
   * 标记分发器是否已停止，停止后所有新消息都会被直接拒绝
   */
  @GuardedBy("this")
  private var stopped = false

  /**
   * 注册RPC端点，创建并返回对应的端点引用
   *
   * @param name 端点名称
   * @param endpoint 要注册的端点实例
   * @return 创建好的端点引用，可用于向该端点发送消息
   */
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      if (endpoints.containsKey(name)) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }

      // 必须在将端点分配给MessageLoop之前完成，因为MessageLoop注册时会激活Inbox
      // 且endpointRef必须在onStart调用前放入endpointRefs，保证onStart中可以获取到自身引用
      endpointRefs.put(endpoint, endpointRef)

      var messageLoop: MessageLoop = null
      try {
        messageLoop = endpoint match {
          case e: IsolatedRpcEndpoint =>
            // 隔离端点使用独立专属的消息循环
            new DedicatedMessageLoop(name, e, this)
          case _ =>
            // 普通端点注册到共享消息循环复用线程
            sharedLoop.register(name, endpoint)
            sharedLoop
        }
        endpoints.put(name, messageLoop)
      } catch {
        case NonFatal(e) =>
          // 注册失败清理已添加的引用，回滚操作
          endpointRefs.remove(endpoint)
          throw e
      }
    }
    endpointRef
  }

  /**
   * 根据端点实例获取对应的端点引用 */
  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  /**
   * 移除端点实例对应的引用映射 */
  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  /**
   * 注销指定名称的RPC端点，支持幂等调用
   * @param name 要注销的端点名称
   */
  // Should be idempotent
  private def unregisterRpcEndpoint(name: String): Unit = {
    val loop = endpoints.remove(name)
    if (loop != null) {
      loop.unregister(name)
    }
    // 这里不清理endpointRefs，因为当前可能还有消息在处理，处理过程会用到getRpcEndpointRef
    // 所以endpointRefs会在Inbox处理完成后通过removeRpcEndpointRef清理
  }

  /**
   * 停止指定端点引用对应的RPC端点
   * @param rpcEndpointRef 要停止的端点引用
   */
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) {
        // 整个分发器已停止，该端点会被统一停止，直接返回
        return
      }
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * 向当前进程所有已注册的RPC端点广播发送消息
   *
   * 用于通知所有端点网络事件，例如"新节点已连接"等全局事件
   * @param message 要广播的消息
   */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
        postMessage(name, message, (e) => { e match {
          case e: RpcEnvStoppedException => logDebug(s"Message $message dropped. ${e.getMessage}")
          case e: Throwable =>
            logWarning(log"Message ${MDC(MESSAGE, message)} dropped. ${MDC(ERROR, e.getMessage)}")
        }}
      )}
  }

  /**
   * 投递远程端点发送过来的RPC请求消息
   * @param message 请求消息封装
   * @param callback 远程调用结果回调
   */
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /**
   * 投递本地端点发送过来的RPC请求消息
   * @param message 请求消息封装
   * @param p 本地调用结果Promise，用于返回结果
   */
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /**
   * 投递单向（不需要响应）消息
   * @param message 单向请求消息封装
   */
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      {
        // SPARK-31922: 本地集群模式下，停止过程中处理异步消息总会触发RpcEnvStoppedException
        // 将该异常仅打debug级别日志，避免Spark Shell停止本地集群时输出冗余错误日志打扰用户
        case re: RpcEnvStoppedException => logDebug(s"Message $message dropped. ${re.getMessage}")
        case e if SparkEnv.get.isStopped =>
          logWarning(log"Message ${MDC(MESSAGE, message)} dropped due to sparkEnv " +
            log"is stopped. ${MDC(ERROR, e.getMessage)}")
        case e => throw e
      })
  }

  /**
   * 向指定端点投递消息
   *
   * @param endpointName 目标端点名称
   * @param message 要投递的消息
   * @param callbackIfStopped 如果端点或分发器已停止时调用的错误回调
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      val loop = endpoints.get(endpointName)
      if (stopped) {
        // 分发器已停止，返回停止异常
        Some(new RpcEnvStoppedException())
      } else if (loop == null) {
        // 找不到对应端点，返回找不到异常
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 投递消息到对应消息循环处理
        loop.post(endpointName, message)
        None
      }
    }
    // 不需要在同步块中调用回调，避免阻塞消息投递
    error.foreach(callbackIfStopped)
  }

  /**
   * 停止整个分发器，终止所有消息循环，清理所有注册端点
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    var stopSharedLoop = false
    endpoints.asScala.foreach { case (name, loop) =>
      unregisterRpcEndpoint(name)
      if (!loop.isInstanceOf[SharedMessageLoop]) {
        // 独立专属消息循环直接停止
        loop.stop()
      } else {
        // 共享消息循环统一后续停止
        stopSharedLoop = true
      }
    }
    if (stopSharedLoop) {
      sharedLoop.stop()
    }
    // 通知等待关闭完成
    shutdownLatch.countDown()
  }

  /**
   * 阻塞等待分发器完全停止完成
   */
  def awaitTermination(): Unit = {
    shutdownLatch.await()
  }

  /**
   * 检查指定名称的端点是否存在
   * @param name 要检查的端点名称
   * @return 存在返回true，否则返回false
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }
}
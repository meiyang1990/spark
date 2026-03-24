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

import java.io._
import java.net.{InetSocketAddress, URI}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel, WritableByteChannel}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.Nullable

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.EXECUTOR_ID
import org.apache.spark.internal.config.Network._
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client._
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server._
import org.apache.spark.rpc._
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, SerializationStream}
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, ThreadUtils, Utils}

/**
 * 基于Netty实现的RPC环境，是Spark分布式通信的核心实现，负责管理端点注册、消息发送与接收、网络连接等核心通信能力
 */
private[netty] class NettyRpcEnv(
    val conf: SparkConf,
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    securityManager: SecurityManager,
    numUsableCores: Int) extends RpcEnv(conf) with Logging {
  // 当前节点角色：driver 或 executor
  val role = conf.get(EXECUTOR_ID).map { id =>
    if (id == SparkContext.DRIVER_IDENTIFIER) "driver" else "executor"
  }

  // 创建RPC传输配置，基于Spark配置构建
  private[netty] val transportConf = SparkTransportConf.fromSparkConf(
    conf.clone.set(RPC_IO_NUM_CONNECTIONS_PER_PEER, 1),
    "rpc",
    conf.get(RPC_IO_THREADS).getOrElse(numUsableCores),
    role,
    sslOptions = Some(securityManager.getRpcSSLOptions())
  )

  // 创建消息分发器，负责将收到的消息路由到对应RpcEndpoint
  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)

  // 创建文件流管理器，支持大文件流式传输
  private val streamManager = new NettyStreamManager(this)

  // 创建传输上下文，注册NettyRpcHandler处理入站消息
  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

  // 创建客户端认证启动器，如果开启认证则添加认证 bootstrap
  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new AuthClientBootstrap(transportConf,
        securityManager.getSaslUser(), securityManager))
    } else {
      java.util.Collections.emptyList[TransportClientBootstrap]
    }
  }

  // 创建RPC客户端连接工厂
  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  /**
   * A separate client factory for file downloads. This avoids using the same RPC handler as
   * the main RPC context, so that events caused by these clients are kept isolated from the
   * main RPC traffic.
   *
   * It also allows for different configuration of certain properties, such as the number of
   * connections per peer.
   */
  @volatile private var fileDownloadFactory: TransportClientFactory = _

  // 超时调度线程池，用于处理RPC请求超时检测
  val timeoutScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout")

  // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
  // to implement non-blocking send/ask.
  // TODO: a non-blocking TransportClientFactory.createClient in future
  // 客户端连接线程池，用于异步建立远程连接，避免阻塞主线程
  private[netty] val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
    "netty-rpc-connection",
    conf.get(RPC_CONNECT_THREADS))

  // RPC服务端实例，负责接收入站连接和请求
  @volatile private var server: TransportServer = _

  // 停止标志，标记当前RPC环境是否已经停止
  private val stopped = new AtomicBoolean(false)

  /**
   * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
   * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
   */
  // 远程地址对应出站消息队列，实现非阻塞发送
  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  /**
   * Remove the address's Outbox and stop it.
   */
  // 移除并停止指定远程地址的出站队列
  private[netty] def removeOutbox(address: RpcAddress): Unit = {
    val outbox = outboxes.remove(address)
    if (outbox != null) {
      outbox.stop()
    }
  }

  /**
   * 启动RPC服务端，绑定指定地址和端口，并注册端点验证器
   */
  def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] =
      if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
    } else {
      java.util.Collections.emptyList()
    }
    server = transportContext.createServer(bindAddress, port, bootstraps)
    // 注册端点验证端点，用于异步验证端点是否存在
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
  }

  @Nullable
  // 当前RPC环境的对外地址，懒加载，服务端启动后才会有值
  override lazy val address: RpcAddress = {
    if (server != null) RpcAddress(host, server.getPort()) else null
  }

  // 注册一个RPC端点，返回对应端点引用
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  /**
   * 根据URI异步获取端点引用，先验证端点是否存在，成功后返回引用
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(conf, addr, this)
    val verifier = new NettyRpcEndpointRef(
      conf, RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME), this)
    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  // 停止指定端点引用对应的端点
  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  // 将消息投递到出站队列，处理远程地址的出站消息管理
  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      // 已有可用客户端连接，直接发送
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      // 获取或创建对应远程地址的出站队列
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
        if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // RPC环境已停止，清理刚创建的出站队列
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
          // 将消息发送到出站队列
          targetOutbox.send(message)
        }
      }
    }

  /**
   * 发送单向请求消息，区分本地和远程端点处理
   */
  private[netty] def send(message: RequestMessage): Unit = {
    val remoteAddr = message.receiver.address
    if (remoteAddr == address) {
      // 本地端点消息，直接分发到本地 dispatcher
      try {
        dispatcher.postOneWayMessage(message)
      } catch {
        case e: RpcEnvStoppedException => logDebug(e.getMessage)
      }
    } else {
      // 远程端点消息，投递到出站队列
      postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
    }
  }

  // 创建到指定远程地址的客户端连接
  private[netty] def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  /**
   * 发起可中止的RPC请求，支持超时和中止操作，返回可中止的Future
   */
  private[netty] def askAbortable[T: ClassTag](
      message: RequestMessage, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    val promise = Promise[Any]()
    val remoteAddr = message.receiver.address
    var rpcMsg: Option[RpcOutboxMessage] = None

    // 请求失败回调，设置Promise失败
    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case e : RpcEnvStoppedException => logDebug(s"Ignored failure: $e")
          case _ => logWarning(log"Ignored failure: ${MDC(ERROR, e)}")
        }
      }
    }

    // 请求成功回调，设置Promise成功
    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply =>
        if (!promise.trySuccess(rpcReply)) {
          logWarning(log"Ignored message: ${MDC(MESSAGE, reply)}")
        }
    }

    // 中止请求回调，处理中止逻辑
    def onAbort(t: Throwable): Unit = {
      onFailure(t)
      rpcMsg.foreach(_.onAbort())
    }

    try {
      if (remoteAddr == address) {
        // 本地端点请求，直接分发到本地 dispatcher
        val p = Promise[Any]()
        p.future.onComplete {
          case Success(response) => onSuccess(response)
          case Failure(e) => onFailure(e)
        }(ThreadUtils.sameThread)
        dispatcher.postLocalMessage(message, p)
      } else {
        // 远程端点请求，创建RPC出站消息投递到出站队列
        val rpcMessage = RpcOutboxMessage(message.serialize(this),
          onFailure,
          (client, response) => onSuccess(deserialize[Any](client, response)))
        rpcMsg = Option(rpcMessage)
        postToOutbox(message.receiver, rpcMessage)
        // 超时处理：Future失败时，如果是超时则触发超时处理
        promise.future.failed.foreach {
          case _: TimeoutException => rpcMessage.onTimeout()
          case _ =>
        }(ThreadUtils.sameThread)
      }

      // 调度超时检测任务
      val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
        override def run(): Unit = {
          val remoteRecAddr = if (remoteAddr == null) {
          Try {
            message.receiver.client.getChannel.remoteAddress()
          }.toOption.orNull
        } else {
          remoteAddr
        }
          // 超时，触发失败回调
          onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteRecAddr} " +
            s"in ${timeout.duration}"))
        }
      }, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
      // 请求完成后取消超时检测
      promise.future.onComplete { v =>
        timeoutCancelable.cancel(true)
      }(ThreadUtils.sameThread)
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }

    new AbortableRpcFuture[T](
      promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread),
      onAbort)
  }

  // 发起普通不可中止的RPC请求
  private[netty] def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  // 使用Java序列化序列化对象
  private[netty] def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  /**
   * Returns [[SerializationStream]] that forwards the serialized bytes to `out`.
   */
  // 获取输出流序列化器，用于流式序列化
  private[netty] def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  // 反序列化响应对象
  private[netty] def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { () =>
        javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  // 根据端点实例获取对应的端点引用
  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  // 关闭并清理RPC环境
  override def shutdown(): Unit = {
    cleanup()
  }

  // 等待dispatcher终止完成
  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  // 清理所有资源：停止出站队列、关闭线程池、关闭服务端和客户端
  private def cleanup(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    // 停止所有出站队列并移除
    val iter = outboxes.values().iterator()
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }
    if (timeoutScheduler != null) {
      timeoutScheduler.shutdownNow()
    }
    if (dispatcher != null) {
      dispatcher.stop()
    }
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }
    if (fileDownloadFactory != null) {
      fileDownloadFactory.close()
    }
    if (transportContext != null) {
      transportContext.close()
    }
  }

  // 反序列化包装，设置当前反序列化环境上下文
  override def deserialize[T](deserializationAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserializationAction()
    }
  }

  // 获取文件服务器接口，由流管理器实现
  override def fileServer: RpcEnvFileServer = streamManager

  /**
   * 打开远程文件通道，异步下载文件并返回可读通道供读取
   */
  override def openChannel(uri: String): ReadableByteChannel = {
    val parsedUri = new URI(uri)
    require(parsedUri.getHost() != null, "Host name must be defined.")
    require(parsedUri.getPort() > 0, "Port must be defined.")
    require(parsedUri.getPath() != null && parsedUri.getPath().nonEmpty, "Path must be defined.")

    // 创建管道，用于异步下载和同步读取之间的桥接
    val pipe = Pipe.open()
    val source = new FileDownloadChannel(pipe.source())
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      // 创建文件下载客户端并发起流请求
      val client = downloadClient(parsedUri.getHost(), parsedUri.getPort())
      val callback
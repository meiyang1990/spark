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

package org.apache.spark.api.r

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}
import io.netty.handler.timeout.ReadTimeoutHandler

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.R._
import org.apache.spark.network.util.{IOMode, NettyUtils}

/**
 * 文件说明: R语言API后端服务，基于Netty实现R客户端与JVM之间的通信，为SparkR提供跨语言交互能力
 */

/**
 * 基于Netty实现的R后端服务，负责R进程与JVM之间的通信交互
 * 核心职责: 维护R-JVM通信通道、管理JVM侧对象引用，处理R调用请求
 */
private[spark] class RBackend {

  private[this] var channelFuture: ChannelFuture = null
  private[this] var bootstrap: ServerBootstrap = null
  private[this] var bossGroup: EventLoopGroup = null

  /** 跟踪本RBackend实例中返回给R的JVM对象，用于生命周期管理 */
  private[r] val jvmObjectTracker = new JVMObjectTracker

  /**
   * 初始化R后端Netty服务，绑定本地随机端口并启动监听
   * @return 返回绑定的端口号和认证助手实例，用于R客户端连接和认证
   */
  def init(): (Int, RAuthHelper) = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    val backendConnectionTimeout = conf.get(R_BACKEND_CONNECTION_TIMEOUT)
    // 创建Netty事件循环组，处理连接请求
    bossGroup = NettyUtils.createEventLoop(IOMode.NIO, conf.get(R_NUM_BACKEND_THREADS), "RBackend")
    val workerGroup = bossGroup
    val handler = new RBackendHandler(this)
    val authHelper = new RAuthHelper(conf)
    val channelClass = NettyUtils.getServerChannelClass(IOMode.NIO)

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(channelClass)

    // 配置每个新连接的Channel处理流水线
    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast("encoder", new ByteArrayEncoder())
          .addLast("frameDecoder",
            // 帧长度解码配置: 最大帧长2G，长度字段偏移0，长度字段4字节，跳过长度字段本身读取帧内容
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addLast("decoder", new ByteArrayDecoder())
          // 添加读超时处理器，连接空闲超时后自动断开
          .addLast("readTimeoutHandler", new ReadTimeoutHandler(backendConnectionTimeout))
          // 添加连接认证处理器，验证R客户端身份
          .addLast(new RBackendAuthHandler(authHelper.secret))
          // 添加业务请求处理器，处理R调用请求
          .addLast("handler", handler)
      }
    })

    // 绑定本地localhost随机端口，同步等待绑定完成
    channelFuture = bootstrap.bind(new InetSocketAddress("localhost", 0))
    channelFuture.syncUninterruptibly()

    val port = channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
    (port, authHelper)
  }

  /**
   * 阻塞等待服务通道关闭，保持服务运行
   */
  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  /**
   * 关闭后端服务，释放所有Netty资源和对象 tracker 缓存
   */
  def close(): Unit = {
    if (channelFuture != null) {
      // 关闭服务通道，设置10秒超时确保资源释放
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.config().group() != null) {
      bootstrap.config().group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.config().childGroup() != null) {
      bootstrap.config().childGroup().shutdownGracefully()
    }
    bootstrap = null
    jvmObjectTracker.clear()
  }

}

/**
 * R后端服务入口对象，负责作为独立进程启动R后端服务，向R客户端传递连接信息
 */
private[spark] object RBackend extends Logging {
  initializeLogIfNecessary(true)

  /**
   * R后端服务主入口方法，启动服务并将连接信息写入临时文件供R进程读取
   * @param args 命令行参数，第一个参数为临时文件路径，用于存储连接信息
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      // scalastyle:off println
      System.err.println("Usage: RBackend <tempFilePath>")
      // scalastyle:on println
      System.exit(-1)
    }

    val sparkRBackend = new RBackend()
    try {
      // 初始化R后端服务，绑定随机端口
      val (boundPort, authHelper) = sparkRBackend.init()
      // 创建第二个监听端口，等待R进程回连完成生命周期管理
      val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
      val listenPort = serverSocket.getLocalPort()
      // 读取连接超时配置，传递给R客户端
      val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
      val backendConnectionTimeout = conf.get(R_BACKEND_CONNECTION_TIMEOUT)

      // 通过临时文件将连接信息传递给R进程
      val path = args(0)
      val f = new File(path + ".tmp")
      val dos = new DataOutputStream(new FileOutputStream(f))
      dos.writeInt(boundPort)
      dos.writeInt(listenPort)
      SerDe.writeString(dos, RUtils.rPackages.getOrElse(""))
      dos.writeInt(backendConnectionTimeout)
      SerDe.writeString(dos, authHelper.secret)
      dos.close()
      // 原子重命名文件，确保R读取到完整的连接信息
      f.renameTo(new File(path))

      // 启动后台线程等待R进程连接，管理服务生命周期
      new Thread("wait for socket to close") {
        setDaemon(true)
        override def run(): Unit = {
          val buf = new Array[Byte](1024)
          // 设置10秒超时，10秒内R未回连则自动退出
          serverSocket.setSoTimeout(10000)

          // 等待R进程回连，允许最多10次认证失败，避免无限循环
          try {
            var remainingAttempts = 10
            var inSocket: Socket = null
            while (inSocket == null) {
              inSocket = serverSocket.accept()
              try {
                // 验证回连客户端身份
                authHelper.authClient(inSocket)
              } catch {
                case e: Exception =>
                  remainingAttempts -= 1
                  if (remainingAttempts == 0) {
                    val msg = "Too many failed authentication attempts."
                    logError(msg)
                    throw new IllegalStateException(msg)
                  }
                  logInfo("Client connection failed authentication.")
                  inSocket = null
              }
            }

            // 关闭回连监听套接字，等待R进程退出
            serverSocket.close()

            // 阻塞读取，R进程退出后套接字会关闭，此方法返回，触发服务关闭流程
            inSocket.getInputStream().read(buf)
          } finally {
            // R进程退出后，关闭所有资源退出JVM
            serverSocket.close()
            sparkRBackend.close()
            System.exit(0)
          }
        }
      }.start()

      // 启动主服务，开始处理R请求
      sparkRBackend.run()
    } catch {
      case e: IOException =>
        logError("Server shutting down: failed with exception ", e)
        sparkRBackend.close()
        System.exit(1)
    }
    System.exit(0)
  }

}
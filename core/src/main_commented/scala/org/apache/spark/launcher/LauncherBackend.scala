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

package org.apache.spark.launcher

import java.net.{InetAddress, Socket}

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.launcher.LauncherProtocol._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 用于与启动服务器通信的后端基类，用户需要继承此类并实现抽象方法提供具体处理逻辑。
 * 
 * 启动通信工作原理请参考 `LauncherServer`。
 * 该类作为Spark应用启动过程中，应用进程与外部启动器进程之间通信的服务端后端，负责接收
 * 外部控制命令并向外部上报应用状态变化。
 */
private[spark] abstract class LauncherBackend {

  private var clientThread: Thread = _
  private var connection: BackendConnection = _
  private var lastState: SparkAppHandle.State = _
  @volatile private var _isConnected = false

  protected def conf: SparkConf

  /**
   * 连接到外部启动器服务器
   * 从配置或环境变量读取端口和密钥，建立本地回环连接完成握手
   */
  def connect(): Unit = {
    val port = conf.getOption(LauncherProtocol.CONF_LAUNCHER_PORT)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT))
      .map(_.toInt)
    val secret = conf.getOption(LauncherProtocol.CONF_LAUNCHER_SECRET)
      .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET))
    if (port.isDefined && secret.isDefined) {
      // 建立到本地回环地址指定端口的Socket连接
      val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
      connection = new BackendConnection(s)
      // 发送握手消息，携带认证密钥和Spark版本
      connection.send(new Hello(secret.get, SPARK_VERSION))
      // 启动客户端通信线程处理消息
      clientThread = LauncherBackend.threadFactory.newThread(connection)
      clientThread.start()
      _isConnected = true
    }
  }

  /**
   * 关闭与启动器服务器的连接，等待通信线程退出
   */
  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } finally {
        if (clientThread != null) {
          clientThread.join()
        }
      }
    }
  }

  /**
   * 向启动器服务器上报应用ID
   * @param appId Spark应用ID
   */
  def setAppId(appId: String): Unit = {
    if (connection != null && isConnected()) {
      connection.send(new SetAppId(appId))
    }
  }

  /**
   * 向启动器服务器上报应用状态变化
   * @param state 应用当前状态
   */
  def setState(state: SparkAppHandle.State): Unit = {
    if (connection != null && isConnected() && lastState != state) {
      connection.send(new SetState(state))
      lastState = state
    }
  }

  /** 返回当前后端是否保持与启动器服务器的连接 */
  def isConnected(): Boolean = _isConnected

  /**
   * 收到停止应用请求时的回调，子类需要实现该方法，尽可能优雅地停止应用
   */
  protected def onStopRequest(): Unit

  /**
   * 连接断开时的回调，子类可按需覆盖实现
   */
  protected def onDisconnected() : Unit = { }

  /**
   * 在单独线程触发停止应用请求，避免阻塞通信线程
   */
  private def fireStopRequest(): Unit = {
    val thread = LauncherBackend.threadFactory.newThread(
      () => Utils.tryLogNonFatalError { onStopRequest() })
    thread.start()
  }

  /**
   * 后端与启动器服务器的连接处理类，负责处理接收到的消息
   * @param s 与客户端建立的Socket连接
   */
  private class BackendConnection(s: Socket) extends LauncherConnection(s) {

    override protected def handle(m: Message): Unit = m match {
      // 收到停止请求，触发停止应用回调
      case _: Stop =>
        fireStopRequest()

      // 不认识的消息类型抛出异常
      case _ =>
        throw new IllegalArgumentException(s"Unexpected message type: ${m.getClass().getName()}")
    }

    override def close(): Unit = {
      try {
        // 更新连接状态为断开
        _isConnected = false
        super.close()
      } finally {
        // 触发断开回调
        onDisconnected()
      }
    }

  }

}

/**
 * LauncherBackend的伴生对象，持有全局命名线程工厂
 */
private object LauncherBackend {

  /** 创建LauncherBackend线程的命名线程工厂 */
  val threadFactory = ThreadUtils.namedThreadFactory("LauncherBackend")

}
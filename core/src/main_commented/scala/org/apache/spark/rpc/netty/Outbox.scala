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

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.rpc.{RpcAddress, RpcEnvStoppedException}

/**
 * 出站消息特质，定义出站RPC消息的通用接口
 */
private[netty] sealed trait OutboxMessage {

  /**
   * 通过指定传输客户端发送消息
   * @param client 传输客户端
   */
  def sendWith(client: TransportClient): Unit

  /**
   * 消息发送失败时的回调处理
   * @param e 发送异常
   */
  def onFailure(e: Throwable): Unit

}

/**
 * 单向出站消息，不需要响应的单向RPC调用
 * @param content 消息内容字节缓冲
 */
private[netty] case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage
  with Logging {

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case e1: RpcEnvStoppedException => logDebug(e1.getMessage)
      case e1: Throwable => logWarning(log"Failed to send one-way RPC.", e1)
    }
  }

}

/**
 * RPC请求出站消息，需要等待响应的双向RPC调用
 * @param content 消息内容字节缓冲
 * @param _onFailure 失败回调函数
 * @param _onSuccess 成功回调函数
 */
private[netty] case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit)
  extends OutboxMessage with RpcResponseCallback with Logging {

  private var client: TransportClient = _
  private var requestId: Long = _

  override def sendWith(client: TransportClient): Unit = {
    this.client = client
    this.requestId = client.sendRpc(content, this)
  }

  /**
   * 从传输客户端移除待处理的RPC请求
   */
  private[netty] def removeRpcRequest(): Unit = {
    if (client != null) {
      client.removeRpcRequest(requestId)
    } else {
      logError("Ask terminated before connecting successfully")
    }
  }

  /**
   * 请求超时处理
   */
  def onTimeout(): Unit = {
    removeRpcRequest()
  }

  /**
   * 请求中止处理
   */
  def onAbort(): Unit = {
    removeRpcRequest()
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  override def onSuccess(response: ByteBuffer): Unit = {
    _onSuccess(client, response)
  }

}

/**
 * 出站消息箱，负责向指定远程RPC地址发送消息，维护连接状态和消息队列
 * 保证消息按顺序发送，处理连接建立和网络故障
 * @param nettyEnv Netty RPC环境实例
 * @param address 目标远程RPC地址
 */
private[netty] class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {

  outbox => // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  // 待发送消息队列
  private val messages = new java.util.LinkedList[OutboxMessage]

  @GuardedBy("this")
  // 当前活动的传输客户端连接
  private var client: TransportClient = null

  /**
   * connectFuture points to the connect task. If there is no connect task, connectFuture will be
   * null.
   */
  @GuardedBy("this")
  // 连接建立任务的Future，null表示无正在进行的连接任务
  private var connectFuture: java.util.concurrent.Future[Unit] = null

  @GuardedBy("this")
  // 出站箱是否已停止
  private var stopped = false

  /**
   * If there is any thread draining the message queue
   */
  @GuardedBy("this")
  // 是否有线程正在处理发送消息队列
  private var draining = false

  /**
   * 发送出站消息，若未建立连接则缓存消息并启动连接任务，若出站箱已停止则通知发送失败
   * @param message 待发送的出站消息
   */
  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      if (stopped) {
        true
      } else {
        messages.add(message)
        false
      }
    }
    if (dropped) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
    } else {
      drainOutbox()
    }
  }

  /**
   * 处理消息队列发送，若已有线程处理或正在连接则直接退出，未连接则启动连接任务
   * 连接就绪后依次发送所有缓存消息
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        launchConnectTask()
        return
      }
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {
          message.sendWith(_client)
        } else {
          assert(stopped)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }

  /**
   * 启动异步连接任务，向目标地址建立传输客户端连接
   * 连接成功后触发消息队列发送
   */
  private def launchConnectTask(): Unit = {
    connectFuture = nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = _client
            if (stopped) {
              closeClient()
            }
          }
        } catch {
          case ie: InterruptedException =>
            // exit
            return
          case NonFatal(e) =>
            outbox.synchronized { connectFuture = null }
            handleNetworkFailure(e)
            return
        }
        outbox.synchronized { connectFuture = null }
        // It's possible that no thread is draining now. If we don't drain here, we cannot send the
        // messages until the next message arrives.
        drainOutbox()
      }
    })
  }

  /**
   * 处理网络故障，停止出站箱并通知所有剩余消息发送失败，从RPC环境移除当前出站箱
   * @param e 网络异常
   */
  private def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == null)
      if (stopped) {
        return
      }
      stopped = true
      closeClient()
    }
    // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
    // with a new connection
    nettyEnv.removeOutbox(address)

    // Notify the connection failure for the remaining messages
    //
    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(e)
      message = messages.poll()
    }
    assert(messages.isEmpty)
  }

  /**
   * 清空客户端引用，不直接关闭连接以便复用
   */
  private def closeClient(): Unit = synchronized {
    // Just set client to null. Don't close it in order to reuse the connection.
    client = null
  }

  /**
   * 停止出站箱，取消正在进行的连接，通知所有剩余消息发送失败
   */
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
      if (connectFuture != null) {
        connectFuture.cancel(true)
      }
      closeClient()
    }

    // We always check `stopped` before updating messages, so here we can make sure no thread will
    // update messages and it's safe to just drain the queue.
    var message = messages.poll()
    while (message != null) {
      message.onFailure(new SparkException("Message is dropped because Outbox is stopped"))
      message = messages.poll()
    }
  }
}
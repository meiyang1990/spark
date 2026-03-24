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

import scala.concurrent.Promise

import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}

/**
 * 文件说明：基于Netty实现的RPC调用上下文抽象基类，负责处理RPC请求的回复逻辑，
 * 支持本地同进程和远程跨节点两种调用场景的回复处理。
 */

/**
 * Netty RPC调用上下文抽象基类，定义RPC请求回复的通用处理逻辑
 * @param senderAddress 请求发送方的网络地址
 */
private[netty] abstract class NettyRpcCallContext(override val senderAddress: RpcAddress)
  extends RpcCallContext with Logging {

  /** 子类需要实现的发送消息抽象方法 */
  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }

}

/**
 * 本地同进程RPC调用上下文，通过Promise完成请求结果传递
 * 用于发送方和接收方处于同一JVM进程时的场景，避免序列化和网络开销
 * @param senderAddress 请求发送方地址
 * @param p 用于传递调用结果的Promise对象
 */
private[netty] class LocalNettyRpcCallContext(
    senderAddress: RpcAddress,
    p: Promise[Any])
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

/**
 * 远程跨节点RPC调用上下文，通过Netty回调将序列化后的回复返回给调用方
 * 用于发送方和接收方处于不同节点的场景，需要序列化消息后通过网络发送
 * @param nettyEnv Netty RPC环境实例，提供消息序列化能力
 * @param callback Netty RPC响应回调，用于将结果返回给调用方
 * @param senderAddress 请求发送方的网络地址
 */
private[netty] class RemoteNettyRpcCallContext(
    nettyEnv: NettyRpcEnv,
    callback: RpcResponseCallback,
    senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {

  override protected def send(message: Any): Unit = {
    // 序列化回复消息
    val reply = nettyEnv.serialize(message)
    // 通过回调将序列化结果返回
    callback.onSuccess(reply)
  }
}
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

import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
 * 用于远程RpcEnv查询指定RpcEndpoint是否存在的端点服务
 * 
 * 在建立远程端点引用时使用，负责验证目标端点是否存在于当前Rpc环境中
 * 
 * @param rpcEnv 当前端点所属的Rpc环境
 * @param dispatcher 消息分发器，用于查询端点存在性
 */
private[netty] class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher)
  extends RpcEndpoint {

  /**
   * 接收请求并回复的处理方法，处理端点存在性查询
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // 处理端点存在性检查请求，调用分发器验证后回复结果
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }
}

/**
 * RpcEndpoint验证器的伴随对象，定义常量和消息协议
 */
private[netty] object RpcEndpointVerifier {
  // 验证器端点注册名称
  val NAME = "endpoint-verifier"

  /** 用于向远程RpcEndpointVerifier询问指定端点是否存在的请求消息 */
  case class CheckExistence(name: String)
}
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

package org.apache.spark.internal.plugin

import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEnv}

/**
 * 插件消息容器，用于在Driver和Executor之间传递插件消息
 * @param pluginName 目标插件名称
 * @param message 实际消息内容
 */
case class PluginMessage(pluginName: String, message: AnyRef)

/**
 * Driver端插件RPC通信端点，负责处理Executor端发来的针对Driver插件的消息
 * 基于隔离线程安全RPC模型实现，所有消息会在独立线程中处理保证线程安全
 * @param plugins 已注册的Driver插件映射，key为插件名称，value为插件实例
 * @param rpcEnv RPC环境实例
 */
private class PluginEndpoint(
    plugins: Map[String, DriverPlugin],
    override val rpcEnv: RpcEnv)
  extends IsolatedThreadSafeRpcEndpoint with Logging {

  /**
   * 处理单向消息（不需要回复的消息）
   */
  override def receive: PartialFunction[Any, Unit] = {
    case PluginMessage(pluginName, message) =>
      // 根据插件名称查找目标插件
      plugins.get(pluginName) match {
        case Some(plugin) =>
          try {
            // 调用插件处理消息
            val reply = plugin.receive(message)
            // 单向消息不应该有返回值，如果存在返回值输出警告日志
            if (reply != null) {
              logWarning(
                log"Plugin ${MDC(PLUGIN_NAME, pluginName)} " +
                  log"returned reply for one-way message of type " +
                  log"${MDC(CLASS_NAME, message.getClass().getName())}.")
            }
          } catch {
            // 捕获插件处理异常，输出警告日志不中断整个流程
            case e: Exception =>
              logWarning(log"Error in plugin ${MDC(PLUGIN_NAME, pluginName)} " +
                log"when handling message of type " +
                log"${MDC(CLASS_NAME, message.getClass().getName())}.", e)
          }

        // 找不到对应插件，抛出异常
        case None =>
          throw new IllegalArgumentException(s"Received message for unknown plugin $pluginName.")
      }
  }

  /**
   * 处理需要回复的双向消息
   * @param context RPC调用上下文，用于发送回复结果
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PluginMessage(pluginName, message) =>
      // 根据插件名称查找目标插件
      plugins.get(pluginName) match {
        case Some(plugin) =>
          // 调用插件处理并将结果回复给调用方
          context.reply(plugin.receive(message))

        // 找不到对应插件，抛出异常
        case None =>
          throw new IllegalArgumentException(s"Received message for unknown plugin $pluginName.")
      }
  }

}
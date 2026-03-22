// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain copy of the License at
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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import io.netty.channel.{Channel, ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.internal.Logging

/**
 * 文件: RBackendAuthHandler.scala
 * 所属模块: Spark核心(core)模块
 * 核心职责: 为R后端进程与JVM后端服务之间的连接提供身份认证处理，保障通信安全
 *
 * 来自R进程连接的身份认证处理器，验证R客户端连接时提供的共享密钥是否匹配
 */
private class RBackendAuthHandler(secret: String)
  extends SimpleChannelInboundHandler[Array[Byte]] with Logging {

  /**
   * 处理R客户端发送的认证消息，验证密钥并完成连接建立
   * @param ctx Netty通道上下文
   * @param msg R客户端发送的认证消息字节数组
   */
  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    // R侧代码会在序列化字符串末尾添加空终止符，此处需要移除该终止符
    val clientSecret = new String(msg, 0, msg.length - 1, UTF_8)
    try {
      require(secret == clientSecret, "Auth secret mismatch.")
      // 认证通过，从通道流水线移除当前认证处理器，后续通信直接走业务处理
      ctx.pipeline().remove(this)
      // 回复认证成功响应
      writeReply("ok", ctx.channel())
    } catch {
      case e: Exception =>
        logInfo("Authentication failure.", e)
        // 认证失败，回复错误响应
        writeReply("err", ctx.channel())
        // 关闭非法连接
        ctx.close()
    }
  }

  /**
   * 向R客户端写入认证响应结果
   * @param reply 响应内容，"ok"表示认证通过，"err"表示认证失败
   * @param chan 通信通道
   */
  private def writeReply(reply: String, chan: Channel): Unit = {
    val out = new ByteArrayOutputStream()
    SerDe.writeString(new DataOutputStream(out), reply)
    chan.writeAndFlush(out.toByteArray())
  }

}
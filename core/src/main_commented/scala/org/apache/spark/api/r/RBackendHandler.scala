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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.TimeUnit

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.handler.timeout.ReadTimeoutException

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.api.r.SerDe._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.R._
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * R后端服务的Netty消息处理器，负责处理R前端发起的RPC调用请求
 * 接收R客户端的方法调用请求，反射调用JVM中的对应方法，并将结果返回给R客户端
 * 标记为Sharable以复用实例，TODO需要确认连接复用是否安全
 * 
 * @param server R后端服务实例，用于访问JVM对象追踪器等核心资源
 */
@Sharable
private[r] class RBackendHandler(server: RBackend)
  extends SimpleChannelInboundHandler[Array[Byte]] with Logging {

  /**
   * 处理Netty通道读取到的R客户端请求消息
   * 解析请求头，分发给对应处理逻辑，构造响应返回给客户端
   * 
   * @param ctx Netty通道上下文
   * @param msg 客户端发送的请求字节数组
   */
  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // 读取请求头：是否静态方法、对象ID、方法名、参数数量
    val isStatic = readBoolean(dis)
    val objId = readString(dis)
    val methodName = readString(dis)
    val numArgs = readInt(dis)

    if (objId == "SparkRHandler") {
      // 处理SparkRHandler内置方法调用
      methodName match {
        // 测试用回声方法，直接返回第一个参数
        case "echo" =>
          val args = readArgs(numArgs, dis)
          assert(numArgs == 1)

          writeInt(dos, 0)
          writeObject(dos, args(0), server.jvmObjectTracker)
        // 停止后端服务方法
        case "stopBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()
        // 删除JVM中追踪的对象方法
        case "rm" =>
          try {
            val t = readObjectType(dis)
            assert(t == 'c')
            val objToRemove = readString(dis)
            server.jvmObjectTracker.remove(JVMObjectId(objToRemove))
            writeInt(dos, 0)
            writeObject(dos, null, server.jvmObjectTracker)
          } catch {
            case e: Exception =>
              logError(log"Removing ${MDC(OBJECT_ID, objId)} failed", e)
              writeInt(dos, -1)
              writeString(dos, s"Removing $objId failed: ${e.getMessage}")
          }
        // 未知方法处理，返回错误
        case _ =>
          dos.writeInt(-1)
          writeString(dos, s"Error: unknown method $methodName")
      }
    } else {
      // 处理普通JVM对象方法调用，启动定时心跳避免R端超时
      // 定期发送心跳响应，告知R端后端仍然存活，继续等待结果
      val execService = ThreadUtils.newDaemonSingleThreadScheduledExecutor("SparkRKeepAliveThread")
      val pingRunner = new Runnable {
        override def run(): Unit = {
          val pingBaos = new ByteArrayOutputStream()
          val pingDaos = new DataOutputStream(pingBaos)
          // +1表示心跳响应，告知客户端继续等待
          writeInt(pingDaos, +1)
          ctx.write(pingBaos.toByteArray)
        }
      }
      // 读取心跳间隔和连接超时配置
      val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
      val heartBeatInterval = conf.get(R_HEARTBEAT_INTERVAL)
      val backendConnectionTimeout = conf.get(R_BACKEND_CONNECTION_TIMEOUT)
      val interval = Math.min(heartBeatInterval, backendConnectionTimeout - 1)

      // 启动定时心跳任务
      execService.scheduleAtFixedRate(pingRunner, interval, interval, TimeUnit.SECONDS)
      // 实际处理方法调用
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos)
      // 方法调用完成后关闭心跳线程池
      execService.shutdown()
      execService.awaitTermination(1, TimeUnit.SECONDS)
    }

    val reply = bos.toByteArray
    ctx.write(reply)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  /**
   * 处理通道异常，根据异常类型处理：读超时直接忽略，其他异常关闭连接
   * 
   * @param ctx Netty通道上下文
   * @param cause 抛出的异常
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause match {
      case timeout: ReadTimeoutException =>
        // 忽略读超时，不需要关闭连接
        logWarning("Ignoring read timeout in RBackendHandler")
      case _ =>
        // 其他异常打印栈追踪并关闭连接
        cause.printStackTrace()
        ctx.close()
    }
  }

  /**
   * 处理R客户端发起的反射方法调用，通过Java反射调用目标JVM对象/类的方法，并返回结果
   * 
   * @param isStatic 是否调用静态方法
   * @param objId 目标对象ID（如果是实例方法）或全类名（如果是静态方法）
   * @param methodName 方法名，构造函数使用特殊值"<init>"
   * @param numArgs 参数数量
   * @param dis 输入流，用于读取参数
   * @param dos 输出流，用于写入调用结果
   */
  def handleMethodCall(
      isStatic: Boolean,
      objId: String,
      methodName: String,
      numArgs: Int,
      dis: DataInputStream,
      dos: DataOutputStream): Unit = {
    var obj: Object = null
    try {
      // 获取目标类：静态方法直接加载类，实例方法从追踪器获取对象后取其类
      val cls = if (isStatic) {
        Utils.classForName(objId)
      } else {
        obj = server.jvmObjectTracker(JVMObjectId(objId))
        obj.getClass
      }

      // 读取所有参数
      val args = readArgs(numArgs, dis)

      // 查找同名方法
      val methods = cls.getMethods
      val selectedMethods = methods.filter(m => m.getName == methodName)
      if (selectedMethods.length > 0) {
        // 匹配方法签名
        val index = findMatchedSignature(
          selectedMethods.map(_.getParameterTypes),
          args)

        if (index.isEmpty) {
          // 未找到匹配的方法签名，记录日志并抛出异常
          logWarning(log"cannot find matching method " +
            log"${MDC(CLASS_NAME, cls)}.${MDC(METHOD_NAME, methodName)}. Candidates are:")
          selectedMethods.foreach { method =>
            logWarning(log"${MDC(METHOD_NAME, methodName)}(" +
              log"${MDC(METHOD_PARAM_TYPES, method.getParameterTypes.mkString(","))})")
          }
          throw new Exception(s"No matched method found for $cls.$methodName")
        }

        // 反射调用方法
        val ret = selectedMethods(index.get).invoke(obj, args : _*)

        // 写入成功状态和返回结果
        writeInt(dos, 0)
        writeObject(dos, ret, server.jvmObjectTracker)
      } else if (methodName == "<init>") {
        // 处理构造函数调用
        val ctors = cls.getConstructors
        // 匹配构造函数签名
        val index = findMatchedSignature(
          ctors.map(_.getParameterTypes),
          args)

        if (index.isEmpty) {
          // 未找到匹配的构造函数，记录日志并抛出异常
          logWarning(log"cannot find matching constructor for ${MDC(CLASS_NAME, cls)}. "
            + log"Candidates are:")
          ctors.foreach { ctor =>
            logWarning(log"${MDC(CLASS_NAME, cls)}(" +
              log"${MDC(METHOD_PARAM_TYPES, ctor.getParameterTypes.mkString(","))})")
          }
          throw new Exception(s"No matched constructor found for $cls")
        }

        // 反射创建实例
        val obj = ctors(index.get).newInstance(args : _*)

        // 写入成功状态和新建实例
        writeInt(dos, 0)
        writeObject(dos, obj.asInstanceOf[AnyRef], server.jvmObjectTracker)
      } else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Exception =>
        // 调用失败，记录日志并返回错误信息给R客户端
        logError(log"${MDC(METHOD_NAME, methodName)} on ${MDC(OBJECT_ID, objId)} failed", e)
        writeInt(dos, -1)
        // 返回异常根因的错误信息给R用户
        writeString(dos, Utils.exceptionString(e.getCause))
    }
  }

  /**
   * 从输入流读取指定数量的参数，反序列化为Java对象数组
   * 
   * @param numArgs 需要读取的参数数量
   * @param dis 数据输入流
   * @return 反序列化后的参数对象数组
   */
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { _ =>
      readObject(dis, server.jvmObjectTracker)
    }.toArray
  }

  /**
   * 根据参数匹配方法或构造函数的签名，支持类型兼容转换
   * 原始类型会自动转换为对应包装类型，Java数组会自动转换为Scala Seq
   * 返回第一个匹配到的签名的索引
   * 
   * @param parameterTypesOfMethods 所有候选方法/构造函数的参数类型数组
   * @param args 调用参数数组，会在匹配过程中对需要转换的参数就地转换
   * @return 匹配到的候选索引，None表示无匹配
   */
  def findMatchedSignature(
      parameterTypesOfMethods: Array[Array[Class[_]]],
      args: Array[Object]): Option[Int] = {
    val numArgs = args.length

    // 遍历所有候选签名
    for (index <- parameterTypesOfMethods.indices) {
      val parameterTypes = parameterTypesOfMethods(index)

      // 参数数量必须一致
      if (parameterTypes.length == numArgs) {
        var argMatched = true
        var i = 0
        while (i < numArgs && argMatched) {
          val parameterType = parameterTypes(i)

          // 特殊处理：参数类型是Scala Seq，参数是Java数组，认为匹配，后续转换
          if (parameterType == classOf[Seq[Any]] && args(i).getClass.isArray) {
            // 不需要标记不匹配，留到后面转换
          } else {
            var parameterWrapperType = parameterType

            // 原始类型转换为对应包装类型，因为参数都存储为Object
            if (parameterType.isPrimitive) {
              parameterWrapperType = parameterType match {
                case java.lang.Integer.TYPE => classOf[java.lang.Integer]
                case java.lang.Long.TYPE => classOf[java.lang.Integer]
                case java.lang.Double.TYPE => classOf[java.lang.Double]
                case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
                case _ => parameterType
              }
            }
            // 检查参数类型是否兼容，如果不兼容则标记不匹配
            if ((parameterType.isPrimitive || args(i) != null) &&
                !parameterWrapperType.isInstance(args(i))) {
              argMatched = false
            }
          }

          i = i + 1
        }

        // 如果所有参数都匹配，就地进行需要的参数转换后返回索引
        if (argMatched) {
          // 目前返回第一个匹配的方法，TODO后续可以实现最优匹配选择
          val parameterTypes = parameterTypesOfMethods(index)

          // 转换需要兼容的参数类型：Java数组转为不可变Scala Seq
          for (i <- 0 until numArgs) {
            if (parameterTypes(i) == classOf[Seq[Any]] && args(i).getClass.isArray) {
              args(i) = args(i).asInstanceOf[Array[_]].toImmutableArraySeq
            }
          }

          return Some(index)
        }
      }
    }
    // 没有找到任何匹配
    None
  }
}
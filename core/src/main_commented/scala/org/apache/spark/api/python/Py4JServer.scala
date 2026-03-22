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

package org.apache.spark.api.python

import java.net.InetAddress
import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Py4J服务器封装类，支持GatewayServer和ClientServer两种模式，实现Python线程到JVM线程的绑定映射
 * 为PySpark提供Python客户端与JVM核心之间的双向通信通道
 * @param sparkConf Spark配置对象，用于获取认证密钥配置和线程绑定配置
 */
private[spark] class Py4JServer(sparkConf: SparkConf) extends Logging {
  private[spark] val secret: String = Utils.createSecret(sparkConf)

  // 启动Py4J网关或客户端服务端，供Python进程连接，允许Python进程访问JVM系统属性等信息
  // 绑定到本地回环地址，仅允许本地Python进程连接
  private val localhost = InetAddress.getLoopbackAddress()
  // 根据环境变量PYSPARK_PIN_THREAD选择服务端模式，默认开启线程绑定
  private[spark] val server = if (sys.env.getOrElse(
      "PYSPARK_PIN_THREAD", "true").toLowerCase(Locale.ROOT) == "true") {
    // 开启线程绑定，使用ClientServer模式
    new py4j.ClientServer.ClientServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .build()
  } else {
    // 不开启线程绑定，使用传统GatewayServer模式
    new py4j.GatewayServer.GatewayServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
      .build()
  }

  /**
   * 启动Py4J服务端，根据服务端类型调用对应启动方法
   */
  def start(): Unit = server match {
    case clientServer: py4j.ClientServer => clientServer.startServer()
    case gatewayServer: py4j.GatewayServer => gatewayServer.start()
    case other => throw SparkCoreErrors.unexpectedPy4JServerError(other)
  }

  /**
   * 获取JVM服务端监听的端口号，供Python客户端连接使用
   * @return JVM服务端监听端口
   */
  def getListeningPort: Int = server match {
    case clientServer: py4j.ClientServer => clientServer.getJavaServer.getListeningPort
    case gatewayServer: py4j.GatewayServer => gatewayServer.getListeningPort
    case other => throw SparkCoreErrors.unexpectedPy4JServerError(other)
  }

  /**
   * 关闭Py4J服务端，释放通信资源
   */
  def shutdown(): Unit = server match {
    case clientServer: py4j.ClientServer => clientServer.shutdown()
    case gatewayServer: py4j.GatewayServer => gatewayServer.shutdown()
    case other => throw SparkCoreErrors.unexpectedPy4JServerError(other)
  }
}
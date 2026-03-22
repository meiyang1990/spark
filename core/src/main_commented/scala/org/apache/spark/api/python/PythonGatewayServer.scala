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

import java.io.{DataOutputStream, File, FileOutputStream}
import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CLASS_NAME, PATH}

/**
 * 文件级注释：PySpark Python网关服务启动入口，负责在临时端口启动Py4J服务器，供Python客户端连接JVM核心
 *
 * 该进程由PySpark驱动程序通过SparkSubmit启动，用于建立Python与JVM之间的Py4J通信通道
 */
private[spark] object PythonGatewayServer extends Logging {
  initializeLogIfNecessary(true)

  /**
   * Python网关服务主入口方法，启动Py4J服务器并向Python进程返回连接信息
   * @param args 启动参数，此处未使用
   */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    // 创建Py4J服务器实例
    val gatewayServer: Py4JServer = new Py4JServer(sparkConf)

    // 启动Py4J服务器监听连接
    gatewayServer.start()
    // 获取服务器绑定的监听端口
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      // 端口绑定失败，退出进程
      logError(log"${MDC(CLASS_NAME, gatewayServer.server.getClass)} failed to bind; exiting")
      System.exit(1)
    } else {
      // 仅监听本地回环地址，保证安全性
      val address = InetAddress.getLoopbackAddress()
      logDebug(s"Started PythonGatewayServer on $address with port $boundPort")
    }

    // 从环境变量获取连接信息文件路径，将连接信息写入该文件供Python进程读取
    val connectionInfoPath = new File(sys.env("_PYSPARK_DRIVER_CONN_INFO_PATH"))
    // 先创建临时文件避免写入不完整问题
    val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
      "connection", ".info").toFile()

    // 打开临时文件输出流
    val dos = new DataOutputStream(new FileOutputStream(tmpPath))
    // 写入监听端口号
    dos.writeInt(boundPort)

    // 写入访问秘钥，用于身份验证
    val secretBytes = gatewayServer.secret.getBytes(UTF_8)
    dos.writeInt(secretBytes.length)
    dos.write(secretBytes, 0, secretBytes.length)
    dos.close()

    // 将临时文件重命名为目标文件，保证原子性写入
    if (!tmpPath.renameTo(connectionInfoPath)) {
      logError(log"Unable to write connection information to ${MDC(PATH, connectionInfoPath)}.")
      System.exit(1)
    }

    // 阻塞读取标准输入，当Python进程退出后会触发EOF，本进程随之退出，保证孤儿进程被清理
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}
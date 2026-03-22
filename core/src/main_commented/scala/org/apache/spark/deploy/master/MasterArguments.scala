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

package org.apache.spark.deploy.master

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.MASTER_UI_PORT
import org.apache.spark.util.{IntParam, Utils}

/**
 * Spark Standalone模式Master节点启动参数解析类
 * 负责解析命令行参数、环境变量和配置文件，得到Master启动所需的所有配置信息
 */
private[master] class MasterArguments(args: Array[String], conf: SparkConf) extends Logging {
  var host = Utils.localHostName()
  var port = 7077
  var webUiPort = 8080
  var propertiesFile: String = null

  // 从环境变量读取SPARK_MASTER_HOST覆盖默认主机地址
  if (System.getenv("SPARK_MASTER_HOST") != null) {
    host = System.getenv("SPARK_MASTER_HOST")
  }
  // 从环境变量读取SPARK_MASTER_PORT覆盖默认服务端口
  if (System.getenv("SPARK_MASTER_PORT") != null) {
    port = System.getenv("SPARK_MASTER_PORT").toInt
  }
  // 从环境变量读取SPARK_MASTER_WEBUI_PORT覆盖默认Web UI端口
  if (System.getenv("SPARK_MASTER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("SPARK_MASTER_WEBUI_PORT").toInt
  }

  // 解析命令行参数
  parse(args.toList)

  // 加载默认Spark配置文件到SparkConf，后续必须在此行之后才能访问配置
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)
  // 在配置加载完成后，重新初始化日志系统，使结构化日志配置生效
  Utils.resetStructuredLogging(conf)
  Logging.uninitialize()

  // 从SparkConf中读取UI端口配置，覆盖之前的设置
  if (conf.contains(MASTER_UI_PORT.key)) {
    webUiPort = conf.get(MASTER_UI_PORT)
  }

  /**
   * 尾递归解析命令行参数列表
   * @param args 待解析的参数列表
   */
  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => // No-op

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * 打印Master使用帮助信息，并以指定退出码退出JVM
   * @param exitCode 退出码
   */
  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off println
    System.err.println(
      "Usage: Master [options]\n" +
      "\n" +
      "Options:\n" +
      "  -h HOST, --host HOST   Hostname to listen on\n" +
      "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
      "  --webui-port PORT      Port for web UI (default: 8080)\n" +
      "  --properties-file FILE Path to a custom Spark properties file.\n" +
      "                         Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }
}
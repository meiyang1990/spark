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

package org.apache.spark.deploy

import java.net.{URI, URISyntaxException}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

import org.apache.logging.log4j.Level

import org.apache.spark.util.{IntParam, MemoryParam, Utils}

/**
 * 文件描述：Spark Standalone 模式驱动客户端命令行参数解析器
 * 核心功能：解析 driver client 启动、kill 两种命令的命令行参数，完成参数校验和初始化
 */
/**
 * Command-line parser for the driver client.
 */
private[deploy] class ClientArguments(args: Array[String]) {
  import ClientArguments._

  // 命令类型：launch 启动Driver 或 kill 停止Driver
  var cmd: String = "" // 'launch' or 'kill'
  var logLevel = Level.WARN

  // 启动Driver的参数
  var masters: Array[String] = null
  var jarUrl: String = ""
  var mainClass: String = ""
  var supervise: Boolean = DEFAULT_SUPERVISE
  var memory: Int = DEFAULT_MEMORY
  var cores: Int = DEFAULT_CORES
  private val _driverOptions = ListBuffer[String]()
  def driverOptions: Seq[String] = _driverOptions.toSeq

  // 停止Driver的参数
  var driverId: String = ""

  parse(args.toList)

  /**
   * 尾递归方式解析命令行参数列表，匹配不同参数选项依次处理
   * @param args 待解析的命令行参数列表
   */
  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value
      parse(tail)

    case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
      memory = value
      parse(tail)

    case ("--supervise" | "-s") :: tail =>
      supervise = true
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case ("--verbose" | "-v") :: tail =>
      logLevel = Level.INFO
      parse(tail)

    case "launch" :: _master :: _jarUrl :: _mainClass :: tail =>
      cmd = "launch"

      if (!ClientArguments.isValidJarUrl(_jarUrl)) {
        // scalastyle:off println
        println(s"Jar url '${_jarUrl}' is not in valid format.")
        println(s"Must be a jar file path in URL format " +
          "(e.g. hdfs://host:port/XX.jar, file:///XX.jar)")
        // scalastyle:on println
        printUsageAndExit(-1)
      }

      jarUrl = _jarUrl
      // 解析多master地址，支持高可用配置
      masters = Utils.parseStandaloneMasterUrls(_master)
      mainClass = _mainClass
      // 将剩余参数作为Driver应用的自定义参数
      _driverOptions ++= tail

    case "kill" :: _master :: _driverId :: tail =>
      cmd = "kill"
      // 解析多master地址
      masters = Utils.parseStandaloneMasterUrls(_master)
      driverId = _driverId

    case _ =>
      // 参数不匹配，打印帮助并退出
      printUsageAndExit(1)
  }

  /**
   * 打印帮助信息并以指定退出码退出JVM
   * @param exitCode JVM退出码
   */
  private def printUsageAndExit(exitCode: Int): Unit = {
    // TODO: It wouldn't be too hard to allow users to submit their app and dependency jars
    //       separately similar to in the YARN client.
    val usage =
     s"""
      |Usage: DriverClient [options] launch <active-master> <jar-url> <main-class> [driver options]
      |Usage: DriverClient kill <active-master> <driver-id>
      |
      |Options:
      |   -c CORES, --cores CORES        Number of cores to request (default: $DEFAULT_CORES)
      |   -m MEMORY, --memory MEMORY     Megabytes of memory to request (default: $DEFAULT_MEMORY)
      |   -s, --supervise                Whether to restart the driver on failure
      |                                  (default: $DEFAULT_SUPERVISE)
      |   -v, --verbose                  Print more debugging output
     """.stripMargin
    // scalastyle:off println
    System.err.println(usage)
    // scalastyle:on println
    System.exit(exitCode)
  }
}

/**
 * ClientArguments 伴生对象，定义默认参数和工具方法
 */
private[deploy] object ClientArguments {
  // 默认请求CPU核数
  val DEFAULT_CORES = 1
  // 默认请求Driver内存，单位MB
  val DEFAULT_MEMORY = Utils.DEFAULT_DRIVER_MEM_MB // MB
  // 默认不开启失败自动重启
  val DEFAULT_SUPERVISE = false

  /**
   * 校验Jar包URL格式是否合法
   * @param s 待校验的Jar包地址字符串
   * @return 合法返回true，否则返回false
   */
  def isValidJarUrl(s: String): Boolean = {
    try {
      val uri = new URI(s)
      // 需要存在scheme、路径，且路径后缀为.jar
      uri.getScheme != null && uri.getPath != null && uri.getPath.endsWith(".jar")
    } catch {
      // URI语法错误返回false
      case _: URISyntaxException => false
    }
  }
}
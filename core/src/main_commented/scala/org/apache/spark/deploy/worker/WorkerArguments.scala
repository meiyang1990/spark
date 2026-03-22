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

package org.apache.spark.deploy.worker

import java.lang.management.ManagementFactory

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Worker._
import org.apache.spark.util.{IntParam, MemoryParam, Utils}

/**
 * Worker节点启动命令行参数解析器，负责解析Worker启动时的各项配置参数
 * 处理环境变量、命令行参数、配置文件，最终生成Worker启动所需的全部配置
 */
private[worker] class WorkerArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 0
  var webUiPort = 8081
  var cores = inferDefaultCores()
  var memory = inferDefaultMemory()
  var masters: Array[String] = null
  var workDir: String = null
  var propertiesFile: String = null

  // 从环境变量中读取Worker配置，覆盖默认值
  if (System.getenv("SPARK_WORKER_PORT") != null) {
    port = System.getenv("SPARK_WORKER_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_CORES") != null) {
    cores = System.getenv("SPARK_WORKER_CORES").toInt
  }
  if (conf.getenv("SPARK_WORKER_MEMORY") != null) {
    memory = Utils.memoryStringToMb(conf.getenv("SPARK_WORKER_MEMORY"))
  }
  if (System.getenv("SPARK_WORKER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("SPARK_WORKER_WEBUI_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_DIR") != null) {
    workDir = System.getenv("SPARK_WORKER_DIR")
  }

  // 解析命令行参数
  parse(args.toList)

  // 加载默认Spark配置文件，会修改传入的SparkConf对象，后续读取必须在此行之后
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)
  // 结构化日志配置生效后，重新初始化日志系统
  Utils.resetStructuredLogging(conf)
  Logging.uninitialize()

  // 如果SparkConf中配置了UI端口，覆盖之前的值
  conf.get(WORKER_UI_PORT).foreach { webUiPort = _ }

  // 检查Worker可用内存配置是否合法
  checkWorkerMemory()

  /**
   * 尾递归方式解析命令行参数列表，匹配不同参数选项并赋值
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

    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value
      parse(tail)

    case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
      memory = value
      parse(tail)

    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      if (masters != null) {  // 传入了多个位置参数，格式错误
        printUsageAndExit(1)
      }
      // 解析Master地址，支持多Master地址配置
      masters = Utils.parseStandaloneMasterUrls(value)
      parse(tail)

    case Nil =>
      if (masters == null) {  // 没有传入Master地址，格式错误
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * 打印Worker命令行使用说明并退出JVM
   * @param exitCode JVM退出码
   */
  def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off println
    System.err.println(
      "Usage: Worker [options] <master>\n" +
      "\n" +
      "Master must be a URL of the form spark://hostname:port\n" +
      "\n" +
      "Options:\n" +
      "  -c CORES, --cores CORES  Number of cores to use\n" +
      "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
      "  -d DIR, --work-dir DIR   Directory to run apps in (default: SPARK_HOME/work)\n" +
      "  -h HOST, --host HOST     Hostname to listen on\n" +
      "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
      "  --webui-port PORT        Port for web UI (default: 8081)\n" +
      "  --properties-file FILE   Path to a custom Spark properties file.\n" +
      "                           Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }

  /**
   * 推断默认CPU核数，直接使用当前机器可用处理器数量
   * @return 默认可用CPU核数
   */
  def inferDefaultCores(): Int = {
    Runtime.getRuntime.availableProcessors()
  }

  /**
   * 推断默认可用内存，获取系统总物理内存后预留1GB给操作系统
   * @return 默认可用内存大小，单位MB
   */
  def inferDefaultMemory(): Int = {
    var totalMb = 0
    try {
      // scalastyle:off classforname
      val bean = ManagementFactory.getOperatingSystemMXBean()
      val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
      val method = beanClass.getDeclaredMethod("getTotalMemorySize")
      totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      // scalastyle:on classforname
    } catch {
      case e: Exception =>
        // 获取系统内存失败，使用默认值2GB
        totalMb = 2*1024
        // scalastyle:off println
        System.out.println("Failed to get total physical memory. Using " + totalMb + " MB")
        // scalastyle:on println
    }
    // 预留1GB给操作系统，不返回小于默认值的内存大小
    math.max(totalMb - 1024, Utils.DEFAULT_DRIVER_MEM_MB)
  }

  /**
   * 检查Worker配置的可用内存是否合法，不合法则抛出异常终止启动
   */
  def checkWorkerMemory(): Unit = {
    if (memory <= 0) {
      val message = "Memory is below 1MB, or missing a M/G at the end of the memory specification?"
      throw new IllegalStateException(message)
    }
  }
}
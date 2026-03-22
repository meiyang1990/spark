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

package org.apache.spark.deploy.history

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, History}
import org.apache.spark.util.Utils

/**
 * 文件: HistoryServerArguments.scala
 * 所属模块: Spark core 模块，历史服务器功能组件
 * 核心职责: 解析历史服务器启动命令行参数，加载配置文件并初始化日志系统
 */

/**
 * 历史服务器命令行参数解析器，负责解析启动参数，加载配置并初始化环境
 * @param conf Spark配置对象，解析结果会写入该对象
 * @param args 启动时传入的命令行参数数组
 */
private[history] class HistoryServerArguments(conf: SparkConf, args: Array[String])
  extends Logging {
  private var propertiesFile: String = null

  // 启动命令行参数解析流程
  parse(args.toList)

  /**
   * 尾递归方式解析命令行参数列表
   * @param args 待解析的命令行参数列表
   */
  @tailrec
  private def parse(args: List[String]): Unit = {
    args match {
      // 匹配帮助选项，打印帮助信息并退出
      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      // 匹配配置文件路径选项，保存路径后继续解析剩余参数
      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      // 参数解析完毕，退出递归
      case Nil =>

      // 匹配无法识别的参数，打印错误和帮助信息并退出
      case other =>
        val errorMsg = s"Unrecognized options: ${other.mkString(" ")}\n"
        printUsageAndExit(1, errorMsg)
    }
  }

  // 加载默认Spark配置属性，会修改传入的SparkConf对象，必须在参数解析完成后执行
  Utils.loadDefaultSparkProperties(conf, propertiesFile)
  // 配置加载完成后，根据新配置重置结构化日志系统
  Utils.resetStructuredLogging(conf)
  // 重新初始化日志系统，使日志配置生效
  Logging.uninitialize()

  /**
   * 打印帮助信息并退出进程
   * @param exitCode 进程退出码
   * @param error 错误提示信息，默认为空
   */
  // scalastyle:off line.size.limit println
  private def printUsageAndExit(exitCode: Int, error: String = ""): Unit = {
    // 从History配置对象中提取所有配置项定义
    val configs = History.getClass.getDeclaredFields
      .filter(f => classOf[ConfigEntry[_]].isAssignableFrom(f.getType))
      .map { f =>
        f.setAccessible(true)
        f.get(History).asInstanceOf[ConfigEntry[_]]
      }
    // 计算最长配置键长度，用于输出对齐
    val maxConfigLength = configs.map(_.key.length).max
    val sb = new StringBuilder(
      s"""
         |${error}Usage: HistoryServer [options]
         |
         |Options:
         |  ${"--properties-file FILE".padTo(maxConfigLength, ' ')} Path to a custom Spark properties file.
         |  ${"".padTo(maxConfigLength, ' ')} Default is conf/spark-defaults.conf.
         |
         |Configuration options can be set by setting the corresponding JVM system property.
         |History Server options are always available; additional options depend on the provider.
         |
         |""".stripMargin)

    /**
     * 将配置项格式化输出到字符串缓冲区，自动折行处理长描述
     * @param configs 待输出的配置项数组
     */
    def printConfigs(configs: Array[ConfigEntry[_]]): Unit = {
      // 按配置键排序输出
      configs.sortBy(_.key).foreach { conf =>
        sb.append("  ").append(conf.key.padTo(maxConfigLength, ' '))
        var currentDocLen = 0
        // 折行后缩进对齐
        val intention = "\n" + " ".repeat(maxConfigLength + 2)
        // 按单词分割，自动折行保证每行不超过60字符
        conf.doc.split("\\s+").foreach { word =>
          if (currentDocLen + word.length > 60) {
            sb.append(intention).append(" ").append(word)
            currentDocLen = word.length + 1
          } else {
            sb.append(" ").append(word)
            currentDocLen += word.length + 1
          }
        }
        // 追加默认值信息
        sb.append(intention).append(" (Default: ").append(conf.defaultValueString).append(")\n")
      }
    }
    // 将配置项分为通用配置和文件系统历史提供者配置两类分别展示
    val (common, fs) = configs.partition(!_.key.startsWith("spark.history.fs."))
    sb.append("History Server options:\n")
    printConfigs(common)
    sb.append("FsHistoryProvider options:\n")
    printConfigs(fs)
    // 输出帮助信息到标准错误
    System.err.println(sb.toString())
    // scalastyle:on line.size.limit println
    // 退出进程
    System.exit(exitCode)
  }
}
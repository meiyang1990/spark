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

import java.util.{Arrays => JArrays, List => JList}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkUserAppException
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher.SPARK_API_MODE
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.util.SparkExitCode

/**
 * 文件级注释：Spark Pipelines 声明式流水线命令行入口实现
 * 
 * 核心职责：实现spark-pipelines命令行接口，将参数分流：
 * 1. 通用Spark提交参数转发给spark-submit处理
 * 2. 流水线专属参数传递给内部Python CLI实现，由Python加载用户代码并提交到后端执行
 * 当前仅支持Spark Connect模式运行声明式流水线
 */
/**
 * Outer implementation of the spark-pipelines command line interface. Responsible for routing
 * spark-submit args to spark-submit, and pipeline-specific args to the inner Python CLI
 * implementation that loads the user code and submits it to the backend.
 */
object SparkPipelines extends Logging {
  /**
   * spark-pipelines命令行入口主函数
   * @param args 命令行输入参数，第一个参数为Python CLI文件路径，后续为提交参数和流水线参数
   */
  def main(args: Array[String]): Unit = {
    val pipelinesCliFile = args(0)
    val sparkSubmitAndPipelinesArgs = args.slice(1, args.length)
    SparkSubmit.main(
      constructSparkSubmitArgs(pipelinesCliFile, sparkSubmitAndPipelinesArgs).toArray)
  }

  /**
   * 构造最终传递给SparkSubmit的参数列表
   * @param pipelinesCliFile 流水线Python CLI实现文件路径
   * @param args 拆分前的原始参数数组
   * @return 拼接完成的SparkSubmit参数序列
   */
  protected[deploy] def constructSparkSubmitArgs(
      pipelinesCliFile: String,
      args: Array[String]): Seq[String] = {
    val (sparkSubmitArgs, pipelinesArgs) = splitArgs(args)
    sparkSubmitArgs ++ Seq(pipelinesCliFile) ++ pipelinesArgs
  }

  /**
   * 将输入参数拆分为SparkSubmit原生参数和流水线专属参数
   * @param args 原始输入参数数组
   * @return 拆分后的(SparkSubmit参数序列, 流水线参数序列)二元组
   */
  private def splitArgs(args: Array[String]): (Seq[String], Seq[String]) = {
    val sparkSubmitArgs = new ArrayBuffer[String]()
    val pipelinesArgs = new ArrayBuffer[String]()
    var remote = "local"

    // 继承SparkSubmit参数解析器，自定义参数处理逻辑分流参数
    new SparkSubmitArgumentsParser() {
      parse(JArrays.asList(args: _*))

      override protected def handle(opt: String, value: String): Boolean = {
        // 解析--remote参数，保存远程地址供后续强制配置
        if (opt == "--remote") {
          remote = value
        } else if (opt == "--class") {
          // 声明式流水线不支持自定义主类，参数非法直接报错
          logError("--class argument not supported.")
          throw SparkUserAppException(SparkExitCode.EXIT_FAILURE)
        } else if ((opt == "--conf" || opt == "-c") && value.startsWith(s"$SPARK_API_MODE=")) {
          val apiMode = value.stripPrefix(s"$SPARK_API_MODE=").trim
          // 声明式流水线仅支持Spark Connect模式，其他模式非法报错
          if (apiMode.toLowerCase(Locale.ROOT) != "connect") {
            logError(
              s"$SPARK_API_MODE must be 'connect' (was '$apiMode'). " +
                "Declarative Pipelines currently only supports Spark Connect."
            )
            throw SparkUserAppException(SparkExitCode.EXIT_FAILURE)
          }
        } else if (Seq("--name", "-h", "--help").contains(opt)) {
          // 帮助等通用参数归到流水线参数，由Python CLI处理
          pipelinesArgs += opt
          if (value != null && value.nonEmpty) {
            pipelinesArgs += value
          }
        } else {
          // 其余已知SparkSubmit参数归到原生提交参数
          sparkSubmitArgs += opt
          if (value != null) {
            sparkSubmitArgs += value
          }
        }

        true
      }

      override protected def handleExtraArgs(extra: JList[String]): Unit = {
        // 额外位置参数全部归到流水线参数
        pipelinesArgs.appendAll(extra.asScala)
      }

      override protected def handleUnknown(opt: String): Boolean = {
        // 未知参数全部归到流水线参数，由Python CLI处理
        pipelinesArgs += opt
        true
      }
    }

    // 强制添加Spark Connect模式配置，确保运行模式符合要求
    sparkSubmitArgs += "--conf"
    sparkSubmitArgs += s"$SPARK_API_MODE=connect"
    sparkSubmitArgs += "--remote"
    sparkSubmitArgs += remote
    (sparkSubmitArgs.toSeq, pipelinesArgs.toSeq)
  }

}
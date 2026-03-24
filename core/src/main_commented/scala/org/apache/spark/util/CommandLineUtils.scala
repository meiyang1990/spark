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

package org.apache.spark.util

import java.io.PrintStream

/**
 * 文件说明: 提供Spark命令行工具的基础公共能力，包含命令行解析框架和通用CLI选项处理能力，供Spark各类命令行入口工具继承使用
 */
/**
 * 提供基础命令行解析功能和通用Spark CLI选项解析方法的基础特质
 * 所有Spark命令行工具可以继承该特质获得标准CLI处理能力
 */
private[spark] trait CommandLineUtils extends CommandLineLoggingUtils {

  /**
   * 命令行入口方法，由具体实现类实现业务逻辑
   * @param args 命令行输入参数数组
   */
  def main(args: Array[String]): Unit
}

/**
 * 提供命令行工具日志输出和异常退出能力的特质
 * 封装了标准化的错误输出、帮助提示和进程退出流程，支持测试替换退出和输出行为
 */
private[spark] trait CommandLineLoggingUtils {
  // Exposed for testing
  /**
   * 进程退出函数，可被测试替换以避免实际退出JVM
   * 参数1: exitCode 退出状态码
   * 参数2: cause 导致退出的异常，None表示无异常
   */
  private[spark] var exitFn: (Int, Option[Throwable]) => Unit =
    (exitCode: Int, cause: Option[Throwable]) => {
      // 如果存在异常，打印堆栈到输出流
      cause.foreach(_.printStackTrace(printStream))
      System.exit(exitCode)
    }

  /** 输出流，默认指向标准错误流，可被测试替换 */
  private[spark] var printStream: PrintStream = System.err

  // scalastyle:off println
  /**
   * 打印消息到输出流
   * @param str 待打印的消息内容
   */
  private[spark] def printMessage(str: String): Unit = printStream.println(str)
  // scalastyle:on println

  /**
   * 打印错误信息并退出进程
   * 自动添加帮助提示，使用退出码1表示错误退出
   * @param str 错误描述信息
   */
  private[spark] def printErrorAndExit(str: String): Unit = {
    printMessage("Error: " + str)
    printMessage("Run with --help for usage help or --verbose for debug output")
    exitFn(1, None)
  }
}
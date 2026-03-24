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

/**
 * Spark应用退出码定义集合，定义了Spark进程不同退出场景对应的标准退出码，
 * 供Spark进程终止时返回给操作系统和调用方，标识应用退出原因。
 * 仅在Spark内部使用，不对外暴露。
 */
private[spark] object SparkExitCode {

  /** 执行成功，正常退出 */
  val EXIT_SUCCESS = 0

  /** 执行失败，通用错误退出 */
  val EXIT_FAILURE = 1

  /** Shell内置命令使用错误，参数或用法不符合预期 */
  val ERROR_MISUSE_SHELL_BUILTIN = 2

  /** 指定路径不存在，无法找到指定路径 */
  val ERROR_PATH_NOT_FOUND = 3

  /** Executor失败次数超过阈值，触发退出 */
  val EXCEED_MAX_EXECUTOR_FAILURES = 11

  /** 进入默认未捕获异常处理器处理，未捕获异常导致退出 */
  val UNCAUGHT_EXCEPTION = 50

  /** 未捕获异常处理器在日志输出过程中再次发生异常，双重异常导致退出 */
  val UNCAUGHT_EXCEPTION_TWICE = 51

  /** 未捕获异常为内存溢出错误（OutOfMemoryError）导致退出 */
  val OOM = 52

  /** 找不到类定义（ClassNotFoundException/NoClassDefFoundError）导致退出 */
  val CLASS_NOT_FOUND = 101

  /** Driver运行时间超过设定超时阈值，触发超时退出 */
  val DRIVER_TIMEOUT = 124

  /** 找不到可执行命令，命令不存在 */
  val ERROR_COMMAND_NOT_FOUND = 127
}
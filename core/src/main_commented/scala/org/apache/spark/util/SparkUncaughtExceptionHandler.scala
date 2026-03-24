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

import org.apache.spark.SparkEnv
import org.apache.spark.executor.{ExecutorExitCode, KilledByTaskReaperException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.THREAD
import org.apache.spark.internal.config.KILL_ON_FATAL_ERROR_DEPTH

/**
 * 文件概述: Spark守护进程默认的未捕获异常处理器，负责处理未被捕获的异常并根据规则终止进程
 * 
 * Spark守护进程默认的未捕获异常处理器，对任何Error都会终止整个进程，
 * 当exitOnException标志为true时，遇到Exception也会终止进程。
 * 主要用于Executor、Driver等后台进程的异常处理，保证进程在遇到严重错误时正确退出。
 *
 * @param exitOnUncaughtException 是否在遇到未捕获异常时终止进程
 */
private[spark] class SparkUncaughtExceptionHandler(val exitOnUncaughtException: Boolean = true)
  extends Thread.UncaughtExceptionHandler with Logging {

  // 预加载SparkExitCode类，避免Spark jar包损坏时无法正常退出，解决SPARK-44542问题
  locally {
    // eagerly load SparkExitCode class, so the System.exit and runtime.halt have a chance to be
    // executed when the disk containing Spark jars is corrupted. See SPARK-44542 for more details.
    val _ = SparkExitCode.OOM
  }

  // 异常原因链中搜索致命错误的最大深度，配置来自KILL_ON_FATAL_ERROR_DEPTH
  // SPARK-50034: 在指定深度内如果找到致命错误，会识别并以正确退出码终止进程
  private val killOnFatalErrorDepth: Int =
    // SparkEnv可能还未初始化，此时使用默认值5
    Option(SparkEnv.get).map(_.conf.get(KILL_ON_FATAL_ERROR_DEPTH)).getOrElse(5)


  /**
   * 处理线程中未捕获的异常，根据异常类型和配置决定是否终止进程
   * 
   * @param thread 抛出未捕获异常的线程
   * @param exception 未被捕获的异常对象
   */
  override def uncaughtException(thread: Thread, exception: Throwable): Unit = {
    try {
      val mdc = MDC(THREAD, thread)
      // 在日志中明确标记容器是否正在关闭，帮助用户分析Executor日志
      if (ShutdownHookManager.inShutdown()) {
        logError(log"[Container in shutdown] Uncaught exception in thread $mdc", exception)
      } else {
        logError(log"Uncaught exception in thread $mdc", exception)
      }

      // 如果已经进入关闭钩子流程，不能调用System.exit()，否则会导致死锁
      if (!ShutdownHookManager.inShutdown()) {
        // 遍历异常原因链，最多遍历killOnFatalErrorDepth层
        var currentException: Throwable = exception
        var depth = 0

        while (currentException != null && depth < killOnFatalErrorDepth) {
          currentException match {
            // 遇到OOM错误，直接以OOM退出码终止进程
            case _: OutOfMemoryError =>
              System.exit(SparkExitCode.OOM)
            // SparkFatalException包装了OOM，同样以OOM退出码终止进程，SPARK-24294防御性处理
            case e: SparkFatalException if e.throwable.isInstanceOf[OutOfMemoryError] =>
              // SPARK-24294: This is defensive code, in case that SparkFatalException is
              // misused and uncaught.
              System.exit(SparkExitCode.OOM)
            // 被TaskReaper杀死，配置允许退出则以对应退出码终止进程
            case _: KilledByTaskReaperException if exitOnUncaughtException =>
              System.exit(ExecutorExitCode.KILLED_BY_TASK_REAPER)
            // 未匹配到致命错误，继续遍历原因链
            case _ =>
          }
          // 移动到异常链下一个原因
          currentException = currentException.getCause
          depth += 1
        }

        // 未找到特定致命错误，配置允许退出则以通用未捕获异常退出码终止进程，SPARK-30310
        if (exitOnUncaughtException) {
          System.exit(SparkExitCode.UNCAUGHT_EXCEPTION)
        }
      }
    } catch {
      // 处理处理过程中再次抛出的OOM
      case oom: OutOfMemoryError =>
        try {
          logError(
            log"Uncaught OutOfMemoryError in thread ${MDC(THREAD, thread)}, process halted.",
            oom)
        } catch {
          // 忽略日志输出过程中的任何异常，我们即将终止进程
          case _: Throwable =>
        }
        // 使用halt强制终止进程，避免死锁
        Runtime.getRuntime.halt(SparkExitCode.OOM)
      // 处理处理过程中再次抛出的其他异常
      case t: Throwable =>
        try {
          logError(
            log"Another uncaught exception in thread ${MDC(THREAD, thread)}, process halted.",
            t)
        } catch {
          // 忽略日志输出过程中的任何异常
          case _: Throwable =>
        }
        // 使用halt强制终止进程，返回多次未捕获异常的退出码
        Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  /**
   * 便捷方法，使用当前线程处理未捕获异常
   * 
   * @param exception 未被捕获的异常对象
   */
  def uncaughtException(exception: Throwable): Unit = {
    uncaughtException(Thread.currentThread(), exception)
  }
}
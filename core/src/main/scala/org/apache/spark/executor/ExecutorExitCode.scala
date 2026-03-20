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

package org.apache.spark.executor

import org.apache.spark.util.SparkExitCode._

/**
 * These are exit codes that executors should use to provide the master with information about
 * executor failures assuming that cluster management framework can capture the exit codes (but
 * perhaps not log files). The exit code constants here are chosen to be unlikely to conflict
 * with "natural" exit statuses that may be caused by the JVM or user code. In particular,
 * exit codes 128+ arise on some Unix-likes as a result of signals, and it appears that the
 * OpenJDK JVM may use exit code 1 in some of its own "last chance" code.
 *
 * Executor 退出码定义。当 Executor 失败时，使用这些退出码向 Master 汇报失败信息。
 * 这些退出码经过精心选择，避免与 JVM 或用户代码的"自然"退出状态冲突：
 * - Unix 系统中 128+ 的退出码通常表示进程被信号终止
 * - OpenJDK JVM 可能在某些场景下使用退出码 1
 */
private[spark]
object ExecutorExitCode {

  // DiskStore 多次尝试后仍无法创建本地临时目录（可能是 spark.local.dir 配置问题）
  /** DiskStore failed to create a local temporary directory after many attempts. */
  val DISK_STORE_FAILED_TO_CREATE_DIR = 53

  // 外部块存储初始化失败
  /** ExternalBlockStore failed to initialize after many attempts. */
  val EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE = 54

  // 外部块存储无法创建本地临时目录
  /** ExternalBlockStore failed to create a local temporary directory after many attempts. */
  val EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR = 55

  /**
   * Executor is unable to send heartbeats to the driver more than
   * "spark.executor.heartbeat.maxFailures" times.
   */
  // Executor 无法向 Driver 发送心跳超过最大失败次数（由 spark.executor.heartbeat.maxFailures 控制）
  val HEARTBEAT_FAILURE = 56

  /** The default uncaught exception handler was reached and the exception was thrown by
   * TaskReaper. */
  // 被 TaskReaper 强制终止（任务长时间无法响应终止信号时触发）
  val KILLED_BY_TASK_REAPER = 57

  // BlockManager 重新注册失败
  /** Executor is unable to re-register BlockManager. */
  val BLOCK_MANAGER_REREGISTRATION_FAILED = 58

  // 根据退出码返回可读的失败原因描述
  def explainExitCode(exitCode: Int): String = {
    exitCode match {
      case UNCAUGHT_EXCEPTION => "Uncaught exception"
      case UNCAUGHT_EXCEPTION_TWICE => "Uncaught exception, and logging the exception failed"
      case OOM => "OutOfMemoryError"
      case DISK_STORE_FAILED_TO_CREATE_DIR =>
        "Failed to create local directory (bad spark.local.dir?)"
      // TODO: replace external block store with concrete implementation name
      case EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE => "ExternalBlockStore failed to initialize."
      // TODO: replace external block store with concrete implementation name
      case EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR =>
        "ExternalBlockStore failed to create a local temporary directory."
      case HEARTBEAT_FAILURE =>
        "Unable to send heartbeats to driver."
      case BLOCK_MANAGER_REREGISTRATION_FAILED =>
        "Executor killed due to a failure of block manager re-registration."
      case KILLED_BY_TASK_REAPER =>
        "Executor killed by TaskReaper."
      case _ =>
        "Unknown executor exit code (" + exitCode + ")" + (
          if (exitCode > 128) {
            " (died from signal " + (exitCode - 128) + "?)"
          } else {
            ""
          }
        )
    }
  }
}

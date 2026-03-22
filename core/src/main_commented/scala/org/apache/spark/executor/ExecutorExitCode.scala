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

package org.apache.spark.executor

import org.apache.spark.util.SparkExitCode._

/**
 * 这些是Executor应当使用的退出码，用于在集群管理框架能够捕获退出码（但不一定能捕获日志文件）的场景下，
 * 向Master提供Executor失败原因信息。这里选择的退出码尽量避免与JVM或用户代码自然产生的退出状态冲突。
 * 特别说明：Unix-like系统中128+的退出码通常由信号触发，OpenJDK JVM在一些自身的"最后机会"代码中可能会使用退出码1。
 * 
 * Executor退出码定义，用于向集群管理者汇报Executor不同类型的失败原因，避免与系统默认退出码冲突
 */
private[spark]
object ExecutorExitCode {

  // DiskStore 多次尝试后仍无法创建本地临时目录，通常是spark.local.dir配置错误导致
  /** DiskStore failed to create a local temporary directory after many attempts. */
  val DISK_STORE_FAILED_TO_CREATE_DIR = 53

  // 外部块存储多次尝试后仍初始化失败
  /** ExternalBlockStore failed to initialize after many attempts. */
  val EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE = 54

  // 外部块存储多次尝试后仍无法创建本地临时目录
  /** ExternalBlockStore failed to create a local temporary directory after many attempts. */
  val EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR = 55

  /**
   * Executor is unable to send heartbeats to the driver more than
   * "spark.executor.heartbeat.maxFailures" times.
   */
  // Executor向Driver发送心跳失败次数超过配置的最大值spark.executor.heartbeat.maxFailures
  val HEARTBEAT_FAILURE = 56

  /** The default uncaught exception handler was reached and the exception was thrown by
   * TaskReaper. */
  // 被TaskReaper强制终止，通常是任务长时间无法响应终止信号时触发
  val KILLED_BY_TASK_REAPER = 57

  /** Executor is unable to re-register BlockManager. */
  // BlockManager重新注册失败，Executor被Driver杀死
  val BLOCK_MANAGER_REREGISTRATION_FAILED = 58

  /**
   * 根据退出码返回对应可读的失败原因描述
   * @param exitCode Executor退出码
   * @return 可读的失败原因字符串
   */
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
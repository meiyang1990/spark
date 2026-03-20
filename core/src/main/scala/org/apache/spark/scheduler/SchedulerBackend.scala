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

// 这个文件已经全部加上中文注释

package org.apache.spark.scheduler

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.storage.BlockManagerId

/**
 * 调度系统的后端接口，允许在 TaskSchedulerImpl 下插入不同的后端实现。
 * 该接口假设一种模型：当机器变为可用时，应用程序获得资源供给（resource offers），
 * 并可以在这些机器上启动任务。
 */
private[spark] trait SchedulerBackend {
  // 默认应用ID
  private val appId = "spark-application-" + System.currentTimeMillis

  /** 启动后端 */
  def start(): Unit
  /** 停止后端 */
  def stop(): Unit
  /** 带退出码停止后端 */
  def stop(exitCode: Int): Unit = stop()
  /** 更新 Executor 的日志级别 */
  def updateExecutorsLogLevel(logLevel: String): Unit = {}
  /**
   * 刷新当前的资源供给并重新调度任务。
   * 当有新任务提交或 Executor 状态变化时调用。
   */
  def reviveOffers(): Unit
  /** 获取默认并行度 */
  def defaultParallelism(): Int

  /**
   * 请求 Executor 终止一个正在运行的任务。
   *
   * @param taskId 任务ID
   * @param executorId 任务所在的 Executor ID
   * @param interruptThread 是否中断任务线程
   * @param reason 终止原因
   */
  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException

  /** 获取任务的线程堆栈信息 */
  def getTaskThreadDump(taskId: Long, executorId: String): Option[ThreadStackTrace]

  /** 判断后端是否就绪 */
  def isReady(): Boolean = true

  /**
   * 获取与作业关联的应用程序ID。
   *
   * @return 应用程序ID
   */
  def applicationId(): String = appId

  /**
   * 获取本次运行的尝试ID（如果集群管理器支持多次尝试）。
   * 以 Client 模式运行的应用不会有尝试ID。
   *
   * @return 应用程序的尝试ID（如果可用）
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * 获取 Driver 的日志URL。这些URL用于在 UI 的 Executors 标签页中显示 Driver 的链接。
   * @return 包含日志名称及其对应URL的映射
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

  /**
   * 获取 Driver 的属性。当指定了自定义日志URL模式时，这些属性用于替换日志URL中的占位符。
   * @return 包含 Driver 属性的映射
   */
  def getDriverAttributes: Option[Map[String, String]] = None

  /**
   * 基于 ResourceProfile 获取可以并发启动的最大任务数，即使其中一些当前正在使用。
   * 注意：请不要缓存此方法返回的值，因为添加/移除 Executor 会导致该数值变化。
   *
   * @param rp 用于计算最大并发任务数的资源配置
   * @return 当前可以并发启动的最大任务数
   */
  def maxNumConcurrentTasks(rp: ResourceProfile): Int

  /**
   * 获取基于推送的 Shuffle (Push-based Shuffle) 的主机位置列表。
   *
   * 目前对于 Stage 重试和 Stage 复用场景，推送式 Shuffle 是禁用的
   * （例如少量分区因故障丢失的情况）。因此此方法对于每个 ShuffleDependency 只应调用一次。
   * @return 外部 Shuffle 服务的位置列表
   */
  def getShufflePushMergerLocations(
      numPartitions: Int,
      resourceProfileId: Int): Seq[BlockManagerId] = Nil

}

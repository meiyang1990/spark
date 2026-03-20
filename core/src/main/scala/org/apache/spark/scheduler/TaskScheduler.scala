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

import scala.collection.mutable.Map

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
 * 底层任务调度器接口，目前仅由 [[org.apache.spark.scheduler.TaskSchedulerImpl]] 实现。
 * 该接口允许插入不同的任务调度器实现。每个 TaskScheduler 为单个 SparkContext 调度任务。
 * 这些调度器从 DAGScheduler 接收每个 Stage 提交的任务集(TaskSet)，负责将任务发送到集群、
 * 运行任务、在失败时重试、以及缓解慢任务(Straggler)。它们将事件返回给 DAGScheduler。
 */
private[spark] trait TaskScheduler {

  // 默认应用ID，使用时间戳生成
  private val appId = "spark-application-" + System.currentTimeMillis

  /** 获取调度池根节点 */
  def rootPool: Pool

  /** 获取调度模式（FIFO/FAIR） */
  def schedulingMode: SchedulingMode

  /** 启动任务调度器 */
  def start(): Unit

  // 系统成功初始化后调用（通常在 SparkContext 中）。
  // YARN 使用此方法基于首选位置引导资源分配、等待 Executor 注册等。
  def postStartHook(): Unit = { }

  // 断开与集群的连接。
  def stop(exitCode: Int = 0): Unit

  // 提交一组任务进行运行。
  def submitTasks(taskSet: TaskSet): Unit

  /**
   * 终止一个任务尝试。
   * 如果后端不支持终止任务，则抛出 UnsupportedOperationException。
   *
   * @return 任务是否被成功终止
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // 终止同一 Stage ID 的所有 Stage 尝试中的所有任务。
  // 如果后端不支持终止任务，则抛出 UnsupportedOperationException。
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit

  // 通知该 Stage 对应的 TaskSetManager，某个分区已经完成，可以跳过该分区的任务运行。
  def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit

  // 设置 DAG 调度器以进行上行调用。保证在 submitTasks 被调用之前完成设置。
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // 获取集群的默认并行度，作为任务规模的参考。
  def defaultParallelism(): Int

  /**
   * 更新正在运行的任务和 Executor 的指标，并通知 Master BlockManager 仍然存活。
   * 如果 Driver 知道给定的 BlockManager，返回 true；否则返回 false，表示 BlockManager 需要重新注册。
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean

  /**
   * 获取与作业关联的应用程序ID。
   *
   * @return 应用程序ID
   */
  def applicationId(): String = appId

  /**
   * 处理正在退役的 Executor。
   */
  def executorDecommission(executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit

  /**
   * 如果 Executor 已退役，返回其对应的退役状态信息。
   */
  def getExecutorDecommissionState(executorId: String): Option[ExecutorDecommissionState]

  /**
   * 处理丢失的 Executor。
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * 处理被移除的 Worker。
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * 获取与作业关联的应用程序尝试ID。
   *
   * @return 应用程序的尝试ID
   */
  def applicationAttemptId(): Option[String]

}

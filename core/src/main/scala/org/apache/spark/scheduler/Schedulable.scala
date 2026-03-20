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

package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * 可调度实体接口。
 * 有两种可调度实体类型：Pool（调度池）和 TaskSetManager（任务集管理器）。
 * Pool 是中间节点，用于组织调度层次结构；TaskSetManager 是叶子节点，管理具体的任务集。
 */
private[spark] trait Schedulable {
  /** 父调度池 */
  var parent: Pool
  /** 子可调度实体队列 */
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]
  /** 调度模式（FIFO/FAIR） */
  def schedulingMode: SchedulingMode
  /** 权重，用于公平调度算法中的资源分配比例 */
  def weight: Int
  /** 最小份额，公平调度中保证的最低资源量 */
  def minShare: Int
  /** 当前正在运行的任务数 */
  def runningTasks: Int
  /** 优先级，FIFO 调度中使用 */
  def priority: Int
  /** 关联的 Stage ID */
  def stageId: Int
  /** 可调度实体的名称 */
  def name: String

  /** 判断当前实体是否可被调度 */
  def isSchedulable: Boolean
  /** 添加子可调度实体 */
  def addSchedulable(schedulable: Schedulable): Unit
  /** 移除子可调度实体 */
  def removeSchedulable(schedulable: Schedulable): Unit
  /** 按名称查找可调度实体 */
  def getSchedulableByName(name: String): Schedulable
  /** 处理 Executor 丢失事件，递归通知所有子实体 */
  def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit
  /** 处理 Executor 退役事件 */
  def executorDecommission(executorId: String): Unit
  /** 检查是否有任务满足推测执行条件 */
  def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean
  /** 获取按调度算法排序后的 TaskSetManager 队列 */
  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]
}

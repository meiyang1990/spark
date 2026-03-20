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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * 可调度实体，表示 Pool（调度池）或 TaskSetManager 的集合。
 * Pool 是调度树中的中间节点，用于将 TaskSetManager 组织成层次结构。
 * 在 FAIR 调度模式下，多个 Pool 共享集群资源；在 FIFO 模式下只有一个根 Pool。
 *
 * @param poolName 调度池名称
 * @param schedulingMode 调度模式（FAIR/FIFO）
 * @param initMinShare 初始最小份额
 * @param initWeight 初始权重
 */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable with Logging {

  /** 子可调度实体队列（线程安全） */
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  /** 名称到可调度实体的映射（线程安全），用于快速查找 */
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  var runningTasks = 0
  val priority = 0

  // Pool 的 stageId 用于调度排序时打破平局
  var stageId = -1
  val name = poolName
  var parent: Pool = null

  /** 根据调度模式选择对应的排序算法 */
  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }

  override def isSchedulable: Boolean = true

  /** 添加子可调度实体，并将其父节点设置为当前 Pool */
  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  /** 移除子可调度实体 */
  override def removeSchedulable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  /** 按名称递归查找可调度实体，先在当前 Pool 查找，再递归查找子实体 */
  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  /** 向所有子实体递归传播 Executor 丢失事件 */
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  /** 向所有子实体递归传播 Executor 退役事件 */
  override def executorDecommission(executorId: String): Unit = {
    schedulableQueue.asScala.foreach(_.executorDecommission(executorId))
  }

  /** 检查所有子实体中是否有可推测执行的任务 */
  override def checkSpeculatableTasks(minTimeToSpeculation: Long): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  /** 获取按调度算法排序后的 TaskSetManager 队列，只包含可调度的任务集 */
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue.filter(_.isSchedulable)
    }
    sortedTaskSetQueue
  }

  /** 增加正在运行的任务数，并递归更新父 Pool */
  def increaseRunningTasks(taskNum: Int): Unit = {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  /** 减少正在运行的任务数，并递归更新父 Pool */
  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}

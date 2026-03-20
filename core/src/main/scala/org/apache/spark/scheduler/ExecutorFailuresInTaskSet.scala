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

import scala.collection.mutable.HashMap

/**
 * 用于跟踪任务失败信息的辅助类，服务于Executor排除机制。
 * 记录在一个TaskSet中，某个Executor上所有任务的失败信息。
 *
 * @param node 该Executor所在的节点主机名
 */
private[scheduler] class ExecutorFailuresInTaskSet(val node: String) {
  /**
   * 从TaskSet中任务索引到（失败次数, 最近失败时间）的映射。
   */
  val taskToFailureCountAndFailureTime = HashMap[Int, (Int, Long)]()

  /**
   * 记录一次任务失败。
   * @param taskIndex 失败任务在TaskSet中的索引
   * @param failureTime 失败时间戳（来自Driver时钟）
   */
  def updateWithFailure(taskIndex: Int, failureTime: Long): Unit = {
    val (prevFailureCount, prevFailureTime) =
      taskToFailureCountAndFailureTime.getOrElse(taskIndex, (0, -1L))
    // 时间戳始终来自Driver，无需担心时钟偏差，但仍做防御性处理以应对时钟非单调性
    val newFailureTime = math.max(prevFailureTime, failureTime)
    taskToFailureCountAndFailureTime(taskIndex) = (prevFailureCount + 1, newFailureTime)
  }

  /** 在此Executor上失败过的不同任务数量 */
  def numUniqueTasksWithFailures: Int = taskToFailureCountAndFailureTime.size

  /**
   * 返回此Executor在指定任务索引上的失败次数。
   */
  def getNumTaskFailures(index: Int): Int = {
    taskToFailureCountAndFailureTime.getOrElse(index, (0, 0))._1
  }

  override def toString(): String = {
    s"numUniqueTasksWithFailures = $numUniqueTasksWithFailures; " +
      s"tasksToFailureCount = $taskToFailureCountAndFailureTime"
  }
}

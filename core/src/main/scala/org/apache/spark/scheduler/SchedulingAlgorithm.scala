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

/**
 * 调度排序算法接口。
 * FIFO：在 TaskSetManager 之间使用先进先出算法。
 * Fair(FS)：在 Pool 之间使用公平调度算法，Pool 内部可以使用 FIFO 或公平调度。
 */
private[spark] trait SchedulingAlgorithm {
  /** 比较两个可调度实体的优先级，返回 true 表示 s1 应排在 s2 前面 */
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}

/**
 * FIFO 调度算法实现。
 * 先按优先级(priority)排序，优先级相同时按 Stage ID 排序，值小的优先。
 */
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    // 先按优先级比较
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      // 优先级相同则按 Stage ID 比较
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}

/**
 * 公平调度算法实现。
 * 调度策略：
 * 1. 未满足最小份额(minShare)的实体优先于已满足的
 * 2. 两者都未满足时，按最小份额使用比例(runningTasks/minShare)排序
 * 3. 两者都已满足时，按任务权重比(runningTasks/weight)排序
 * 4. 比例相同时，按名称字典序排序
 */
private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    // 判断是否低于最小份额（即"饥饿"状态）
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    // 计算最小份额使用比例
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    // 计算任务数与权重的比例
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    var compare = 0
    if (s1Needy && !s2Needy) {
      // s1 饥饿而 s2 不饥饿，s1 优先
      return true
    } else if (!s1Needy && s2Needy) {
      // s2 饥饿而 s1 不饥饿，s2 优先
      return false
    } else if (s1Needy && s2Needy) {
      // 两者都饥饿，按最小份额使用比例排序，比例小的优先
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      // 两者都不饥饿，按任务权重比排序，比例小的优先
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      // 比例相同时按名称字典序排序
      s1.name < s2.name
    }
  }
}


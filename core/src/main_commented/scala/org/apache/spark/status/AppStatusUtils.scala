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

package org.apache.spark.status

import org.apache.spark.status.api.v1.TaskData

/**
 * 应用状态统计工具类，为Spark UI提供任务指标计算能力
 */
private[spark] object AppStatusUtils {

  // 所有表示任务已结束的状态集合
  private val TASK_FINISHED_STATES = Set("FAILED", "KILLED", "SUCCESS")

  /**
   * 判断任务是否已经执行完成
   * @param task 任务数据对象
   * @return true表示任务已结束，false表示任务仍在运行
   */
  private def isTaskFinished(task: TaskData): Boolean = {
    TASK_FINISHED_STATES.contains(task.status)
  }

  /**
   * 计算任务的调度延迟时间（调度队列等待时间 + 数据拉取时间等非执行耗时）
   * @param task 任务数据对象
   * @return 调度延迟总毫秒数
   */
  def schedulerDelay(task: TaskData): Long = {
    if (isTaskFinished(task) && task.taskMetrics.isDefined && task.duration.isDefined) {
      val m = task.taskMetrics.get
      schedulerDelay(task.launchTime.getTime(), fetchStart(task), task.duration.get,
        m.executorDeserializeTime, m.resultSerializationTime, m.executorRunTime)
    } else {
      // 任务仍在运行，指标未完整采集，返回0
      0L
    }
  }

  /**
   * 计算任务获取结果的耗时
   * @param task 任务数据对象
   * @return 获取结果的毫秒数
   */
  def gettingResultTime(task: TaskData): Long = {
    gettingResultTime(task.launchTime.getTime(), fetchStart(task), task.duration.getOrElse(-1L))
  }

  /**
   * 计算任务调度延迟，总耗时减去各类执行阶段耗时得到调度等待耗时
   * @param launchTime 任务启动时间戳（毫秒）
   * @param fetchStart 结果拉取开始时间戳（毫秒）
   * @param duration 任务总耗时（毫秒）
   * @param deserializeTime 执行器反序列化耗时（毫秒）
   * @param serializeTime 结果序列化耗时（毫秒）
   * @param runTime 执行器运行任务耗时（毫秒）
   * @return 调度延迟毫秒数
   */
  def schedulerDelay(
      launchTime: Long,
      fetchStart: Long,
      duration: Long,
      deserializeTime: Long,
      serializeTime: Long,
      runTime: Long): Long = {
    math.max(0, duration - runTime - deserializeTime - serializeTime -
      gettingResultTime(launchTime, fetchStart, duration))
  }

  /**
   * 计算任务结果拉取阶段耗时
   * @param launchTime 任务启动时间戳（毫秒）
   * @param fetchStart 结果拉取开始时间戳（毫秒）
   * @param duration 任务总耗时（毫秒），-1表示任务未完成
   * @return 结果拉取耗时毫秒数
   */
  def gettingResultTime(launchTime: Long, fetchStart: Long, duration: Long): Long = {
    if (fetchStart > 0) {
      if (duration > 0) {
        launchTime + duration - fetchStart
      } else {
        System.currentTimeMillis() - fetchStart
      }
    } else {
      0L
    }
  }

  /**
   * 从任务对象中获取结果拉取开始时间戳
   * @param task 任务数据对象
   * @return 结果拉取开始时间戳，无数据则返回-1
   */
  private def fetchStart(task: TaskData): Long = {
    if (task.resultFetchStart.isDefined) {
      task.resultFetchStart.get.getTime()
    } else {
      -1
    }
  }

  /**
   * 基于已排序数据计算分位数值，用于任务指标分位数统计
   * @param values 已排序的输入值序列
   * @param quantiles 需要计算的分位数数组（0~1范围）
   * @return 对应分位数的结果序列
   */
  def getQuantilesValue(
    values: IndexedSeq[Double],
    quantiles: Array[Double]): IndexedSeq[Double] = {
    val count = values.size
    if (count > 0) {
      val indices = quantiles.map { q => math.min((q * count).toLong, count - 1) }
      indices.map(i => values(i.toInt)).toIndexedSeq
    } else {
      IndexedSeq.fill(quantiles.length)(0.0)
    }
  }
}
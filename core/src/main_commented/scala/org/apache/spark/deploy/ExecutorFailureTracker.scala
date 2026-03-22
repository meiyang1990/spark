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

package org.apache.spark.deploy

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Streaming.STREAMING_DYN_ALLOCATION_MAX_EXECUTORS
import org.apache.spark.util.{Clock, SystemClock, Utils

/**
 * 文件: Executor失败追踪器，负责分别统计每个主机和全局的Executor失败记录，用于故障判定
 * 核心功能是跟踪一定时间窗口内的Executor失败次数，为集群故障判断提供依据
 */
private[spark] class ExecutorFailureTracker(
  sparkConf: SparkConf,
  val clock: Clock = new SystemClock) extends Logging {

  // Executor失败记录的有效时间窗口，超出窗口内的失败才会被计数，配置为-1表示不设有效期
  private val executorFailuresValidityInterval =
    sparkConf.get(EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS).getOrElse(-1L)

  // 存储每个主机上Executor失败的时间戳队列
  private val failedExecutorsTimeStampsPerHost = mutable.Map[String, mutable.Queue[Long]]()

  // 全局所有主机Executor失败的时间戳队列
  private val failedExecutorsTimeStamps = new mutable.Queue[Long]()

  /**
   * 清理过期失败记录，统计当前有效窗口内的失败次数
   * @param failedExecutorsWithTimeStamps 存储失败时间戳的队列
   * @return 当前有效失败次数
   */
  private def updateAndCountFailures(failedExecutorsWithTimeStamps: mutable.Queue[Long]): Int = {
    val endTime = clock.getTimeMillis()
    // 移除超出有效期的失败记录，从队头开始清理
    while (executorFailuresValidityInterval > 0 &&
      failedExecutorsWithTimeStamps.nonEmpty &&
      failedExecutorsWithTimeStamps.head < endTime - executorFailuresValidityInterval) {
      failedExecutorsWithTimeStamps.dequeue()
    }
    failedExecutorsWithTimeStamps.size
  }

  /**
   * 获取全局所有主机的有效Executor失败总数
   * @return 全局有效失败次数
   */
  def numFailedExecutors: Int = synchronized {
    updateAndCountFailures(failedExecutorsTimeStamps)
  }

  /**
   * 注册指定主机上的一次Executor失败，同时更新全局和按主机的失败记录
   * @param hostname 发生失败的主机地址
   */
  def registerFailureOnHost(hostname: String): Unit = synchronized {
    val timeMillis = clock.getTimeMillis()
    failedExecutorsTimeStamps.enqueue(timeMillis)
    // 获取或新建该主机的失败队列
    val failedExecutorsOnHost =
      failedExecutorsTimeStampsPerHost.getOrElse(hostname, {
        val failureOnHost = mutable.Queue[Long]()
        failedExecutorsTimeStampsPerHost.put(hostname, failureOnHost)
        failureOnHost
      })
    failedExecutorsOnHost.enqueue(timeMillis)
  }

  /**
   * 注册一次全局的Executor失败，不更新按主机分类的记录
   */
  def registerExecutorFailure(): Unit = synchronized {
    val timeMillis = clock.getTimeMillis()
    failedExecutorsTimeStamps.enqueue(timeMillis)
  }

  /**
   * 获取指定主机上的有效Executor失败次数
   * @param hostname 目标主机地址
   * @return 该主机有效失败次数
   */
  def numFailuresOnHost(hostname: String): Int = {
    failedExecutorsTimeStampsPerHost.get(hostname).map { failedExecutorsOnHost =>
      updateAndCountFailures(failedExecutorsOnHost)
    }.getOrElse(0)
  }
}

/**
 * 提供计算最大允许Executor失败次数的工具方法
 */
object ExecutorFailureTracker {

  /**
   * 计算应用允许的最大Executor失败次数，默认值为当前总Executor数的2倍，最小为3，动态分配场景下使用最大Executor数计算
   * @param sparkConf Spark配置对象
   * @return 最大允许的Executor失败次数
   */
  def maxNumExecutorFailures(sparkConf: SparkConf): Int = {
    // 计算默认最大失败次数的内部方法
    def defaultMaxNumExecutorFailures: Int = {
      // 根据分配模式获取有效总Executor数
      val effectiveNumExecutors =
        if (Utils.isStreamingDynamicAllocationEnabled(sparkConf)) {
          sparkConf.get(STREAMING_DYN_ALLOCATION_MAX_EXECUTORS)
        } else if (Utils.isDynamicAllocationEnabled(sparkConf)) {
          sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS)
        } else {
          sparkConf.get(EXECUTOR_INSTANCES).getOrElse(0)
        }
      // 计算结果不超过Int边界，保证最小值为3，避免整数溢出
      math.max(3,
        if (effectiveNumExecutors > Int.MaxValue / 2) Int.MaxValue else 2 * effectiveNumExecutors)
    }

    // 用户配置了自定义值则使用用户配置，否则使用默认计算值
    sparkConf.get(MAX_EXECUTOR_FAILURES).getOrElse(defaultMaxNumExecutorFailures)
  }
}
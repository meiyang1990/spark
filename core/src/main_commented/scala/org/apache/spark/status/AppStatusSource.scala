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

/**
 * 文件路径: core/src/main/scala/org/apache/spark/status/AppStatusSource.scala
 * 应用状态指标源，为Spark应用提供作业、阶段、任务、执行器等核心组件运行指标的Metrics统计支持
 */
package org.apache.spark.status

import java.util.concurrent.atomic.AtomicLong

import AppStatusSource.getCounter
import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.METRICS_APP_STATUS_SOURCE_ENABLED
import org.apache.spark.metrics.source.Source

/**
 * 作业总时长计量Gauge实现，用于暴露当前应用累计作业总时长指标
 * @param value 存储累计作业总时长的原子变量，单位毫秒
 */
private [spark] class JobDuration(val value: AtomicLong) extends Gauge[Long] {
  override def getValue: Long = value.get()
}

/**
 * 应用状态指标源，向Metrics系统暴露Spark应用运行过程中的各类统计指标
 * 包含作业、阶段、任务、执行器的计数统计和作业总时长统计，用于监控应用运行状态
 */
private[spark] class AppStatusSource extends Source {

  override implicit val metricRegistry: MetricRegistry = new MetricRegistry()

  override val sourceName = "appStatus"

  val jobDuration = new JobDuration(new AtomicLong(0L))

  // 注册作业总时长指标，单位为毫秒
  val JOB_DURATION = metricRegistry
    .register(MetricRegistry.name("jobDuration"), jobDuration)

  // 失败阶段计数
  val FAILED_STAGES = getCounter("stages", "failedStages")

  // 跳过阶段计数
  val SKIPPED_STAGES = getCounter("stages", "skippedStages")

  // 完成阶段计数
  val COMPLETED_STAGES = getCounter("stages", "completedStages")

  // 成功作业计数
  val SUCCEEDED_JOBS = getCounter("jobs", "succeededJobs")

  // 失败作业计数
  val FAILED_JOBS = getCounter("jobs", "failedJobs")

  // 完成任务计数
  val COMPLETED_TASKS = getCounter("tasks", "completedTasks")

  // 失败任务计数
  val FAILED_TASKS = getCounter("tasks", "failedTasks")

  // 被杀掉任务计数
  val KILLED_TASKS = getCounter("tasks", "killedTasks")

  // 跳过任务计数
  val SKIPPED_TASKS = getCounter("tasks", "skippedTasks")

  // 应用级拉黑执行器计数，不包含阶段级拉黑，已废弃，改用EXCLUDED_EXECUTORS
  @deprecated("use excludedExecutors instead", "3.1.0")
  val BLACKLISTED_EXECUTORS = getCounter("tasks", "blackListedExecutors")

  // 应用级取消拉黑执行器计数，不包含阶段级取消拉黑，已废弃，改用UNEXCLUDED_EXECUTORS
  @deprecated("use unexcludedExecutors instead", "3.1.0")
  val UNBLACKLISTED_EXECUTORS = getCounter("tasks", "unblackListedExecutors")

  // 应用级排除执行器计数，不包含阶段级排除
  val EXCLUDED_EXECUTORS = getCounter("tasks", "excludedExecutors")

  // 应用级取消排除执行器计数，不包含阶段级取消排除
  val UNEXCLUDED_EXECUTORS = getCounter("tasks", "unexcludedExecutors")

}

/**
 * AppStatusSource的伴生对象，提供指标创建和源实例初始化工具方法
 */
private[spark] object AppStatusSource {

  /**
   * 在指定指标注册表中创建并注册一个Counter计数器
   * @param prefix 指标名称前缀
   * @param name 指标名称
   * @param metricRegistry 指标注册表
   * @return 注册好的Counter计数器实例
   */
  def getCounter(prefix: String, name: String)(implicit metricRegistry: MetricRegistry): Counter = {
    metricRegistry.counter(MetricRegistry.name(prefix, name))
  }

  /**
   * 根据配置创建AppStatusSource实例，仅当配置开启时才会创建
   * @param conf Spark配置
   * @return  Some(AppStatusSource)如果开启，None如果未开启
   */
  def createSource(conf: SparkConf): Option[AppStatusSource] = {
    Option(conf.get(METRICS_APP_STATUS_SOURCE_ENABLED))
      .filter(identity)
      .map { _ => new AppStatusSource() }
  }
}
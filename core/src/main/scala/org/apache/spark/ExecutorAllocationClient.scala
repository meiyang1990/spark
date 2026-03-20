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

package org.apache.spark

import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.util.ArrayImplicits._

/**
 * 与集群管理器通信以请求或销毁 Executor 的客户端接口。
 * 目前仅在 Kubernetes 和 YARN 模式下支持。
 */
private[spark] trait ExecutorAllocationClient {

  /** 获取当前活跃的 Executor ID 列表 */
  private[spark] def getExecutorIds(): Seq[String]

  /**
   * 检查 Executor 是否活跃（可用于执行任务）。
   */
  def isExecutorActive(id: String): Boolean

  /**
   * 向集群管理器更新调度需求，包含三项信息帮助其做决策：
   *
   * @param resourceProfileIdToNumExecutors 每个 ResourceProfile 期望的 Executor 总数
   *         （集群管理器不应为达到此数而 kill 已运行的 Executor）
   * @param numLocalityAwareTasksPerResourceProfileId 每个 ResourceProfile 中有数据本地性偏好的任务数
   * @param hostToLocalTaskCount 每个 ResourceProfile 中各主机期望运行的任务数
   * @return 集群管理器是否确认了此请求
   */
  private[spark] def requestTotalExecutors(
      resourceProfileIdToNumExecutors: Map[Int, Int],
      numLocalityAwareTasksPerResourceProfileId: Map[Int, Int],
      hostToLocalTaskCount: Map[Int, Map[String, Int]]): Boolean

  /**
   * 为默认 ResourceProfile 请求额外的 Executor。
   * @return 集群管理器是否确认了此请求
   */
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * 请求集群管理器 kill 指定的 Executor。
   *
   * @param executorIds 要 kill 的 Executor ID 列表
   * @param adjustTargetNumExecutors 是否在 kill 后调低目标 Executor 数
   * @param countFailures 是否将 kill 时正在运行的任务计为失败
   * @param force 是否强制 kill 繁忙的 Executor
   * @return 集群管理器确认移除的 Executor ID 列表
   */
  def killExecutors(
    executorIds: Seq[String],
    adjustTargetNumExecutors: Boolean,
    countFailures: Boolean,
    force: Boolean = false): Seq[String]

  /**
   * 请求集群管理器优雅下线指定的 Executor。
   * 默认实现委托给 kill，如果调度器支持优雅下线需重写此方法。
   *
   * @param executorsAndDecomInfo Executor ID 及其下线信息
   * @param adjustTargetNumExecutors 是否在下线后调低目标数
   * @param triggeredByExecutor 是否由 Executor 端触发的下线
   * @return 集群管理器确认移除的 Executor ID 列表
   */
  def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = {
    killExecutors(executorsAndDecomInfo.map(_._1).toImmutableArraySeq,
      adjustTargetNumExecutors,
      countFailures = false)
  }


  /**
   * 请求下线单个 Executor（委托给 decommissionExecutors）。
   * @return 集群管理器是否确认了此请求
   */
  final def decommissionExecutor(
      executorId: String,
      decommissionInfo: ExecutorDecommissionInfo,
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean = false): Boolean = {
    val decommissionedExecutors = decommissionExecutors(
      Array((executorId, decommissionInfo)),
      adjustTargetNumExecutors = adjustTargetNumExecutors,
      triggeredByExecutor = triggeredByExecutor)
    decommissionedExecutors.nonEmpty && decommissionedExecutors(0).equals(executorId)
  }

  /**
   * 请求下线指定主机上的所有 Executor。
   */
  def decommissionExecutorsOnHost(host: String): Boolean

  /**
   * 请求 kill 指定主机上的所有 Executor。
   */
  def killExecutorsOnHost(host: String): Boolean

  /** 请求 kill 单个 Executor */
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId), adjustTargetNumExecutors = true,
      countFailures = false)
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}

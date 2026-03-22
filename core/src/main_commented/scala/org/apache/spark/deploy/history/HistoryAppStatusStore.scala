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

package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorLogUrlHandler
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.KVStore

/**
 * 历史服务器应用状态存储，为已完成/不完整的历史应用提供状态查询能力
 * 扩展通用AppStatusStore，支持自定义Executor日志URL的替换处理
 *
 * @param conf Spark配置
 * @param store 存储应用状态数据的KV存储
 */
private[spark] class HistoryAppStatusStore(
    conf: SparkConf,
    store: KVStore)
  extends AppStatusStore(store, None) with Logging {

  /** 根据配置和应用状态，决定是否启用自定义Executor日志URL模式 */
  private val logUrlPattern: Option[String] = {
    val appInfo = super.applicationInfo()
    val applicationCompleted = appInfo.attempts.nonEmpty && appInfo.attempts.head.completed
    // 应用已完成 或 配置允许对不完整应用应用自定义URL，才返回配置的模式
    if (applicationCompleted || conf.get(APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP)) {
      conf.get(CUSTOM_EXECUTOR_LOG_URL)
    } else {
      None
    }
  }

  /** 用于处理自定义Executor日志URL替换的处理器实例 */
  private val logUrlHandler = new ExecutorLogUrlHandler(logUrlPattern)

  /**
   * 获取Executor列表，若配置了自定义URL则替换日志链接
   * @param activeOnly 是否只返回活跃Executor
   * @return 处理后的Executor概要列表
   */
  override def executorList(activeOnly: Boolean): Seq[v1.ExecutorSummary] = {
    val execList = super.executorList(activeOnly)
    if (logUrlPattern.nonEmpty) {
      execList.map(replaceLogUrls)
    } else {
      execList
    }
  }

  /**
   * 获取单个Executor概要信息，若配置了自定义URL则替换日志链接
   * @param executorId Executor ID
   * @return 处理后的Executor概要
   */
  override def executorSummary(executorId: String): v1.ExecutorSummary = {
    val execSummary = super.executorSummary(executorId)
    if (logUrlPattern.nonEmpty) {
      replaceLogUrls(execSummary)
    } else {
      execSummary
    }
  }

  /**
   * 替换单个Executor概要中的日志URL
   * @param exec 原始Executor概要
   * @return 替换后的Executor概要
   */
  private def replaceLogUrls(exec: v1.ExecutorSummary): v1.ExecutorSummary = {
    val newLogUrlMap = logUrlHandler.applyPattern(exec.executorLogs, exec.attributes)
    replaceExecutorLogs(exec, newLogUrlMap)
  }

  /**
   * 构造替换了日志URL的新Executor概要对象
   * @param source 原始Executor概要
   * @param newExecutorLogs 替换后的日志URL映射
   * @return 新的Executor概要对象
   */
  private def replaceExecutorLogs(
      source: v1.ExecutorSummary,
      newExecutorLogs: Map[String, String]): v1.ExecutorSummary = {
    new v1.ExecutorSummary(source.id, source.hostPort, source.isActive, source.rddBlocks,
      source.memoryUsed, source.diskUsed, source.totalCores, source.maxTasks, source.activeTasks,
      source.failedTasks, source.completedTasks, source.totalTasks, source.totalDuration,
      source.totalGCTime, source.totalInputBytes, source.totalShuffleRead,
      source.totalShuffleWrite, source.isBlacklisted, source.maxMemory, source.addTime,
      source.removeTime, source.removeReason, newExecutorLogs, source.memoryMetrics,
      source.blacklistedInStages, source.peakMemoryMetrics, source.attributes, source.resources,
      source.resourceProfileId, source.isExcluded, source.excludedInStages)
  }

}
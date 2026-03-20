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

package org.apache.spark.shuffle

import org.apache.spark.{FetchFailed, TaskContext, TaskFailedReason}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Shuffle 数据块获取失败异常。
 * Executor 捕获此异常并通过 TaskEndReason 传递给 DAGScheduler，以触发上一 Stage 的重新提交。
 *
 * 注意：bmAddress 可以为 null。
 *
 * 为防止用户代码隐藏此获取失败，在构造函数中调用 [[TaskContext.setFetchFailed()]]。
 * 这意味着创建此异常后必须立即抛出——不能创建后检查某些条件再决定忽略
 * （否则可能触发其他异常）。参见 SPARK-19276。
 *
 * @param bmAddress 失败的 BlockManager 地址，可为 null
 * @param shuffleId Shuffle ID
 * @param mapId Map 任务 ID
 * @param mapIndex Map 任务索引
 * @param reduceId Reduce ID（分区 ID）
 * @param message 错误消息
 * @param cause 原始异常
 */
private[spark] class FetchFailedException(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Long,
    mapIndex: Int,
    reduceId: Int,
    message: String,
    cause: Throwable = null)
  extends Exception(message, cause) {

  def this(
      bmAddress: BlockManagerId,
      shuffleId: Int,
      mapTaskId: Long,
      mapIndex: Int,
      reduceId: Int,
      cause: Throwable) = {
    this(bmAddress, shuffleId, mapTaskId, mapIndex, reduceId, cause.getMessage, cause)
  }

  // SPARK-19276: 将获取失败设置到任务上下文中，即使有用户代码拦截此异常（可能包装它），
  // Executor 仍能识别发生了获取失败，并将正确的错误消息发送回 Driver。
  // 使用 Option 包装是因为某些测试场景下 TaskContext 未定义。
  Option(TaskContext.get()).foreach(_.setFetchFailed(this))

  /** 将异常转换为 TaskFailedReason，用于向 Driver 报告失败原因 */
  def toTaskFailedReason: TaskFailedReason = FetchFailed(
    bmAddress, shuffleId, mapId, mapIndex, reduceId, Utils.exceptionString(this))
}

/**
 * 从 [[org.apache.spark.MapOutputTracker]] 获取 Shuffle 元数据失败异常。
 * 当无法从 MapOutputTracker 获取 Map 输出位置信息时抛出。
 */
private[spark] class MetadataFetchFailedException(
    shuffleId: Int,
    reduceId: Int,
    message: String)
  extends FetchFailedException(null, shuffleId, -1L, -1, reduceId, message)

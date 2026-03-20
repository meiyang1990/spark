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

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{NUM_MERGER_LOCATIONS, SHUFFLE_ID, STAGE_ID}
import org.apache.spark.scheduler.MapStatus

/**
 * 自定义 Shuffle 写入流程的接口。
 * Driver 创建 ShuffleWriteProcessor 并放入 [[ShuffleDependency]]，
 * Executor 在每个 ShuffleMapTask 中使用它。
 */
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * 从任务上下文创建 [[ShuffleWriteMetricsReporter]]。
   * 由于报告器是逐行操作，需仔细考虑性能。
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * 特定分区的写入流程，控制从 [[ShuffleManager]] 获取的 [[ShuffleWriter]] 的生命周期，
   * 最终返回此任务的 [[MapStatus]]。
   *
   * @param inputs 输入数据迭代器
   * @param dep Shuffle 依赖
   * @param mapId Map 任务 ID
   * @param mapIndex Map 任务索引
   * @param context 任务上下文
   * @return MapStatus，包含数据块位置信息
   */
  def write(
      inputs: Iterator[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      mapIndex: Int,
      context: TaskContext): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))
      writer.write(inputs.asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      val mapStatus = writer.stop(success = true)
      if (mapStatus.isDefined) {
        // 检查是否有足够的 Shuffle merger 供 ShuffleMapTask 推送
        if (dep.shuffleMergeAllowed && dep.getMergerLocs.isEmpty) {
          val mapOutputTracker = SparkEnv.get.mapOutputTracker
          val mergerLocs =
            mapOutputTracker.getShufflePushMergerLocations(dep.shuffleId)
          if (mergerLocs.nonEmpty) {
            dep.setMergerLocs(mergerLocs)
          }
        }
        // 如果启用了 Push-based Shuffle，启动 Shuffle 推送流程
        // Map 任务只负责将 Shuffle 数据文件转换为多个数据块推送请求，
        // 实际推送委托给另一个线程池 - ShuffleBlockPusher.BLOCK_PUSHER_POOL
        if (!dep.shuffleMergeFinalized) {
          manager.shuffleBlockResolver match {
            case resolver: IndexShuffleBlockResolver =>
              logInfo(log"Shuffle merge enabled with" +
                log" ${MDC(NUM_MERGER_LOCATIONS, dep.getMergerLocs.size)} merger locations" +
                log" for stage ${MDC(STAGE_ID, context.stageId())}" +
                log" with shuffle ID ${MDC(SHUFFLE_ID, dep.shuffleId)}")
              logDebug(s"Starting pushing blocks for the task ${context.taskAttemptId()}")
              val dataFile = resolver.getDataFile(dep.shuffleId, mapId)
              new ShuffleBlockPusher(SparkEnv.get.conf)
                .initiateBlockPush(dataFile, writer.getPartitionLengths(), dep, mapIndex)
            case _ =>
          }
        }
      }
      mapStatus.get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}

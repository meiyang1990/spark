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

package org.apache.spark.mapred

import java.io.IOException

import org.apache.hadoop.mapreduce.{TaskAttemptContext => MapReduceTaskAttemptContext}
import org.apache.hadoop.mapreduce.{OutputCommitter => MapReduceOutputCommitter}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{TASK_ATTEMPT_ID, TOTAL_TIME}
import org.apache.spark.util.Utils

/**
 * Spark集成Hadoop MapReduce的工具类，提供任务输出提交的协调能力
 * 处理多个任务尝试同时提交同一输出分区的竞争问题，保证输出一致性
 */
object SparkHadoopMapRedUtil extends Logging {
  /**
   * 协调并提交Hadoop MapReduce任务输出
   * 
   * 在提交任务输出前，需要和Driver端协调确认当前尝试是否有权限提交，避免多个尝试同时提交同一分区导致数据错误
   * 仅当配置`spark.hadoop.outputCommitCoordination.enabled`为true时启用提交协调（默认开启）
   * 
   * @param committer Hadoop MapReduce输出提交器
   * @param mrTaskContext Hadoop MapReduce任务尝试上下文
   * @param jobId Spark作业ID
   * @param splitId 输入分片ID（对应输出分区ID）
   */
  def commitTask(
      committer: MapReduceOutputCommitter,
      mrTaskContext: MapReduceTaskAttemptContext,
      jobId: Int,
      splitId: Int): Unit = {

    val mrTaskAttemptID = mrTaskContext.getTaskAttemptID

    /**
     * 执行实际的任务输出提交操作，在获得Driver授权后调用
     */
    def performCommit(): Unit = {
      try {
        // 统计提交任务耗时并执行Hadoop原生提交逻辑
        val (_, timeCost) = Utils.timeTakenMs(committer.commitTask(mrTaskContext))
        logInfo(log"${MDC(TASK_ATTEMPT_ID, mrTaskAttemptID)}: Committed." +
          log" Elapsed time: ${MDC(TOTAL_TIME, timeCost)} ms.")
      } catch {
        case cause: IOException =>
          // 提交失败记录日志，终止当前任务并回滚
          logError(
            log"Error committing the output of task: ${MDC(TASK_ATTEMPT_ID, mrTaskAttemptID)}",
            cause)
          committer.abortTask(mrTaskContext)
          throw cause
      }
    }

    // 首先检查是否需要提交当前任务输出
    if (committer.needsTaskCommit(mrTaskContext)) {
      // 决定是否需要与Driver协调提交权限
      val shouldCoordinateWithDriver: Boolean = {
        val sparkConf = SparkEnv.get.conf
        // 仅当存在并发任务尝试时需要协调，即使未开启推测执行也可能出现并发尝试（详见SPARK-8029）
        // 该配置是逃生舱，可关闭协调解决提交逻辑引发的问题
        sparkConf.getBoolean("spark.hadoop.outputCommitCoordination.enabled", defaultValue = true)
      }

      if (shouldCoordinateWithDriver) {
        // 从Spark环境获取输出提交协调器
        val outputCommitCoordinator = SparkEnv.get.outputCommitCoordinator
        val ctx = TaskContext.get()
        // 向Driver申请提交权限，确认当前尝试是否可以提交
        val canCommit = outputCommitCoordinator.canCommit(ctx.stageId(), ctx.stageAttemptNumber(),
          splitId, ctx.attemptNumber())

        if (canCommit) {
          // 获得授权，执行提交
          performCommit()
        } else {
          // 未获得授权，终止任务并回滚，让Driver重新调度
          val message = log"${MDC(TASK_ATTEMPT_ID, mrTaskAttemptID)}: Not committed because" +
            log" the driver did not authorize commit"
          logInfo(message)
          committer.abortTask(mrTaskContext)
          throw new CommitDeniedException(message.message, ctx.stageId(), splitId,
            ctx.attemptNumber())
        }
      } else {
        // 协调已关闭，直接执行提交
        performCommit()
      }
    } else {
      // 已有其他尝试完成提交，当前尝试无需操作直接成功
      logInfo(log"No need to commit output of task because needsTaskCommit=false:" +
        log" ${MDC(TASK_ATTEMPT_ID, mrTaskAttemptID)}")
    }
  }
}
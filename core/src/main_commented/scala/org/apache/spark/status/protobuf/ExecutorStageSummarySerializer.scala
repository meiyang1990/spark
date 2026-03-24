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

package org.apache.spark.status.protobuf

import org.apache.spark.status.api.v1.ExecutorStageSummary
import org.apache.spark.status.protobuf.Utils.getOptional

/**
 * Protobuf序列化工具，负责Executor阶段运行摘要的序列化与反序列化
 * 用于Spark状态存储的持久化与网络传输
 */
private[protobuf] object ExecutorStageSummarySerializer {

  /**
   * 将内存中ExecutorStageSummary对象序列化为Protobuf格式
   * @param input 待序列化的Executor阶段运行摘要对象
   * @return 序列化后的Protobuf格式对象
   */
  def serialize(input: ExecutorStageSummary): StoreTypes.ExecutorStageSummary = {
    val builder = StoreTypes.ExecutorStageSummary.newBuilder()
      .setTaskTime(input.taskTime)
      .setFailedTasks(input.failedTasks)
      .setSucceededTasks(input.succeededTasks)
      .setKilledTasks(input.killedTasks)
      .setInputBytes(input.inputBytes)
      .setInputRecords(input.inputRecords)
      .setOutputBytes(input.outputBytes)
      .setOutputRecords(input.outputRecords)
      .setShuffleRead(input.shuffleRead)
      .setShuffleReadRecords(input.shuffleReadRecords)
      .setShuffleWrite(input.shuffleWrite)
      .setShuffleWriteRecords(input.shuffleWriteRecords)
      .setMemoryBytesSpilled(input.memoryBytesSpilled)
      .setDiskBytesSpilled(input.diskBytesSpilled)
      .setIsBlacklistedForStage(input.isBlacklistedForStage)
      .setIsExcludedForStage(input.isExcludedForStage)
    // 如果存在峰值内存指标，序列化后添加到Protobuf对象
    input.peakMemoryMetrics.map { m =>
      builder.setPeakMemoryMetrics(ExecutorMetricsSerializer.serialize(m))
    }
    builder.build()
  }

  /**
   * 将Protobuf格式的二进制数据反序列化为内存中ExecutorStageSummary对象
   * @param binary 待反序列化的Protobuf格式对象
   * @return 反序列化后的内存对象
   */
  def deserialize(binary: StoreTypes.ExecutorStageSummary): ExecutorStageSummary = {
    // 处理可选字段：仅当存在峰值内存指标时才进行反序列化
    val peakMemoryMetrics =
      getOptional(binary.hasPeakMemoryMetrics,
        () => ExecutorMetricsSerializer.deserialize(binary.getPeakMemoryMetrics))
    new ExecutorStageSummary(
      taskTime = binary.getTaskTime,
      failedTasks = binary.getFailedTasks,
      succeededTasks = binary.getSucceededTasks,
      killedTasks = binary.getKilledTasks,
      inputBytes = binary.getInputBytes,
      inputRecords = binary.getInputRecords,
      outputBytes = binary.getOutputBytes,
      outputRecords = binary.getOutputRecords,
      shuffleRead = binary.getShuffleRead,
      shuffleReadRecords = binary.getShuffleReadRecords,
      shuffleWrite = binary.getShuffleWrite,
      shuffleWriteRecords = binary.getShuffleWriteRecords,
      memoryBytesSpilled = binary.getMemoryBytesSpilled,
      diskBytesSpilled = binary.getDiskBytesSpilled,
      isBlacklistedForStage = binary.getIsBlacklistedForStage,
      peakMemoryMetrics = peakMemoryMetrics,
      isExcludedForStage = binary.getIsExcludedForStage)
  }
}
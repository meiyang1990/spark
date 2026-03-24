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

import java.util.Date

import scala.jdk.CollectionConverters._

import org.apache.spark.status.StageDataWrapper
import org.apache.spark.status.api.v1.{ExecutorMetricsDistributions, ExecutorPeakMetricsDistributions, InputMetricDistributions, InputMetrics, OutputMetricDistributions, OutputMetrics, ShufflePushReadMetricDistributions, ShufflePushReadMetrics, ShuffleReadMetricDistributions, ShuffleReadMetrics, ShuffleWriteMetricDistributions, ShuffleWriteMetrics, SpeculationStageSummary, StageData, TaskData, TaskMetricDistributions, TaskMetrics}
import org.apache.spark.status.protobuf.Utils._
import org.apache.spark.util.Utils.weakIntern
import org.apache.spark.util.collection.Utils.isNotEmpty

/**
 * StageDataWrapper 的 Protobuf 序列化与反序列化器
 * 负责将Spark状态存储中的Stage数据转换为Protobuf二进制格式，以及从二进制还原回对象，用于状态持久化存储
 */
private[protobuf] class StageDataWrapperSerializer extends ProtobufSerDe[StageDataWrapper] {

  /**
   * 将StageDataWrapper对象序列化为Protobuf字节数组
   * @param input 待序列化的Stage包装对象
   * @return 序列化后的二进制字节数组
   */
  override def serialize(input: StageDataWrapper): Array[Byte] = {
    val builder = StoreTypes.StageDataWrapper.newBuilder()
    // 序列化核心Stage数据
    builder.setInfo(serializeStageData(input.info))
    // 添加关联的Job ID列表
    input.jobIds.foreach(id => builder.addJobIds(id.toLong))
    // 添加数据本地性统计
    input.locality.foreach { entry =>
      builder.putLocality(entry._1, entry._2)
    }
    builder.build().toByteArray
  }

  /**
   * 将StageData对象序列化为Protobuf对象
   * @param stageData 待序列化的Stage数据对象
   * @return 序列化后的Protobuf StageData对象
   */
  private def serializeStageData(stageData: StageData): StoreTypes.StageData = {
    val stageDataBuilder = StoreTypes.StageData.newBuilder()
    // 填充基本统计和指标字段
    stageDataBuilder
      .setStatus(StageStatusSerializer.serialize(stageData.status))
      .setStageId(stageData.stageId.toLong)
      .setAttemptId(stageData.attemptId)
      .setNumTasks(stageData.numTasks)
      .setNumActiveTasks(stageData.numActiveTasks)
      .setNumCompleteTasks(stageData.numCompleteTasks)
      .setNumFailedTasks(stageData.numFailedTasks)
      .setNumKilledTasks(stageData.numKilledTasks)
      .setNumCompletedIndices(stageData.numCompletedIndices)
      .setExecutorDeserializeTime(stageData.executorDeserializeTime)
      .setExecutorDeserializeCpuTime(stageData.executorDeserializeCpuTime)
      .setExecutorRunTime(stageData.executorRunTime)
      .setExecutorCpuTime(stageData.executorCpuTime)
      .setResultSize(stageData.resultSize)
      .setJvmGcTime(stageData.jvmGcTime)
      .setResultSerializationTime(stageData.resultSerializationTime)
      .setMemoryBytesSpilled(stageData.memoryBytesSpilled)
      .setDiskBytesSpilled(stageData.diskBytesSpilled)
      .setPeakExecutionMemory(stageData.peakExecutionMemory)
      .setInputBytes(stageData.inputBytes)
      .setInputRecords(stageData.inputRecords)
      .setOutputBytes(stageData.outputBytes)
      .setOutputRecords(stageData.outputRecords)
      .setShuffleRemoteBlocksFetched(stageData.shuffleRemoteBlocksFetched)
      .setShuffleLocalBlocksFetched(stageData.shuffleLocalBlocksFetched)
      .setShuffleFetchWaitTime(stageData.shuffleFetchWaitTime)
      .setShuffleRemoteBytesRead(stageData.shuffleRemoteBytesRead)
      .setShuffleRemoteBytesReadToDisk(stageData.shuffleRemoteBytesReadToDisk)
      .setShuffleLocalBytesRead(stageData.shuffleLocalBytesRead)
      .setShuffleReadBytes(stageData.shuffleReadBytes)
      .setShuffleReadRecords(stageData.shuffleReadRecords)
      .setShuffleCorruptMergedBlockChunks(stageData.shuffleCorruptMergedBlockChunks)
      .setShuffleMergedFetchFallbackCount(stageData.shuffleMergedFetchFallbackCount)
      .setShuffleMergedRemoteBlocksFetched(stageData.shuffleMergedRemoteBlocksFetched)
      .setShuffleMergedLocalBlocksFetched(stageData.shuffleMergedLocalBlocksFetched)
      .setShuffleMergedRemoteChunksFetched(stageData.shuffleMergedRemoteChunksFetched)
      .setShuffleMergedLocalChunksFetched(stageData.shuffleMergedLocalChunksFetched)
      .setShuffleMergedRemoteBytesRead(stageData.shuffleMergedRemoteBytesRead)
      .setShuffleMergedLocalBytesRead(stageData.shuffleMergedLocalBytesRead)
      .setShuffleRemoteReqsDuration(stageData.shuffleRemoteReqsDuration)
      .setShuffleMergedRemoteReqsDuration(stageData.shuffleMergedRemoteReqsDuration)
      .setShuffleWriteBytes(stageData.shuffleWriteBytes)
      .setShuffleWriteTime(stageData.shuffleWriteTime)
      .setShuffleWriteRecords(stageData.shuffleWriteRecords)
      .setResourceProfileId(stageData.resourceProfileId)
      .setIsShufflePushEnabled(stageData.isShufflePushEnabled)
      .setShuffleMergersCount(stageData.shuffleMergersCount)
    // 设置可选字符串字段
    setStringField(stageData.name, stageDataBuilder.setName)
    setStringField(stageData.details, stageDataBuilder.setDetails)
    setStringField(stageData.schedulingPool, stageDataBuilder.setSchedulingPool)
    // 设置可选时间字段
    stageData.submissionTime.foreach { d =>
      stageDataBuilder.setSubmissionTime(d.getTime)
    }
    stageData.firstTaskLaunchedTime.foreach { d =>
      stageDataBuilder.setFirstTaskLaunchedTime(d.getTime)
    }
    stageData.completionTime.foreach { d =>
      stageDataBuilder.setCompletionTime(d.getTime)
    }
    stageData.failureReason.foreach { fr =>
      stageDataBuilder.setFailureReason(fr)
    }
    stageData.description.foreach { d =>
      stageDataBuilder.setDescription(d)
    }
    // 添加关联RDD ID列表
    stageData.rddIds.foreach(id => stageDataBuilder.addRddIds(id.toLong))
    // 序列化累加器更新
    stageData.accumulatorUpdates.foreach { update =>
      stageDataBuilder.addAccumulatorUpdates(
        AccumulableInfoSerializer.serialize(update))
    }
    // 序列化所有Task数据
    stageData.tasks.foreach { t =>
      t.foreach { entry =>
        stageDataBuilder.putTasks(entry._1, serializeTaskData(entry._2))
      }
    }
    // 序列化Executor执行概要
    stageData.executorSummary.foreach { es =>
      es.foreach { entry =>
        stageDataBuilder.putExecutorSummary(entry._1,
          ExecutorStageSummarySerializer.serialize(entry._2))
      }
    }
    // 序列化推测执行概要
    stageData.speculationSummary.foreach { ss =>
      stageDataBuilder.setSpeculationSummary(serializeSpeculationStageSummary(ss))
    }
    // 添加被杀死任务统计
    stageData.killedTasksSummary.foreach { entry =>
      stageDataBuilder.putKilledTasksSummary(entry._1, entry._2)
    }
    // 序列化峰值Executor指标
    stageData.peakExecutorMetrics.foreach { pem =>
      stageDataBuilder.setPeakExecutorMetrics(ExecutorMetricsSerializer.serialize(pem))
    }
    // 序列化任务指标分布统计
    stageData.taskMetricsDistributions.foreach { tmd =>
      stageDataBuilder.setTaskMetricsDistributions(serializeTaskMetricDistributions(tmd))
    }
    // 序列化Executor指标分布统计
    stageData.executorMetricsDistributions.foreach { emd =>
      stageDataBuilder.setExecutorMetricsDistributions(serializeExecutorMetricsDistributions(emd))
    }
    stageDataBuilder.build()
  }

  /**
   * 将TaskData对象序列化为Protobuf对象
   * @param t 待序列化的任务数据对象
   * @return 序列化后的Protobuf TaskData对象
   */
  private def serializeTaskData(t: TaskData): StoreTypes.TaskData = {
    val taskDataBuilder = StoreTypes.TaskData.newBuilder()
    // 填充基本任务字段
    taskDataBuilder
      .setTaskId(t.taskId)
      .setIndex(t.index)
      .setAttempt(t.attempt)
      .setPartitionId(t.partitionId)
      .setLaunchTime(t.launchTime.getTime)
      .setSpeculative(t.speculative)
      .setSchedulerDelay(t.schedulerDelay)
      .setGettingResultTime(t.gettingResultTime)
    // 设置可选字符串字段
    setStringField(t.executorId, taskDataBuilder.setExecutorId)
    setStringField(t.host, taskDataBuilder.setHost)
    setStringField(t.status, taskDataBuilder.setStatus)
    setStringField(t.taskLocality, taskDataBuilder.setTaskLocality)
    // 设置可选时间与时长字段
    t.resultFetchStart.foreach { rfs =>
      taskDataBuilder.setResultFetchStart(rfs.getTime)
    }
    t.duration.foreach { d =>
      taskDataBuilder.setDuration(d)
    }
    // 序列化累加器更新
    t.accumulatorUpdates.foreach { update =>
      taskDataBuilder.addAccumulatorUpdates(
        AccumulableInfoSerializer.serialize(update))
    }
    // 设置错误信息
    t.errorMessage.foreach { em =>
      taskDataBuilder.setErrorMessage(em)
    }
    // 序列化任务指标
    t.taskMetrics.foreach { tm =>
      taskDataBuilder.setTaskMetrics(serializeTaskMetrics(tm))
    }
    // 添加Executor日志链接
    t.executorLogs.foreach { entry =>
      taskDataBuilder.putExecutorLogs(entry._1, entry._2)
    }
    taskDataBuilder.build()
  }

  /**
   * 将TaskMetrics对象序列化为Protobuf对象
   * @param tm 待序列化的任务指标对象
   * @return 序列化后的Protobuf TaskMetrics对象
   */
  private def serializeTaskMetrics(tm: TaskMetrics): StoreTypes.TaskMetrics = {
    val taskMetricsBuilder = StoreTypes.TaskMetrics.newBuilder()
    taskMetricsBuilder
      .setExecutorDeserializeTime(tm.executorDeserializeTime)
      .setExecutorDeserializeCpuTime(tm.executorDeserializeCpuTime)
      .setExecutorRunTime(tm.executorRunTime)
      .setExecutorCpuTime(tm.executorCpuTime)
      .setResultSize(tm.resultSize)
      .setJvmGcTime(tm.jvmGcTime)
      .setResultSerializationTime(tm.resultSerializationTime)
      .setMemoryBytesSpilled(tm.memoryBytesSpilled)
      .setDiskBytesSpilled(tm.diskBytesSpilled)
      .setPeakExecutionMemory(tm.peakExecutionMemory)
      .setInputMetrics(serializeInputMetrics(tm.inputMetrics))
      .setOutputMetrics(serializeOutputMetrics(tm.outputMetrics))
      .setShuffleReadMetrics(serializeShuffleReadMetrics(tm.shuffleReadMetrics))
      .setShuffleWriteMetrics(serializeShuffleWriteMetrics(tm.shuffleWriteMetrics))
    taskMetricsBuilder.build()
  }

  /**
   * 将InputMetrics对象序列化为Protobuf对象
   * @param im 待序列化的输入指标对象
   * @return 序列化后的Protobuf InputMetrics对象
   */
  private def serializeInputMetrics(im: InputMetrics): StoreTypes.InputMetrics = {
    StoreTypes.InputMetrics.newBuilder()
      .setBytesRead(im.bytesRead)
      .setRecordsRead(im.recordsRead)
      .build()
  }

  /**
   * 将OutputMetrics对象序列化为Protobuf对象
   * @param om 待序列化的输出指标对象
   * @return 序列化后的Protobuf OutputMetrics对象
   */
  private def serializeOutputMetrics(om: OutputMetrics): StoreTypes.OutputMetrics = {
    StoreTypes.OutputMetrics.newBuilder()
      .setBytesWritten(om.bytesWritten)
      .setRecordsWritten(om.recordsWritten)
      .build()
  }

  /**
   * 将ShuffleReadMetrics对象序列化为Protobuf对象
   * @param srm 待序列化的Shuffle读指标对象
   * @return 序列化后的Protobuf ShuffleReadMetrics对象
   */
  private def serializeShuffleReadMetrics(
      srm: ShuffleReadMetrics): StoreTypes.ShuffleReadMetrics = {
    StoreTypes.ShuffleReadMetrics.newBuilder()
      .setRemoteBlocksFetched(srm.remoteBlocksFetched)
      .setLocalBlocksFetched(srm.localBlocksFetched)
      .setFetchWaitTime(srm.fetchWaitTime)
      .setRemoteBytesRead(srm.remoteBytesRead)
      .setRemoteBytesReadToDisk(srm.remoteBytesReadToDisk)
      .setLocalBytesRead(srm.localBytesRead)
      .setRecordsRead(srm.recordsRead)
      .setRemoteReqsDuration(srm.remoteReqsDuration)
      .setShufflePushReadMetrics(serializeShufflePushReadMetrics(srm.shufflePushReadMetrics))
      .build()
  }

  /**
   * 将ShufflePushReadMetrics对象序列化为Protobuf对象
   * @param sprm 待序列化的Shuffle推送读指标对象
   * @return 序列化后的Protobuf ShufflePushReadMetrics对象
   */
  private def serializeShufflePushReadMetrics(
      sprm: ShufflePushReadMetrics): StoreTypes.ShufflePushReadMetrics = {
    StoreTypes.ShufflePushReadMetrics.newBuilder()
      .setCorruptMergedBlockChunks(sprm.corruptMergedBlockChunks)
      .setMergedFetchFallbackCount(sprm.mergedFetchFallbackCount)
      .setRemoteMergedBlocksFetched(sprm.remoteMergedBlocksFetched)
      .setLocalMergedBlocksFetched(sprm.localMergedBlocksFetched)
      .setRemoteMergedChunksFetched(sprm.remoteMergedChunksFetched)
      .setLocalMergedChunksFetched(sprm.localMergedChunksFetched)
      .setRemoteMergedBytesRead(sprm.remoteMergedBytesRead)
      .setLocalMergedBytesRead(sprm.localMergedBytesRead)
      .setRemoteMergedReqsDuration(sprm.remoteMergedReqsDuration)
      .build()
  }

  /**
   * 将ShuffleWriteMetrics对象序列化为Protobuf对象
   * @param swm 待序列化的Shuffle写指标对象
   * @return 序列化后的Protobuf ShuffleWriteMetrics对象
   */
  private def serializeShuffleWriteMetrics(
      swm: ShuffleWriteMetrics): StoreTypes.ShuffleWriteMetrics = {
    StoreTypes.ShuffleWriteMetrics.newBuilder()
      .setBytesWritten(swm.bytesWritten)
      .setWriteTime(swm.writeTime)
      .setRecordsWritten(swm.recordsWritten)
      .build()
  }

  /**
   * 将SpeculationStageSummary对象序列化为Protobuf对象
   * @param sss 待序列化的推测执行阶段概要对象
   * @return 序列化后的Protobuf SpeculationStageSummary对象
   */
  private def serializeSpeculationStageSummary(
      sss: SpeculationStageSummary): StoreTypes.SpeculationStageSummary = {
    StoreTypes.SpeculationStageSummary.newBuilder()
      .setNumTasks(sss.numTasks)
      .setNumActiveTasks(sss.numActiveTasks)
      .setNumCompletedTasks(sss.numCompletedTasks)
      .setNumFailedTasks(sss.numFailedTasks)
      .setNumKilledTasks(sss.numKilledTasks)
      .build()
  }

  /**
   * 将TaskMetricDistributions对象序列化为Protobuf对象
   * @param tmd 待序列化的任务指标分布统计对象
   * @return 序列化后的Protobuf TaskMetricDistributions对象
   */
  private def serializeTaskMetricDistributions(
      tmd: TaskMetricDistributions): StoreTypes.TaskMetricDistributions = {
    val builder = StoreTypes.TaskMetricDistributions.newBuilder()
    // 添加分位数和各指标分布数据
    tmd.quantiles.foreach(q => builder.addQuantiles(q))
    tmd.duration.
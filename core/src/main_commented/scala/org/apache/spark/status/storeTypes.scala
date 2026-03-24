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

import java.lang.{Long => JLong}
import java.util.Date

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.scope._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVIndex

/**
 * 文件级说明：定义KV存储中存储应用状态数据的实体类型包装类，为UI展示提供结构化索引数据
 */

/**
 * 应用状态存储元数据，保存存储版本号用于兼容性处理
 */
private[spark] case class AppStatusStoreMetadata(version: Long)

/**
 * 应用信息包装类，适配KV存储要求提供索引
 * @param info 原始应用信息对象
 */
private[spark] class ApplicationInfoWrapper(val info: ApplicationInfo) {

  @JsonIgnore @KVIndex
  def id: String = info.id

}

/**
 * 应用运行环境信息包装类，适配KV存储要求提供索引
 * @param info 原始应用环境信息对象
 */
private[spark] class ApplicationEnvironmentInfoWrapper(val info: ApplicationEnvironmentInfo) {

  /**
   * There's always a single ApplicationEnvironmentInfo object per application, so this
   * ID doesn't need to be dynamic. But the KVStore API requires an ID.
   */
  @JsonIgnore @KVIndex
  def id: String = classOf[ApplicationEnvironmentInfoWrapper].getName()

}

/**
 * 执行器概要信息包装类，适配KV存储并添加索引
 * @param info 原始执行器概要信息对象
 */
private[spark] class ExecutorSummaryWrapper(val info: ExecutorSummary) {

  @JsonIgnore @KVIndex
  private def id: String = info.id

  @JsonIgnore @KVIndex("active")
  private def active: Boolean = info.isActive

  @JsonIgnore @KVIndex("host")
  val host: String = Utils.parseHostPort(info.hostPort)._1

}

/**
 * Keep track of the existing stages when the job was submitted, and those that were
 * completed during the job's execution. This allows a more accurate accounting of how
 * many tasks were skipped for the job.
 */
private[spark] class JobDataWrapper(
    val info: JobData,
    val skippedStages: Set[Int],
    val sqlExecutionId: Option[Long]) {

  @JsonIgnore @KVIndex
  private def id: Int = info.jobId

  @JsonIgnore @KVIndex("completionTime")
  private def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)
}

/**
 * 阶段数据包装类，适配KV存储并添加索引
 * @param info 原始阶段数据对象
 * @param jobIds 关联的Job ID集合
 * @param locality 数据本地性分布统计
 */
private[spark] class StageDataWrapper(
    val info: StageData,
    val jobIds: Set[Int],
    @JsonDeserialize(contentAs = classOf[JLong])
    val locality: Map[String, Long]) {

  @JsonIgnore @KVIndex
  private[this] val id: Array[Int] = Array(info.stageId, info.attemptId)

  @JsonIgnore @KVIndex("stageId")
  private def stageId: Int = info.stageId

  @JsonIgnore @KVIndex("active")
  private def active: Boolean = info.status == StageStatus.ACTIVE

  @JsonIgnore @KVIndex("completionTime")
  def completionTime: Long = info.completionTime.map(_.getTime).getOrElse(-1L)
}

/**
 * Tasks have a lot of indices that are used in a few different places. This object keeps logical
 * names for these indices, mapped to short strings to save space when using a disk store.
 */
private[spark] object TaskIndexNames {
  final val ACCUMULATORS = "acc"
  final val ATTEMPT = "att"
  final val DESER_CPU_TIME = "dct"
  final val DESER_TIME = "des"
  final val DISK_SPILL = "dbs"
  final val DURATION = "dur"
  final val ERROR = "err"
  final val EXECUTOR = "exe"
  final val HOST = "hst"
  final val EXEC_CPU_TIME = "ect"
  final val EXEC_RUN_TIME = "ert"
  final val GC_TIME = "gc"
  final val GETTING_RESULT_TIME = "grt"
  final val INPUT_RECORDS = "ir"
  final val INPUT_SIZE = "is"
  final val LAUNCH_TIME = "lt"
  final val LOCALITY = "loc"
  final val MEM_SPILL = "mbs"
  final val OUTPUT_RECORDS = "or"
  final val OUTPUT_SIZE = "os"
  final val PEAK_MEM = "pem"
  final val RESULT_SIZE = "rs"
  final val SCHEDULER_DELAY = "dly"
  final val SER_TIME = "rst"
  final val SHUFFLE_LOCAL_BLOCKS = "slbl"
  final val SHUFFLE_READ_RECORDS = "srr"
  final val SHUFFLE_READ_FETCH_WAIT_TIME = "srt"
  final val SHUFFLE_REMOTE_BLOCKS = "srbl"
  final val SHUFFLE_REMOTE_READS = "srby"
  final val SHUFFLE_REMOTE_READS_TO_DISK = "srbd"
  final val SHUFFLE_TOTAL_READS = "stby"
  final val SHUFFLE_TOTAL_BLOCKS = "stbl"
  final val SHUFFLE_WRITE_RECORDS = "swr"
  final val SHUFFLE_WRITE_SIZE = "sws"
  final val SHUFFLE_WRITE_TIME = "swt"
  final val SHUFFLE_REMOTE_REQS_DURATION = "srrd"
  final val SHUFFLE_PUSH_CORRUPT_MERGED_BLOCK_CHUNKS = "spcmbc"
  final val SHUFFLE_PUSH_MERGED_FETCH_FALLBACK_COUNT = "spmffc"
  final val SHUFFLE_PUSH_MERGED_REMOTE_BLOCKS = "spmrb"
  final val SHUFFLE_PUSH_MERGED_LOCAL_BLOCKS = "spmlb"
  final val SHUFFLE_PUSH_MERGED_REMOTE_CHUNKS = "spmrc"
  final val SHUFFLE_PUSH_MERGED_LOCAL_CHUNKS = "spmlc"
  final val SHUFFLE_PUSH_MERGED_REMOTE_READS = "spmrr"
  final val SHUFFLE_PUSH_MERGED_LOCAL_READS = "spmlr"
  final val SHUFFLE_PUSH_MERGED_REMOTE_REQS_DURATION = "spmrrd"
  final val STAGE = "stage"
  final val STATUS = "sta"
  final val TASK_INDEX = "idx"
  final val TASK_PARTITION_ID = "partid"
  final val COMPLETION_TIME = "ct"
}

/**
 * Unlike other data types, the task data wrapper does not keep a reference to the API's TaskData.
 * That is to save memory, since for large applications there can be a large number of these
 * elements (by default up to 100,000 per stage), and every bit of wasted memory adds up.
 *
 * It also contains many secondary indices, which are used to sort data efficiently in the UI at the
 * expense of storage space (and slower write times).
 */
private[spark] class TaskDataWrapper(
    // Storing this as an object actually saves memory; it's also used as the key in the in-memory
    // store, so in that case you'd save the extra copy of the value here.
    @KVIndexParam(parent = TaskIndexNames.STAGE)
    val taskId: JLong,
    @KVIndexParam(value = TaskIndexNames.TASK_INDEX, parent = TaskIndexNames.STAGE)
    val index: Int,
    @KVIndexParam(value = TaskIndexNames.ATTEMPT, parent = TaskIndexNames.STAGE)
    val attempt: Int,
    @KVIndexParam(value = TaskIndexNames.TASK_PARTITION_ID, parent = TaskIndexNames.STAGE)
    // Different kvstores have different default values (eg 0 or -1),
    // thus we use the default value here for backwards compatibility.
    val partitionId: Int = -1,
    @KVIndexParam(value = TaskIndexNames.LAUNCH_TIME, parent = TaskIndexNames.STAGE)
    val launchTime: Long,
    val resultFetchStart: Long,
    @KVIndexParam(value = TaskIndexNames.DURATION, parent = TaskIndexNames.STAGE)
    val duration: Long,
    @KVIndexParam(value = TaskIndexNames.EXECUTOR, parent = TaskIndexNames.STAGE)
    val executorId: String,
    @KVIndexParam(value = TaskIndexNames.HOST, parent = TaskIndexNames.STAGE)
    val host: String,
    @KVIndexParam(value = TaskIndexNames.STATUS, parent = TaskIndexNames.STAGE)
    val status: String,
    @KVIndexParam(value = TaskIndexNames.LOCALITY, parent = TaskIndexNames.STAGE)
    val taskLocality: String,
    val speculative: Boolean,
    val accumulatorUpdates: collection.Seq[AccumulableInfo],
    val errorMessage: Option[String],

    val hasMetrics: Boolean,
    // The following is an exploded view of a TaskMetrics API object. This saves 5 objects
    // (= 80 bytes of Java object overhead) per instance of this wrapper. Non successful
    // tasks' metrics will have negative values in `TaskDataWrapper`. `TaskData` will have
    // actual metric values. To recover the actual metric value from `TaskDataWrapper`,
    // need use `getMetricValue` method. If `hasMetrics` is false, it means the metrics
    // for this task have not been recorded.
    @KVIndexParam(value = TaskIndexNames.DESER_TIME, parent = TaskIndexNames.STAGE)
    val executorDeserializeTime: Long,
    @KVIndexParam(value = TaskIndexNames.DESER_CPU_TIME, parent = TaskIndexNames.STAGE)
    val executorDeserializeCpuTime: Long,
    @KVIndexParam(value = TaskIndexNames.EXEC_RUN_TIME, parent = TaskIndexNames.STAGE)
    val executorRunTime: Long,
    @KVIndexParam(value = TaskIndexNames.EXEC_CPU_TIME, parent = TaskIndexNames.STAGE)
    val executorCpuTime: Long,
    @KVIndexParam(value = TaskIndexNames.RESULT_SIZE, parent = TaskIndexNames.STAGE)
    val resultSize: Long,
    @KVIndexParam(value = TaskIndexNames.GC_TIME, parent = TaskIndexNames.STAGE)
    val jvmGcTime: Long,
    @KVIndexParam(value = TaskIndexNames.SER_TIME, parent = TaskIndexNames.STAGE)
    val resultSerializationTime: Long,
    @KVIndexParam(value = TaskIndexNames.MEM_SPILL, parent = TaskIndexNames.STAGE)
    val memoryBytesSpilled: Long,
    @KVIndexParam(value = TaskIndexNames.DISK_SPILL, parent = TaskIndexNames.STAGE)
    val diskBytesSpilled: Long,
    @KVIndexParam(value = TaskIndexNames.PEAK_MEM, parent = TaskIndexNames.STAGE)
    val peakExecutionMemory: Long,
    @KVIndexParam(value = TaskIndexNames.INPUT_SIZE, parent = TaskIndexNames.STAGE)
    val inputBytesRead: Long,
    @KVIndexParam(value = TaskIndexNames.INPUT_RECORDS, parent = TaskIndexNames.STAGE)
    val inputRecordsRead: Long,
    @KVIndexParam(value = TaskIndexNames.OUTPUT_SIZE, parent = TaskIndexNames.STAGE)
    val outputBytesWritten: Long,
    @KVIndexParam(value = TaskIndexNames.OUTPUT_RECORDS, parent = TaskIndexNames.STAGE)
    val outputRecordsWritten: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleRemoteBlocksFetched: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_LOCAL_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleLocalBlocksFetched: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_FETCH_WAIT_TIME,
      parent = TaskIndexNames.STAGE)
    val shuffleFetchWaitTime: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_READS, parent = TaskIndexNames.STAGE)
    val shuffleRemoteBytesRead: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_REMOTE_READS_TO_DISK,
      parent = TaskIndexNames.STAGE)
    val shuffleRemoteBytesReadToDisk: Long,
    val shuffleLocalBytesRead: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_READ_RECORDS, parent = TaskIndexNames.STAGE)
    val shuffleRecordsRead: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_CORRUPT_MERGED_BLOCK_CHUNKS,
      parent = TaskIndexNames.STAGE)
    val shuffleCorruptMergedBlockChunks: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_PUSH_MERGED_FETCH_FALLBACK_COUNT,
      parent = TaskIndexNames.STAGE)
    val shuffleMergedFetchFallbackCount: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleMergedRemoteBlocksFetched: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_LOCAL_BLOCKS, parent = TaskIndexNames.STAGE)
    val shuffleMergedLocalBlocksFetched: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_CHUNKS, parent = TaskIndexNames.STAGE)
    val shuffleMergedRemoteChunksFetched: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_LOCAL_CHUNKS, parent = TaskIndexNames.STAGE)
    val shuffleMergedLocalChunksFetched: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_READS, parent = TaskIndexNames.STAGE)
    val shuffleMergedRemoteBytesRead: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_LOCAL_READS, parent = TaskIndexNames.STAGE)
    val shuffleMergedLocalBytesRead: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_REMOTE_REQS_DURATION, parent = TaskIndexNames.STAGE)
    val shuffleRemoteReqsDuration: Long,
    @KVIndexParam(
      value = TaskIndexNames.SHUFFLE_PUSH_MERGED_REMOTE_REQS_DURATION,
      parent = TaskIndexNames.STAGE)
    val shuffleMergedRemoteReqDuration: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_SIZE, parent = TaskIndexNames.STAGE)
    val shuffleBytesWritten: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_TIME, parent = TaskIndexNames.STAGE)
    val shuffleWriteTime: Long,
    @KVIndexParam(value = TaskIndexNames.SHUFFLE_WRITE_RECORDS, parent = TaskIndexNames.STAGE)
    val shuffleRecordsWritten: Long,

    val stageId: Int,
    val stageAttemptId: Int) {

  // SPARK-26260: To handle non successful tasks metrics (Running, Failed, Killed).
  /**
   * 从存储值还原出正确的指标数值，处理未成功任务的编码规则
   * @param metric 存储的编码后指标值
   * @return 还原后的实际指标值
   */
  private def getMetricValue(metric: Long): Long = {
    if (status != "SUCCESS") {
      math.abs(metric + 1)
    } else {
      metric
    }
  }

  /**
   * 转换为API兼容的TaskData对象供UI使用
   * @return 标准TaskData对象
   */
  def toApi: TaskData = {
    val metrics = if (hasMetrics) {
      Some(new TaskMetrics(
        getMetricValue(executorDeserializeTime),
        getMetricValue(executorDeserializeCpuTime),
        getMetricValue
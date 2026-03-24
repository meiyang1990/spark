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
 * Spark REST API v1 版本数据模型定义文件
 * 定义了状态API返回的所有实体数据结构，用于UI和外部系统查询应用状态信息
 */
package org.apache.spark.status.api.v1

import java.lang.{Long => JLong}
import java.util.Date

import scala.xml.{NodeSeq, Text}

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, TaskResourceRequest}
import org.apache.spark.status.AppStatusUtils.getQuantilesValue

/**
 * Spark应用基本信息数据结构
 * @param id 应用ID
 * @param name 应用名称
 * @param coresGranted 已分配的核心数
 * @param maxCores 最大核心数
 * @param coresPerExecutor 每个Executor的核心数
 * @param memoryPerExecutorMB 每个Executor的内存大小(MB)
 * @param attempts 应用的多次尝试信息列表
 */
case class ApplicationInfo private[spark](
    id: String,
    name: String,
    coresGranted: Option[Int],
    maxCores: Option[Int],
    coresPerExecutor: Option[Int],
    memoryPerExecutorMB: Option[Int],
    attempts: collection.Seq[ApplicationAttemptInfo])

/**
 * 应用单次尝试运行信息数据结构
 * @param attemptId 尝试ID
 * @param startTime 开始时间
 * @param endTime 结束时间
 * @param lastUpdated 最后更新时间
 * @param duration 运行时长
 * @param sparkUser 提交用户
 * @param completed 是否完成
 * @param appSparkVersion 应用使用的Spark版本
 * @param logSourceName 日志源名称
 * @param logSourceFullPath 日志源完整路径
 */
@JsonIgnoreProperties(
  value = Array("startTimeEpoch", "endTimeEpoch", "lastUpdatedEpoch"),
  allowGetters = true)
case class ApplicationAttemptInfo private[spark](
    attemptId: Option[String],
    startTime: Date,
    endTime: Date,
    lastUpdated: Date,
    duration: Long,
    sparkUser: String,
    completed: Boolean = false,
    appSparkVersion: String,
    logSourceName: Option[String] = None,
    logSourceFullPath: Option[String] = None) {

  def getStartTimeEpoch: Long = startTime.getTime

  def getEndTimeEpoch: Long = endTime.getTime

  def getLastUpdatedEpoch: Long = lastUpdated.getTime

}

/**
 * 资源配置信息数据结构
 * @param id 资源配置ID
 * @param executorResources Executor资源请求配置
 * @param taskResources 任务资源请求配置
 */
class ResourceProfileInfo private[spark](
    val id: Int,
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest])

/**
 * Stage在单个Executor上的汇总统计信息
 * @param taskTime 任务总运行时间
 * @param failedTasks 失败任务数
 * @param succeededTasks 成功任务数
 * @param killedTasks 被杀死任务数
 * @param inputBytes 输入字节数
 * @param inputRecords 输入记录数
 * @param outputBytes 输出字节数
 * @param outputRecords 输出记录数
 * @param shuffleRead Shuffle读字节数
 * @param shuffleReadRecords Shuffle读记录数
 * @param shuffleWrite Shuffle写字节数
 * @param shuffleWriteRecords Shuffle写记录数
 * @param memoryBytesSpilled 内存溢出到磁盘字节数
 * @param diskBytesSpilled 磁盘溢出字节数
 * @param isBlacklistedForStage 是否在该Stage被黑名单(已废弃)
 * @param peakMemoryMetrics 峰值内存指标
 * @param isExcludedForStage 是否在该Stage被排除
 */
class ExecutorStageSummary private[spark](
    val taskTime : Long,
    val failedTasks : Int,
    val succeededTasks : Int,
    val killedTasks : Int,
    val inputBytes : Long,
    val inputRecords : Long,
    val outputBytes : Long,
    val outputRecords : Long,
    val shuffleRead : Long,
    val shuffleReadRecords : Long,
    val shuffleWrite : Long,
    val shuffleWriteRecords : Long,
    val memoryBytesSpilled : Long,
    val diskBytesSpilled : Long,
    @deprecated("use isExcludedForStage instead", "3.1.0")
    val isBlacklistedForStage: Boolean,
    @JsonSerialize(using = classOf[ExecutorMetricsJsonSerializer])
    @JsonDeserialize(using = classOf[ExecutorMetricsJsonDeserializer])
    val peakMemoryMetrics: Option[ExecutorMetrics],
    val isExcludedForStage: Boolean)

/**
 * Stage推测执行汇总统计信息
 * @param numTasks 总任务数
 * @param numActiveTasks 活跃任务数
 * @param numCompletedTasks 已完成任务数
 * @param numFailedTasks 失败任务数
 * @param numKilledTasks 被杀死任务数
 */
class SpeculationStageSummary private[spark](
   val numTasks: Int,
   val numActiveTasks: Int,
   val numCompletedTasks: Int,
   val numFailedTasks: Int,
   val numKilledTasks: Int)

/**
 * Executor整体汇总信息
 * @param id Executor ID
 * @param hostPort 主机端口
 * @param isActive 是否活跃
 * @param rddBlocks RDD块数
 * @param memoryUsed 已用内存
 * @param diskUsed 已用磁盘
 * @param totalCores 总核心数
 * @param maxTasks 最大任务数
 * @param activeTasks 活跃任务数
 * @param failedTasks 失败任务数
 * @param completedTasks 完成任务数
 * @param totalTasks 总任务数
 * @param totalDuration 总运行时长
 * @param totalGCTime 总GC时间
 * @param totalInputBytes 总输入字节数
 * @param totalShuffleRead 总Shuffle读字节数
 * @param totalShuffleWrite 总Shuffle写字节数
 * @param isBlacklisted 是否被黑名单(已废弃)
 * @param maxMemory 最大内存
 * @param addTime 添加时间
 * @param removeTime 移除时间
 * @param removeReason 移除原因
 * @param executorLogs Executor日志链接
 * @param memoryMetrics 内存使用指标
 * @param blacklistedInStages 被黑名单的Stage集合(已废弃)
 * @param peakMemoryMetrics 峰值内存指标
 * @param attributes Executor自定义属性
 * @param resources 分配的资源信息
 * @param resourceProfileId 资源配置ID
 * @param isExcluded 是否被排除
 * @param excludedInStages 被排除的Stage集合
 */
class ExecutorSummary private[spark](
    val id: String,
    val hostPort: String,
    val isActive: Boolean,
    val rddBlocks: Int,
    val memoryUsed: Long,
    val diskUsed: Long,
    val totalCores: Int,
    val maxTasks: Int,
    val activeTasks: Int,
    val failedTasks: Int,
    val completedTasks: Int,
    val totalTasks: Int,
    val totalDuration: Long,
    val totalGCTime: Long,
    val totalInputBytes: Long,
    val totalShuffleRead: Long,
    val totalShuffleWrite: Long,
    @deprecated("use isExcluded instead", "3.1.0")
    val isBlacklisted: Boolean,
    val maxMemory: Long,
    val addTime: Date,
    val removeTime: Option[Date],
    val removeReason: Option[String],
    val executorLogs: Map[String, String],
    val memoryMetrics: Option[MemoryMetrics],
    @deprecated("use excludedInStages instead", "3.1.0")
    val blacklistedInStages: Set[Int],
    @JsonSerialize(using = classOf[ExecutorMetricsJsonSerializer])
    @JsonDeserialize(using = classOf[ExecutorMetricsJsonDeserializer])
    val peakMemoryMetrics: Option[ExecutorMetrics],
    val attributes: Map[String, String],
    val resources: Map[String, ResourceInformation],
    val resourceProfileId: Int,
    val isExcluded: Boolean,
    val excludedInStages: Set[Int])

/**
 * 内存使用指标数据结构
 * @param usedOnHeapStorageMemory 已用堆内存储内存
 * @param usedOffHeapStorageMemory 已用堆外存储内存
 * @param totalOnHeapStorageMemory 总堆内存储内存
 * @param totalOffHeapStorageMemory 总堆外存储内存
 */
class MemoryMetrics private[spark](
    val usedOnHeapStorageMemory: Long,
    val usedOffHeapStorageMemory: Long,
    val totalOnHeapStorageMemory: Long,
    val totalOffHeapStorageMemory: Long)

/** deserializer for peakMemoryMetrics: convert map to ExecutorMetrics */
/**
 * ExecutorMetrics JSON反序列化器
 * 将JSON中的Map结构转换为ExecutorMetrics对象
 */
private[spark] class ExecutorMetricsJsonDeserializer
    extends JsonDeserializer[Option[ExecutorMetrics]] {
  override def deserialize(
      jsonParser: JsonParser,
      deserializationContext: DeserializationContext): Option[ExecutorMetrics] = {
    // 读取JSON中的指标Map
    val metricsMap = jsonParser.readValueAs[Option[Map[String, Long]]](
      new TypeReference[Option[Map[String, java.lang.Long]]] {})
    // 转换为ExecutorMetrics对象
    metricsMap.map(metrics => new ExecutorMetrics(metrics))
  }

  override def getNullValue(ctxt: DeserializationContext): Option[ExecutorMetrics] = {
    // 空值返回None
    None
  }
}

/** serializer for peakMemoryMetrics: convert ExecutorMetrics to map with metric name as key */
/**
 * ExecutorMetrics JSON序列化器
 * 将ExecutorMetrics对象转换为以指标名为key的Map结构输出
 */
private[spark] class ExecutorMetricsJsonSerializer
    extends JsonSerializer[Option[ExecutorMetrics]] {
  override def serialize(
      metrics: Option[ExecutorMetrics],
      jsonGenerator: JsonGenerator,
      serializerProvider: SerializerProvider): Unit = {
    if (metrics.isEmpty) {
      // 空值写null
      jsonGenerator.writeNull()
    } else {
      metrics.foreach { m: ExecutorMetrics =>
        // 转换为指标名-值Map
        val metricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
          metric -> m.getMetricValue(metric)
        }
        jsonGenerator.writeObject(metricsMap)
      }
    }
  }

  override def isEmpty(provider: SerializerProvider, value: Option[ExecutorMetrics]): Boolean =
    value.isEmpty
}

/**
 * Executor峰值指标分位数分布JSON序列化器
 * 将ExecutorPeakMetricsDistributions转换为指标名-分位数分布Map输出
 */
private[spark] class ExecutorPeakMetricsDistributionsJsonSerializer
  extends JsonSerializer[ExecutorPeakMetricsDistributions] {
  override def serialize(
    metrics: ExecutorPeakMetricsDistributions,
    jsonGenerator: JsonGenerator,
    serializerProvider: SerializerProvider): Unit = {
    // 转换为指标名-分位数分布Map
    val metricsMap = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
      metric -> metrics.getMetricDistribution(metric)
    }
    jsonGenerator.writeObject(metricsMap)
  }
}

/**
 * Job数据结构
 * @param jobId Job ID
 * @param name Job名称
 * @param description Job描述
 * @param submissionTime 提交时间
 * @param completionTime 完成时间
 * @param stageIds 包含的Stage ID列表
 * @param jobGroup Job分组
 * @param jobTags Job标签列表
 * @param status 执行状态
 * @param numTasks 总任务数
 * @param numActiveTasks 活跃任务数
 * @param numCompletedTasks 完成任务数
 * @param numSkippedTasks 跳过任务数
 * @param numFailedTasks 失败任务数
 * @param numKilledTasks 被杀死任务数
 * @param numCompletedIndices 完成的分区索引数
 * @param numActiveStages 活跃Stage数
 * @param numCompletedStages 完成Stage数
 * @param numSkippedStages 跳过Stage数
 * @param numFailedStages 失败Stage数
 * @param killedTasksSummary 被杀死任务原因统计
 */
class JobData private[spark](
    val jobId: Int,
    val name: String,
    val description: Option[String],
    val submissionTime: Option[Date],
    val completionTime: Option[Date],
    val stageIds: collection.Seq[Int],
    val jobGroup: Option[String],
    val jobTags: collection.Seq[String],
    val status: JobExecutionStatus,
    val numTasks: Int,
    val numActiveTasks: Int,
    val numCompletedTasks: Int,
    val numSkippedTasks: Int,
    val numFailedTasks: Int,
    val numKilledTasks: Int,
    val numCompletedIndices: Int,
    val numActiveStages: Int,
    val numCompletedStages: Int,
    val numSkippedStages: Int,
    val numFailedStages: Int,
    val killedTasksSummary: Map[String, Int])

/**
 * RDD存储信息数据结构
 * @param id RDD ID
 * @param name RDD名称
 * @param numPartitions 总分区数
 * @param numCachedPartitions 缓存分区数
 * @param storageLevel 存储级别
 * @param memoryUsed 内存使用量
 * @param diskUsed 磁盘使用量
 * @param dataDistribution 各节点数据分布
 * @param partitions 分区信息列表
 */
class RDDStorageInfo private[spark](
    val id: Int,
    val name: String,
    val numPartitions: Int,
    val numCachedPartitions: Int,
    val storageLevel: String,
    val memoryUsed: Long,
    val diskUsed: Long,
    val dataDistribution: Option[collection.Seq[RDDDataDistribution]],
    val partitions: Option[collection.Seq[RDDPartitionInfo]])

/**
 * RDD在单个节点上的数据分布信息
 * @param address 节点地址
 * @param memoryUsed 已用内存
 * @param memoryRemaining 剩余内存
 * @param diskUsed 已用磁盘
 * @param onHeapMemoryUsed 已用堆内内存
 * @param offHeapMemoryUsed 已用堆外内存
 * @param onHeapMemoryRemaining 剩余堆内内存
 * @param offHeapMemoryRemaining 剩余堆外内存
 */
class RDDDataDistribution private[spark](
    val address: String,
    val memoryUsed: Long,
    val memoryRemaining: Long,
    val diskUsed: Long,
    @JsonDeserialize(contentAs = classOf[JLong])
    val onHeapMemoryUsed: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    val offHeapMemoryUsed: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    val onHeapMemoryRemaining: Option[Long],
    @JsonDeserialize(contentAs = classOf[JLong])
    val offHeapMemoryRemaining: Option[Long])

/**
 * RDD分区存储信息
 * @param blockName 块名称
 * @param storageLevel 存储级别
 * @param memoryUsed 内存使用量
 * @param diskUsed 磁盘使用量
 * @param executors 存储该分区的Executor列表
 */
class RDDPartitionInfo private[spark](
    val blockName: String,
    val storageLevel: String,
    val memoryUsed: Long,
    val diskUsed: Long,
    val executors: collection.Seq[String])

/**
 * Stage数据结构
 * @param status Stage状态
 * @param stageId Stage ID
 * @param attemptId 尝试ID
 * @param numTasks 总任务数
 * @param numActiveTasks 活跃任务数
 * @param numCompleteTasks 完成任务数
 * @param numFailedTasks 失败任务数
 * @param numKilledTasks 被杀死任务数
 * @param numCompletedIndices 完成分区索引数
 * @param submissionTime 提交时间
 * @param firstTaskLaunchedTime 第一个任务启动时间
 * @param completionTime 完成时间
 * @param failureReason 失败原因
 * @param executorDeserializeTime Executor反序列化时间
 *
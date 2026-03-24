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

package org.apache.spark.util

import java.util.{Properties, UUID}

import scala.collection.Map
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.core.{JsonGenerator, StreamReadConstraints}
import com.fasterxml.jackson.databind.JsonNode
import org.json4s.jackson.JsonMethods.compact

import org.apache.spark._
import org.apache.spark.executor._
import org.apache.spark.internal.config._
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.rdd.{DeterministicLevel, RDDOperationScope}
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, ResourceProfile, TaskResourceRequest}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils.weakIntern

/**
 * 用于向JsonProtocol传递配置选项的辅助类
 * 我们使用该类而不是直接传递SparkConf，避免每次读取时重复解析配置值
 */
private[spark] class JsonProtocolOptions(conf: SparkConf) {
  val includeTaskMetricsAccumulators: Boolean =
    conf.get(EVENT_LOG_INCLUDE_TASK_METRICS_ACCUMULATORS)
}

/**
 * SparkListener事件的JSON序列化与反序列化工具，提供强向后/向前兼容性保证：
 * 任何版本的Spark都应该能够读取其他任何版本（包括新版本）写出的JSON输出
 *
 * 修改方法时需要遵循以下规则以保证兼容性：
 *  - 永远不要删除已有的JSON字段
 *  - 任何新增JSON字段都必须是可选的，在反序列化方法中使用`jsonOption`读取
 *
 * 本类是Spark事件日志的核心序列化组件，用于事件日志的持久化和历史服务器的事件读取
 */
private[spark] object JsonProtocol extends JsonUtils {
  // TODO: Remove this file and put JSON serialization into each individual class.

  // SPARK-49872: 移除Jackson JSON字符串长度限制，支持超长JSON输出
  mapper.getFactory.setStreamReadConstraints(
    StreamReadConstraints.builder().maxStringLength(Int.MaxValue).build()
  )

  private[util]
  val defaultOptions: JsonProtocolOptions = new JsonProtocolOptions(new SparkConf(false))

  /** ------------------------------------------------- *
   * SparkListener事件的JSON序列化方法 |
   * -------------------------------------------------- */

  // 仅用于测试，生产代码应使用下方双参数重载方法
  /**
   * 将Spark事件转换为JSON字符串，使用默认配置
   * @param event 目标Spark事件
   * @return 序列化后的JSON字符串
   */
  def sparkEventToJsonString(event: SparkListenerEvent): String = {
    sparkEventToJsonString(event, defaultOptions)
  }

  /**
   * 将Spark事件转换为JSON字符串，使用指定配置
   * @param event 目标Spark事件
   * @param options JSON序列化配置选项
   * @return 序列化后的JSON字符串
   */
  def sparkEventToJsonString(event: SparkListenerEvent, options: JsonProtocolOptions): String = {
    toJsonString { generator =>
      writeSparkEventToJson(event, generator, options)
    }
  }

  /**
   * 将Spark事件写入JSON生成器
   * @param event 目标Spark事件
   * @param g JSON生成器
   * @param options JSON序列化配置选项
   */
  def writeSparkEventToJson(
      event: SparkListenerEvent,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        stageSubmittedToJson(stageSubmitted, g, options)
      case stageCompleted: SparkListenerStageCompleted =>
        stageCompletedToJson(stageCompleted, g, options)
      case taskStart: SparkListenerTaskStart =>
        taskStartToJson(taskStart, g, options)
      case taskGettingResult: SparkListenerTaskGettingResult =>
        taskGettingResultToJson(taskGettingResult, g, options)
      case taskEnd: SparkListenerTaskEnd =>
        taskEndToJson(taskEnd, g, options)
      case jobStart: SparkListenerJobStart =>
        jobStartToJson(jobStart, g, options)
      case jobEnd: SparkListenerJobEnd =>
        jobEndToJson(jobEnd, g)
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        environmentUpdateToJson(environmentUpdate, g)
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        blockManagerAddedToJson(blockManagerAdded, g)
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        blockManagerRemovedToJson(blockManagerRemoved, g)
      case unpersistRDD: SparkListenerUnpersistRDD =>
        unpersistRDDToJson(unpersistRDD, g)
      case applicationStart: SparkListenerApplicationStart =>
        applicationStartToJson(applicationStart, g)
      case applicationEnd: SparkListenerApplicationEnd =>
        applicationEndToJson(applicationEnd, g)
      case executorAdded: SparkListenerExecutorAdded =>
        executorAddedToJson(executorAdded, g)
      case executorRemoved: SparkListenerExecutorRemoved =>
        executorRemovedToJson(executorRemoved, g)
      case logStart: SparkListenerLogStart =>
        logStartToJson(logStart, g)
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        executorMetricsUpdateToJson(metricsUpdate, g)
      case stageExecutorMetrics: SparkListenerStageExecutorMetrics =>
        stageExecutorMetricsToJson(stageExecutorMetrics, g)
      case blockUpdate: SparkListenerBlockUpdated =>
        blockUpdateToJson(blockUpdate, g)
      case resourceProfileAdded: SparkListenerResourceProfileAdded =>
        resourceProfileAddedToJson(resourceProfileAdded, g)
      case _ =>
        mapper.writeValue(g, event)
    }
  }

  /**
   * Stage提交事件序列化
   */
  def stageSubmittedToJson(
      stageSubmitted: SparkListenerStageSubmitted,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageSubmitted)
    g.writeFieldName("Stage Info")
    // SPARK-42205: 启动事件不记录累加器，减少日志大小
    stageInfoToJson(stageSubmitted.stageInfo, g, options, includeAccumulables = false)
    Option(stageSubmitted.properties).foreach { properties =>
      g.writeFieldName("Properties")
      propertiesToJson(properties, g)
    }
    g.writeEndObject()
  }

  /**
   * Stage完成事件序列化
   */
  def stageCompletedToJson(
      stageCompleted: SparkListenerStageCompleted,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.stageCompleted)
    g.writeFieldName("Stage Info")
    stageInfoToJson(stageCompleted.stageInfo, g, options, includeAccumulables = true)
    g.writeEndObject()
  }

  /**
   * Task启动事件序列化
   */
  def taskStartToJson(
      taskStart: SparkListenerTaskStart,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskStart)
    g.writeNumberField("Stage ID", taskStart.stageId)
    g.writeNumberField("Stage Attempt ID", taskStart.stageAttemptId)
    g.writeFieldName("Task Info")
    // SPARK-42205: 启动事件不记录累加器，减少日志大小
    taskInfoToJson(taskStart.taskInfo, g, options, includeAccumulables = false)
    g.writeEndObject()
  }

  /**
   * Task获取结果事件序列化
   */
  def taskGettingResultToJson(
      taskGettingResult: SparkListenerTaskGettingResult,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    val taskInfo = taskGettingResult.taskInfo
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskGettingResult)
    g.writeFieldName("Task Info")
    // SPARK-42205: 获取结果事件不记录累加器，减少日志大小
    taskInfoToJson(taskInfo, g, options, includeAccumulables = false)
    g.writeEndObject()
  }

  /**
   * Task完成事件序列化
   */
  def taskEndToJson(
      taskEnd: SparkListenerTaskEnd,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.taskEnd)
    g.writeNumberField("Stage ID", taskEnd.stageId)
    g.writeNumberField("Stage Attempt ID", taskEnd.stageAttemptId)
    g.writeStringField("Task Type", taskEnd.taskType)
    g.writeFieldName("Task End Reason")
    taskEndReasonToJson(taskEnd.reason, g)
    g.writeFieldName("Task Info")
    taskInfoToJson(taskEnd.taskInfo, g, options, includeAccumulables = true)
    g.writeFieldName("Task Executor Metrics")
    executorMetricsToJson(taskEnd.taskExecutorMetrics, g)
    Option(taskEnd.taskMetrics).foreach { m =>
      g.writeFieldName("Task Metrics")
      taskMetricsToJson(m, g)
    }
    g.writeEndObject()
  }

  /**
   * Job启动事件序列化
   */
  def jobStartToJson(
      jobStart: SparkListenerJobStart,
      g: JsonGenerator,
      options: JsonProtocolOptions): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobStart)
    g.writeNumberField("Job ID", jobStart.jobId)
    g.writeNumberField("Submission Time", jobStart.time)
    g.writeArrayFieldStart("Stage Infos")  // Spark 1.2.0新增
    // SPARK-42205: 此处需要包含累加器，以准确记录作业提交时已完成Stage的完整信息
    // 一个Stage可能属于多个并发作业，即使没有事件日志和Stage完成的竞态条件也可能出现这种情况
    jobStart.stageInfos.foreach(stageInfoToJson(_, g, options, includeAccumulables = true))
    g.writeEndArray()
    g.writeArrayFieldStart("Stage IDs")
    jobStart.stageIds.foreach(g.writeNumber)
    g.writeEndArray()
    Option(jobStart.properties).foreach { properties =>
      g.writeFieldName("Properties")
      propertiesToJson(properties, g)
    }

    g.writeEndObject()
  }

  /**
   * Job完成事件序列化
   */
  def jobEndToJson(jobEnd: SparkListenerJobEnd, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.jobEnd)
    g.writeNumberField("Job ID", jobEnd.jobId)
    g.writeNumberField("Completion Time", jobEnd.time)
    g.writeFieldName("Job Result")
    jobResultToJson(jobEnd.jobResult, g)
    g.writeEndObject()
  }

  /**
   * 环境更新事件序列化
   */
  def environmentUpdateToJson(
      environmentUpdate: SparkListenerEnvironmentUpdate,
      g: JsonGenerator): Unit = {
    val environmentDetails = environmentUpdate.environmentDetails
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.environmentUpdate)
    writeMapField("JVM Information", environmentDetails("JVM Information").toMap, g)
    writeMapField("Spark Properties", environmentDetails("Spark Properties").toMap, g)
    writeMapField("Hadoop Properties", environmentDetails("Hadoop Properties").toMap, g)
    writeMapField("System Properties", environmentDetails("System Properties").toMap, g)
    writeMapField("Metrics Properties", environmentDetails("Metrics Properties").toMap, g)
    writeMapField("Classpath Entries", environmentDetails("Classpath Entries").toMap, g)
    g.writeEndObject()
  }

  /**
   * BlockManager添加事件序列化
   */
  def blockManagerAddedToJson(
      blockManagerAdded: SparkListenerBlockManagerAdded,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerAdded)
    g.writeFieldName("Block Manager ID")
    blockManagerIdToJson(blockManagerAdded.blockManagerId, g)
    g.writeNumberField("Maximum Memory", blockManagerAdded.maxMem)
    g.writeNumberField("Timestamp", blockManagerAdded.time)
    blockManagerAdded.maxOnHeapMem.foreach(g.writeNumberField("Maximum Onheap Memory", _))
    blockManagerAdded.maxOffHeapMem.foreach(g.writeNumberField("Maximum Offheap Memory", _))
    g.writeEndObject()
  }

  /**
   * BlockManager移除事件序列化
   */
  def blockManagerRemovedToJson(
      blockManagerRemoved: SparkListenerBlockManagerRemoved,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.blockManagerRemoved)
    g.writeFieldName("Block Manager ID")
    blockManagerIdToJson(blockManagerRemoved.blockManagerId, g)
    g.writeNumberField("Timestamp", blockManagerRemoved.time)
    g.writeEndObject()
  }

  /**
   * RDD取消持久化事件序列化
   */
  def unpersistRDDToJson(unpersistRDD: SparkListenerUnpersistRDD, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.unpersistRDD)
    g.writeNumberField("RDD ID", unpersistRDD.rddId)
    g.writeEndObject()
  }

  /**
   * 应用启动事件序列化
   */
  def applicationStartToJson(
      applicationStart: SparkListenerApplicationStart,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationStart)
    g.writeStringField("App Name", applicationStart.appName)
    applicationStart.appId.foreach(g.writeStringField("App ID", _))
    g.writeNumberField("Timestamp", applicationStart.time)
    g.writeStringField("User", applicationStart.sparkUser)
    applicationStart.appAttemptId.foreach(g.writeStringField("App Attempt ID", _))
    applicationStart.driverLogs.foreach(writeMapField("Driver Logs", _, g))
    applicationStart.driverAttributes.foreach(writeMapField("Driver Attributes", _, g))
    g.writeEndObject()
  }

  /**
   * 应用结束事件序列化
   */
  def applicationEndToJson(
      applicationEnd: SparkListenerApplicationEnd,
      g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.applicationEnd)
    g.writeNumberField("Timestamp", applicationEnd.time)
    applicationEnd.exitCode.foreach(exitCode => g.writeNumberField("ExitCode", exitCode))
    g.writeEndObject()
  }

  /**
   * 资源配置添加事件序列化
   */
  def resourceProfileAddedToJson(
      profileAdded: SparkListenerResourceProfileAdded,
      g: JsonGenerator
    ): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.resourceProfileAdded)
    g.writeNumberField("Resource Profile Id", profileAdded.resourceProfile.id)
    g.writeFieldName("Executor Resource Requests")
    executorResourceRequestMapToJson(profileAdded.resourceProfile.executorResources, g)
    g.writeFieldName("Task Resource Requests")
    taskResourceRequestMapToJson(profileAdded.resourceProfile.taskResources, g)
    g.writeEndObject()
  }

  /**
   * Executor添加事件序列化
   */
  def executorAddedToJson(executorAdded: SparkListenerExecutorAdded, g: JsonGenerator): Unit = {
    g.writeStartObject()
    g.writeStringField("Event", SPARK_LISTENER_EVENT_FORMATTED_CLASS_NAMES.executorAdded)
    g.writeNumberField("Timestamp", executorAdded.time)
    g.writeStringField("Executor ID", executorAdded.executorId)
    g.writeFieldName("Executor Info")
    executorInfoToJson(executorAdded.executorInfo, g)
    g.writeEndObject()
  }
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

import org.apache.spark.status.JobDataWrapper
import org.apache.spark.status.api.v1.JobData
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}

/**
 * JobDataWrapper 对象的 Protobuf 序列化器，实现将作业状态数据序列化/反序列化，用于状态持久化存储
 * 为 Spark 历史服务器提供状态数据的Protobuf编解码能力
 */
private[protobuf] class JobDataWrapperSerializer extends ProtobufSerDe[JobDataWrapper] {

  /**
   * 将 JobDataWrapper 对象序列化为二进制字节数组
   * @param j 待序列化的作业数据包装对象
   * @return 序列化后的二进制字节数组
   */
  override def serialize(j: JobDataWrapper): Array[Byte] = {
    // 序列化内层 JobData 对象
    val jobData = serializeJobData(j.info)
    // 构建 Protobuf 对象
    val builder = StoreTypes.JobDataWrapper.newBuilder()
    builder.setInfo(jobData)
    // 添加所有跳过的Stage ID
    j.skippedStages.foreach(builder.addSkippedStages)
    // 设置关联的SQL执行ID（可选）
    j.sqlExecutionId.foreach(builder.setSqlExecutionId)
    builder.build().toByteArray
  }

  /**
   * 将二进制字节数组反序列化为 JobDataWrapper 对象
   * @param bytes 待反序列化的二进制字节数组
   * @return 反序列化得到的 JobDataWrapper 对象
   */
  def deserialize(bytes: Array[Byte]): JobDataWrapper = {
    // 解析 Protobuf 对象
    val wrapper = StoreTypes.JobDataWrapper.parseFrom(bytes)
    // 获取可选的SQL执行ID
    val sqlExecutionId = getOptional(wrapper.hasSqlExecutionId, wrapper.getSqlExecutionId)
    // 反序列化内层JobData，转换跳过Stage集合，构建JobDataWrapper对象
    new JobDataWrapper(
      deserializeJobData(wrapper.getInfo),
      wrapper.getSkippedStagesList.asScala.map(_.toInt).toSet,
      sqlExecutionId
    )
  }

  /**
   * 序列化 JobData 对象为 Protobuf 格式
   * @param jobData 待序列化的API层作业数据对象
   * @return 序列化后的Protobuf JobData对象
   */
  private def serializeJobData(jobData: JobData): StoreTypes.JobData = {
    val jobDataBuilder = StoreTypes.JobData.newBuilder()
    jobDataBuilder.setJobId(jobData.jobId.toLong)
      // 序列化作业执行状态
      .setStatus(JobExecutionStatusSerializer.serialize(jobData.status))
      // 设置各类任务计数统计
      .setNumTasks(jobData.numTasks)
      .setNumActiveTasks(jobData.numActiveTasks)
      .setNumCompletedTasks(jobData.numCompletedTasks)
      .setNumSkippedTasks(jobData.numSkippedTasks)
      .setNumFailedTasks(jobData.numFailedTasks)
      .setNumKilledTasks(jobData.numKilledTasks)
      .setNumCompletedIndices(jobData.numCompletedIndices)
      // 设置各类阶段计数统计
      .setNumActiveStages(jobData.numActiveStages)
      .setNumCompletedStages(jobData.numCompletedStages)
      .setNumSkippedStages(jobData.numSkippedStages)
      .setNumFailedStages(jobData.numFailedStages)
    // 设置作业名称（处理空值情况）
    setStringField(jobData.name, jobDataBuilder.setName)
    // 设置作业描述（可选）
    jobData.description.foreach(jobDataBuilder.setDescription)
    // 设置提交时间戳（可选）
    jobData.submissionTime.foreach { d =>
      jobDataBuilder.setSubmissionTime(d.getTime)
    }
    // 设置完成时间戳（可选）
    jobData.completionTime.foreach { d =>
      jobDataBuilder.setCompletionTime(d.getTime)
    }
    // 添加所有关联Stage的ID
    jobData.stageIds.foreach(id => jobDataBuilder.addStageIds(id.toLong))
    // 设置作业组ID（可选）
    jobData.jobGroup.foreach(jobDataBuilder.setJobGroup)
    // 添加所有作业标签
    jobData.jobTags.foreach(jobDataBuilder.addJobTags)
    // 添加已杀死任务汇总信息
    jobData.killedTasksSummary.foreach { entry =>
      jobDataBuilder.putKillTasksSummary(entry._1, entry._2)
    }
    jobDataBuilder.build()
  }

  /**
   * 从 Protobuf 对象反序列化得到 API 层 JobData 对象
   * @param info Protobuf 格式的JobData对象
   * @return 反序列化得到的API层JobData对象
   */
  private def deserializeJobData(info: StoreTypes.JobData): JobData = {
    // 获取可选的作业描述
    val description = getOptional(info.hasDescription, info.getDescription)
    // 获取可选的提交时间，转换为Date对象
    val submissionTime =
      getOptional(info.hasSubmissionTime, () => new Date(info.getSubmissionTime))
    // 获取可选的完成时间，转换为Date对象
    val completionTime = getOptional(info.hasCompletionTime, () => new Date(info.getCompletionTime))
    // 获取可选的作业组
    val jobGroup = getOptional(info.hasJobGroup, info.getJobGroup)
    // 反序列化作业执行状态
    val status = JobExecutionStatusSerializer.deserialize(info.getStatus)

    // 构建API层JobData对象
    new JobData(
      jobId = info.getJobId.toInt,
      name = getStringField(info.hasName, info.getName),
      description = description,
      submissionTime = submissionTime,
      completionTime = completionTime,
      stageIds = info.getStageIdsList.asScala.map(_.toInt),
      jobGroup = jobGroup,
      jobTags = info.getJobTagsList.asScala,
      status = status,
      numTasks = info.getNumTasks,
      numActiveTasks = info.getNumActiveTasks,
      numCompletedTasks = info.getNumCompletedTasks,
      numSkippedTasks = info.getNumSkippedTasks,
      numFailedTasks = info.getNumFailedTasks,
      numKilledTasks = info.getNumKilledTasks,
      numCompletedIndices = info.getNumCompletedIndices,
      numActiveStages = info.getNumActiveStages,
      numCompletedStages = info.getNumCompletedStages,
      numSkippedStages = info.getNumSkippedStages,
      numFailedStages = info.getNumFailedStages,
      killedTasksSummary = info.getKillTasksSummaryMap.asScala.toMap.transform((_, v) => v.toInt))
  }
}
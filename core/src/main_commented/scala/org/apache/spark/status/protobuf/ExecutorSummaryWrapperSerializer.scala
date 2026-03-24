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

import org.apache.spark.resource.ResourceInformation
import org.apache.spark.status.ExecutorSummaryWrapper
import org.apache.spark.status.api.v1.{ExecutorSummary, MemoryMetrics}
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

/**
 * ExecutorSummaryWrapper 的 Protobuf 序列化/反序列化器，用于将执行器摘要信息存储到状态存储
 * 实现了 Spark 状态存储的 Protobuf 序列化接口，负责将内存中的执行器摘要对象转换为字节数组持久化，
 * 并在读取时将字节数组反序列化回对象。
 */
private[protobuf] class ExecutorSummaryWrapperSerializer
  extends ProtobufSerDe[ExecutorSummaryWrapper] {

  /**
   * 将 ExecutorSummaryWrapper 对象序列化为 Protobuf 字节数组
   * @param input 待序列化的执行器摘要包装对象
   * @return 序列化后的字节数组
   */
  override def serialize(input: ExecutorSummaryWrapper): Array[Byte] = {
    val info = serializeExecutorSummary(input.info)
    val builder = StoreTypes.ExecutorSummaryWrapper.newBuilder()
      .setInfo(info)
    builder.build().toByteArray
  }

  /**
   * 将 Protobuf 字节数组反序列化为 ExecutorSummaryWrapper 对象
   * @param bytes 待反序列化的字节数组
   * @return 反序列化得到的执行器摘要包装对象
   */
  def deserialize(bytes: Array[Byte]): ExecutorSummaryWrapper = {
    val binary = StoreTypes.ExecutorSummaryWrapper.parseFrom(bytes)
    val info = deserializeExecutorSummary(binary.getInfo)
    new ExecutorSummaryWrapper(info = info)
  }

  /**
   * 将 ExecutorSummary 对象序列化为 Protobuf 对象
   * @param input 待序列化的执行器摘要API对象
   * @return 序列化后的 Protobuf ExecutorSummary 对象
   */
  private def serializeExecutorSummary(
      input: ExecutorSummary): StoreTypes.ExecutorSummary = {
    val builder = StoreTypes.ExecutorSummary.newBuilder()
      .setIsActive(input.isActive)
      .setRddBlocks(input.rddBlocks)
      .setMemoryUsed(input.memoryUsed)
      .setDiskUsed(input.diskUsed)
      .setTotalCores(input.totalCores)
      .setMaxTasks(input.maxTasks)
      .setActiveTasks(input.activeTasks)
      .setFailedTasks(input.failedTasks)
      .setCompletedTasks(input.completedTasks)
      .setTotalTasks(input.totalTasks)
      .setTotalDuration(input.totalDuration)
      .setTotalGcTime(input.totalGCTime)
      .setTotalInputBytes(input.totalInputBytes)
      .setTotalShuffleRead(input.totalShuffleRead)
      .setTotalShuffleWrite(input.totalShuffleWrite)
      .setIsBlacklisted(input.isBlacklisted)
      .setMaxMemory(input.maxMemory)
      .setAddTime(input.addTime.getTime)
    // 设置执行器ID字符串
    setStringField(input.id, builder.setId)
    // 设置执行器主机端口字符串
    setStringField(input.hostPort, builder.setHostPort)
    // 移除时间存在时设置移除时间时间戳
    input.removeTime.foreach {
      date => builder.setRemoveTime(date.getTime)
    }
    // 移除原因存在时设置移除原因
    input.removeReason.foreach(builder.setRemoveReason)
    // 遍历设置执行器日志链接映射
    input.executorLogs.foreach { case (k, v) =>
      builder.putExecutorLogs(k, v)
    }
    // 内存指标存在时序列化并设置内存指标
    input.memoryMetrics.foreach { metrics =>
      builder.setMemoryMetrics(serializeMemoryMetrics(metrics))
    }
    // 遍历添加被拉黑的阶段ID
    input.blacklistedInStages.foreach { stage =>
      builder.addBlacklistedInStages(stage.toLong)
    }
    // 峰值内存指标存在时序列化并设置峰值内存指标
    input.peakMemoryMetrics.foreach { metrics =>
      builder.setPeakMemoryMetrics(ExecutorMetricsSerializer.serialize(metrics))
    }
    // 遍历设置执行器属性映射
    input.attributes.foreach { case (k, v) =>
      builder.putAttributes(k, v)
    }
    // 遍历序列化并设置资源信息映射
    input.resources.foreach { case (k, v) =>
      builder.putResources(k, serializeResourceInformation(v))
    }

    // 设置资源配置文件ID
    builder.setResourceProfileId(input.resourceProfileId)
    // 设置是否被排除标记
    builder.setIsExcluded(input.isExcluded)

    // 遍历添加被排除的阶段ID
    input.excludedInStages.foreach { stage =>
      builder.addExcludedInStages(stage.toLong)
    }

    builder.build()
  }

  /**
   * 将 Protobuf 对象反序列化为 ExecutorSummary 对象
   * @param binary 待反序列化的 Protobuf ExecutorSummary 对象
   * @return 反序列化得到的执行器摘要API对象
   */
  private def deserializeExecutorSummary(
      binary: StoreTypes.ExecutorSummary): ExecutorSummary = {
    // 反序列化峰值内存指标（存在时）
    val peakMemoryMetrics =
      getOptional(binary.hasPeakMemoryMetrics,
        () => ExecutorMetricsSerializer.deserialize(binary.getPeakMemoryMetrics))
    // 获取移除时间（存在时）
    val removeTime = getOptional(binary.hasRemoveTime, () => new Date(binary.getRemoveTime))
    // 获取移除原因（存在时）
    val removeReason = getOptional(binary.hasRemoveReason, () => binary.getRemoveReason)
    // 反序列化内存指标（存在时）
    val memoryMetrics =
      getOptional(binary.hasMemoryMetrics,
        () => deserializeMemoryMetrics(binary.getMemoryMetrics))
    new ExecutorSummary(
      id = getStringField(binary.hasId, binary.getId),
      hostPort = getStringField(binary.hasHostPort, () => weakIntern(binary.getHostPort)),
      isActive = binary.getIsActive,
      rddBlocks = binary.getRddBlocks,
      memoryUsed = binary.getMemoryUsed,
      diskUsed = binary.getDiskUsed,
      totalCores = binary.getTotalCores,
      maxTasks = binary.getMaxTasks,
      activeTasks = binary.getActiveTasks,
      failedTasks = binary.getFailedTasks,
      completedTasks = binary.getCompletedTasks,
      totalTasks = binary.getTotalTasks,
      totalDuration = binary.getTotalDuration,
      totalGCTime = binary.getTotalGcTime,
      totalInputBytes = binary.getTotalInputBytes,
      totalShuffleRead = binary.getTotalShuffleRead,
      totalShuffleWrite = binary.getTotalShuffleWrite,
      isBlacklisted = binary.getIsBlacklisted,
      maxMemory = binary.getMaxMemory,
      addTime = new Date(binary.getAddTime),
      removeTime = removeTime,
      removeReason = removeReason,
      executorLogs = binary.getExecutorLogsMap.asScala.toMap,
      memoryMetrics = memoryMetrics,
      blacklistedInStages = binary.getBlacklistedInStagesList.asScala.map(_.toInt).toSet,
      peakMemoryMetrics = peakMemoryMetrics,
      attributes = binary.getAttributesMap.asScala.toMap,
      resources =
        binary.getResourcesMap.asScala.toMap.transform((_, v) => deserializeResourceInformation(v)),
      resourceProfileId = binary.getResourceProfileId,
      isExcluded = binary.getIsExcluded,
      excludedInStages = binary.getExcludedInStagesList.asScala.map(_.toInt).toSet)
  }

  /**
   * 将内存指标对象序列化为 Protobuf 对象
   * @param metrics 内存指标对象
   * @return 序列化后的 Protobuf 内存指标对象
   */
  private def serializeMemoryMetrics(metrics: MemoryMetrics): StoreTypes.MemoryMetrics = {
    val builder = StoreTypes.MemoryMetrics.newBuilder()
    builder.setUsedOnHeapStorageMemory(metrics.usedOnHeapStorageMemory)
    builder.setUsedOffHeapStorageMemory(metrics.usedOffHeapStorageMemory)
    builder.setTotalOnHeapStorageMemory(metrics.totalOnHeapStorageMemory)
    builder.setTotalOffHeapStorageMemory(metrics.totalOffHeapStorageMemory)
    builder.build()
  }

  /**
   * 将 Protobuf 内存指标对象反序列化为内存指标对象
   * @param binary Protobuf 内存指标对象
   * @return 反序列化得到的内存指标对象
   */
  private def deserializeMemoryMetrics(binary: StoreTypes.MemoryMetrics): MemoryMetrics = {
    new MemoryMetrics(
      usedOnHeapStorageMemory = binary.getUsedOnHeapStorageMemory,
      usedOffHeapStorageMemory = binary.getUsedOffHeapStorageMemory,
      totalOnHeapStorageMemory = binary.getTotalOnHeapStorageMemory,
      totalOffHeapStorageMemory = binary.getTotalOffHeapStorageMemory
    )
  }

  /**
   * 将资源信息对象序列化为 Protobuf 对象
   * @param info 资源信息对象
   * @return 序列化后的 Protobuf 资源信息对象
   */
  private def serializeResourceInformation(info: ResourceInformation):
    StoreTypes.ResourceInformation = {
    val builder = StoreTypes.ResourceInformation.newBuilder()
    setStringField(info.name, builder.setName)
    if (info.addresses != null) {
      info.addresses.foreach(builder.addAddresses)
    }
    builder.build()
  }

  /**
   * 将 Protobuf 资源信息对象反序列化为资源信息对象
   * @param binary Protobuf 资源信息对象
   * @return 反序列化得到的资源信息对象
   */
  private def deserializeResourceInformation(binary: StoreTypes.ResourceInformation):
    ResourceInformation = {
    new ResourceInformation(
      name = getStringField(binary.hasName, () => weakIntern(binary.getName)),
      addresses = binary.getAddressesList.asScala.map(weakIntern).toArray)
  }
}
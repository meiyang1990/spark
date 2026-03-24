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

import org.apache.spark.status.ProcessSummaryWrapper
import org.apache.spark.status.api.v1.ProcessSummary
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}

/**
 * ProcessSummaryWrapper对象的Protobuf序列化器，实现ProtobufSerDe接口
 * 负责将Spark状态跟踪中的进程概要包装类序列化/反序列化为Protobuf格式，用于状态持久化
 */
private[protobuf] class ProcessSummaryWrapperSerializer
  extends ProtobufSerDe[ProcessSummaryWrapper] {

  /**
   * 将ProcessSummaryWrapper对象序列化为二进制字节数组
   * @param input 待序列化的ProcessSummaryWrapper对象
   * @return 序列化后的Protobuf格式字节数组
   */
  override def serialize(input: ProcessSummaryWrapper): Array[Byte] = {
    val builder = StoreTypes.ProcessSummaryWrapper.newBuilder()
    // 序列化内部包含的ProcessSummary对象并设置到Protobuf builder
    builder.setInfo(serializeProcessSummary(input.info))
    builder.build().toByteArray
  }

  /**
   * 将二进制字节数组反序列化为ProcessSummaryWrapper对象
   * @param bytes Protobuf格式的二进制字节数组
   * @return 反序列化得到的ProcessSummaryWrapper对象
   */
  def deserialize(bytes: Array[Byte]): ProcessSummaryWrapper = {
    val wrapper = StoreTypes.ProcessSummaryWrapper.parseFrom(bytes)
    new ProcessSummaryWrapper(
      info = deserializeProcessSummary(wrapper.getInfo)
    )
  }

  /**
   * 序列化ProcessSummary进程概要对象为Protobuf结构
   * @param info 待序列化的ProcessSummary对象
   * @return 序列化后的Protobuf ProcessSummary对象
   */
  private def serializeProcessSummary(info: ProcessSummary): StoreTypes.ProcessSummary = {
    val builder = StoreTypes.ProcessSummary.newBuilder()
    setStringField(info.id, builder.setId)
    setStringField(info.hostPort, builder.setHostPort)
    builder.setIsActive(info.isActive)
    builder.setTotalCores(info.totalCores)
    // 设置添加时间，转换为时间戳存储
    builder.setAddTime(info.addTime.getTime)
    // 移除时间存在时才设置，处理可选字段
    info.removeTime.foreach { d =>
      builder.setRemoveTime(d.getTime)
    }
    // 遍历添加所有进程日志链接
    info.processLogs.foreach { case (k, v) =>
      builder.putProcessLogs(k, v)
    }
    builder.build()
  }

  /**
   * 从Protobuf结构反序列化得到ProcessSummary对象
   * @param info Protobuf格式的ProcessSummary对象
   * @return 反序列化得到的Spark ProcessSummary领域对象
   */
  private def deserializeProcessSummary(info: StoreTypes.ProcessSummary): ProcessSummary = {
    // 处理可选的移除时间，仅当存在时创建Date对象
    val removeTime = getOptional(info.hasRemoveTime, () => new Date(info.getRemoveTime))
    new ProcessSummary(
      id = getStringField(info.hasId, info.getId),
      hostPort = getStringField(info.hasHostPort, info.getHostPort),
      isActive = info.getIsActive,
      totalCores = info.getTotalCores,
      // 从时间戳重建添加时间Date对象
      addTime = new Date(info.getAddTime),
      removeTime = removeTime,
      // 将Java Map转换为Scala Map
      processLogs = info.getProcessLogsMap.asScala.toMap
    )
  }
}
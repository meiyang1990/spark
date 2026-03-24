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

import org.apache.spark.status.SpeculationStageSummaryWrapper
import org.apache.spark.status.api.v1.SpeculationStageSummary

/**
 * 推测执行阶段摘要包装器的Protobuf序列化与反序列化实现
 * 负责将Spark状态存储中的推测执行阶段摘要对象转换为Protobuf二进制格式，或反向转换
 */
private[protobuf] class SpeculationStageSummaryWrapperSerializer
  extends ProtobufSerDe[SpeculationStageSummaryWrapper] {

  /**
   * 将推测执行阶段摘要包装器序列化为Protobuf二进制字节数组
   * @param s 待序列化的推测执行阶段摘要包装器对象
   * @return 序列化后的二进制字节数组
   */
  override def serialize(s: SpeculationStageSummaryWrapper): Array[Byte] = {
    // 序列化内部的推测执行阶段摘要信息
    val summary = serializeSpeculationStageSummary(s.info)
    // 构建Protobuf包装器对象
    val builder = StoreTypes.SpeculationStageSummaryWrapper.newBuilder()
    // 设置阶段ID
    builder.setStageId(s.stageId.toLong)
    // 设置阶段尝试ID
    builder.setStageAttemptId(s.stageAttemptId)
    // 设置序列化后的摘要信息
    builder.setInfo(summary)
    // 构建对象并转换为字节数组返回
    builder.build().toByteArray
  }

  /**
   * 从Protobuf二进制字节数组反序列化出推测执行阶段摘要包装器
   * @param bytes 待反序列化的Protobuf二进制字节数组
   * @return 反序列化得到的推测执行阶段摘要包装器对象
   */
  def deserialize(bytes: Array[Byte]): SpeculationStageSummaryWrapper = {
    // 从字节数组解析出Protobuf包装器对象
    val wrapper = StoreTypes.SpeculationStageSummaryWrapper.parseFrom(bytes)
    // 构建Spark内部使用的对象，转换ID类型并反序列化摘要信息
    new SpeculationStageSummaryWrapper(
      stageId = wrapper.getStageId.toInt,
      stageAttemptId = wrapper.getStageAttemptId,
      info = deserializeSpeculationStageSummary(wrapper.getInfo)
    )
  }

  /**
   * 将API层的推测执行阶段摘要对象序列化为Protobuf对象
   * @param summary API层的推测执行阶段摘要对象
   * @return Protobuf格式的推测执行阶段摘要对象
   */
  private def serializeSpeculationStageSummary(summary: SpeculationStageSummary):
    StoreTypes.SpeculationStageSummary = {
    val summaryBuilder = StoreTypes.SpeculationStageSummary.newBuilder()
    summaryBuilder.setNumTasks(summary.numTasks)
    summaryBuilder.setNumActiveTasks(summary.numActiveTasks)
    summaryBuilder.setNumCompletedTasks(summary.numCompletedTasks)
    summaryBuilder.setNumFailedTasks(summary.numFailedTasks)
    summaryBuilder.setNumKilledTasks(summary.numKilledTasks)
    summaryBuilder.build()
  }

  /**
   * 从Protobuf格式的推测执行阶段摘要反序列化为API层对象
   * @param info Protobuf格式的推测执行阶段摘要对象
   * @return API层的推测执行阶段摘要对象
   */
  private def deserializeSpeculationStageSummary(info: StoreTypes.SpeculationStageSummary):
    SpeculationStageSummary = {
    new SpeculationStageSummary(
      numTasks = info.getNumTasks,
      numActiveTasks = info.getNumActiveTasks,
      numCompletedTasks = info.getNumCompletedTasks,
      numFailedTasks = info.getNumFailedTasks,
      numKilledTasks = info.getNumKilledTasks)
  }
}
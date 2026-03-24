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
 * 文件说明：Executor阶段执行摘要包装器的Protobuf序列化器实现
 * 所属模块：Spark核心状态存储模块，负责将应用运行状态数据序列化存储为Protobuf格式
 */
package org.apache.spark.status.protobuf

import org.apache.spark.status.ExecutorStageSummaryWrapper
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

/**
 * ExecutorStageSummaryWrapper的Protobuf序列化/反序列化实现类
 * 负责将Executor在某个Stage的执行摘要包装类转换为Protobuf字节数组，以及从字节数组反序列化还原对象
 * 用于Spark状态存储系统的持久化序列化
 */
private[protobuf] class ExecutorStageSummaryWrapperSerializer
  extends ProtobufSerDe[ExecutorStageSummaryWrapper] {

  /**
   * 将ExecutorStageSummaryWrapper对象序列化为Protobuf字节数组
   * @param input 待序列化的Executor阶段执行摘要包装对象
   * @return 序列化后的二进制字节数组
   */
  override def serialize(input: ExecutorStageSummaryWrapper): Array[Byte] = {
    // 先序列化内部封装的Executor阶段执行摘要信息
    val info = ExecutorStageSummarySerializer.serialize(input.info)
    // 构建Protobuf消息对象，填充各个字段
    val builder = StoreTypes.ExecutorStageSummaryWrapper.newBuilder()
      .setStageId(input.stageId.toLong)
      .setStageAttemptId(input.stageAttemptId)
      .setInfo(info)
    // 设置executorId可选字段，处理空值情况
    setStringField(input.executorId, builder.setExecutorId)
    builder.build().toByteArray
  }

  /**
   * 从Protobuf二进制字节数组反序列化还原ExecutorStageSummaryWrapper对象
   * @param bytes 待反序列化的二进制字节数组
   * @return 反序列化得到的Executor阶段执行摘要包装对象
   */
  def deserialize(bytes: Array[Byte]): ExecutorStageSummaryWrapper = {
    // 从字节数组解析Protobuf消息
    val binary = StoreTypes.ExecutorStageSummaryWrapper.parseFrom(bytes)
    // 反序列化内部封装的执行摘要信息
    val info = ExecutorStageSummarySerializer.deserialize(binary.getInfo)
    // 构建原始对象，使用弱字符串驻留优化executorId内存占用
    new ExecutorStageSummaryWrapper(
      stageId = binary.getStageId.toInt,
      stageAttemptId = binary.getStageAttemptId,
      executorId = getStringField(binary.hasExecutorId, () => weakIntern(binary.getExecutorId)),
      info = info)
  }
}
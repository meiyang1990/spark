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

import org.apache.spark.status.StreamBlockData
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

/**
 * StreamBlockData 的 Protobuf 序列化/反序列化器，实现状态存储中流块数据的编解码
 * 用于 Spark 状态存储系统的持久化与网络传输
 */
private[protobuf] class StreamBlockDataSerializer extends ProtobufSerDe[StreamBlockData] {

  /**
   * 将 StreamBlockData 对象序列化为 Protobuf 字节数组
   * @param data 待序列化的流块数据对象
   * @return 序列化后的字节数组
   */
  override def serialize(data: StreamBlockData): Array[Byte] = {
    // 创建 Protobuf 消息构建器
    val builder = StoreTypes.StreamBlockData.newBuilder()
    // 设置流块名称字段
    setStringField(data.name, builder.setName)
    // 设置所属执行器ID字段
    setStringField(data.executorId, builder.setExecutorId)
    // 设置执行器主机端口字段
    setStringField(data.hostPort, builder.setHostPort)
    // 设置存储级别字段
    setStringField(data.storageLevel, builder.setStorageLevel)
    // 设置是否使用内存存储
    builder.setUseMemory(data.useMemory)
      // 设置是否使用磁盘存储
      .setUseDisk(data.useDisk)
      // 设置是否反序列化存储
      .setDeserialized(data.deserialized)
      // 设置内存使用大小
      .setMemSize(data.memSize)
      // 设置磁盘使用大小
      .setDiskSize(data.diskSize)
    // 构建消息并转换为字节数组返回
    builder.build().toByteArray
  }

  /**
   * 将 Protobuf 字节数组反序列化为 StreamBlockData 对象
   * @param bytes 待反序列化的 Protobuf 字节数组
   * @return 反序列化得到的流块数据对象
   */
  override def deserialize(bytes: Array[Byte]): StreamBlockData = {
    // 从字节数组解析 Protobuf 消息
    val binary = StoreTypes.StreamBlockData.parseFrom(bytes)
    new StreamBlockData(
      name = getStringField(binary.hasName, () => binary.getName),
      executorId = getStringField(binary.hasExecutorId, () => weakIntern(binary.getExecutorId)),
      hostPort = getStringField(binary.hasHostPort, () => weakIntern(binary.getHostPort)),
      storageLevel =
        getStringField(binary.hasStorageLevel, () => weakIntern(binary.getStorageLevel)),
      useMemory = binary.getUseMemory,
      useDisk = binary.getUseDisk,
      deserialized = binary.getDeserialized,
      memSize = binary.getMemSize,
      diskSize = binary.getDiskSize)
  }
}
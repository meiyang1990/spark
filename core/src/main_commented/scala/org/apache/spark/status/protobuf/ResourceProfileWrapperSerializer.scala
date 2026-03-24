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

import org.apache.spark.status.ResourceProfileWrapper

/**
 * ResourceProfileWrapper的Protobuf序列化与反序列化实现
 * 用于将Spark状态存储中的资源配置封装对象转换为Protobuf二进制格式，实现持久化存储
 */
private[protobuf] class ResourceProfileWrapperSerializer
  extends ProtobufSerDe[ResourceProfileWrapper] {

  private val appEnvSerializer = new ApplicationEnvironmentInfoWrapperSerializer

  /**
   * 将ResourceProfileWrapper对象序列化为Protobuf二进制字节数组
   * @param input 待序列化的资源配置封装对象
   * @return 序列化后的二进制字节数组
   */
  override def serialize(input: ResourceProfileWrapper): Array[Byte] = {
    // 构建Protobuf消息构造器
    val builder = StoreTypes.ResourceProfileWrapper.newBuilder()
    // 序列化嵌套的资源配置信息，复用应用环境序列化器
    builder.setRpInfo(appEnvSerializer.serializeResourceProfileInfo(input.rpInfo))
    // 构建消息并转换为字节数组返回
    builder.build().toByteArray
  }

  /**
   * 将Protobuf二进制字节数组反序列化为ResourceProfileWrapper对象
   * @param bytes 待反序列化的二进制字节数组
   * @return 反序列化得到的资源配置封装对象
   */
  def deserialize(bytes: Array[Byte]): ResourceProfileWrapper = {
    // 从字节数组解析Protobuf消息
    val wrapper = StoreTypes.ResourceProfileWrapper.parseFrom(bytes)
    // 反序列化嵌套的资源配置信息，构造返回对象
    new ResourceProfileWrapper(
      rpInfo = appEnvSerializer.deserializeResourceProfileInfo(wrapper.getRpInfo)
    )
  }
}
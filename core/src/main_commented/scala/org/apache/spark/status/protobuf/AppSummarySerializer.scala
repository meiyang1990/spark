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

import org.apache.spark.status.AppSummary

/**
 * 应用概要信息的Protobuf序列化器，实现AppSummary对象与Protobuf字节流的互相转换
 * 用于Spark状态存储的持久化与远程传输
 */
private[protobuf] class AppSummarySerializer extends ProtobufSerDe[AppSummary] {

  /**
   * 将内存中的AppSummary对象序列化为Protobuf字节数组
   * @param input 待序列化的应用概要内存对象
   * @return 序列化后的Protobuf格式字节数组
   */
  override def serialize(input: AppSummary): Array[Byte] = {
    val builder = StoreTypes.AppSummary.newBuilder()
      .setNumCompletedJobs(input.numCompletedJobs)
      .setNumCompletedStages(input.numCompletedStages)
    builder.build().toByteArray
  }

  /**
   * 将Protobuf字节数组反序列化为内存中的AppSummary对象
   * @param bytes 待反序列化的Protobuf格式字节数组
   * @return 反序列化后的应用概要内存对象
   */
  override def deserialize(bytes: Array[Byte]): AppSummary = {
    val summary = StoreTypes.AppSummary.parseFrom(bytes)
    new AppSummary(
      numCompletedJobs = summary.getNumCompletedJobs,
      numCompletedStages = summary.getNumCompletedStages
    )
  }
}
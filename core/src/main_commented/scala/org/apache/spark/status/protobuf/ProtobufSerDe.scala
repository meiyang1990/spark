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

import org.apache.spark.annotation.{DeveloperApi, Unstable}

/**
 * 文件级：Spark UI状态存储Protobuf序列化反序列化接口
 * 核心职责：定义UI状态数据的Protobuf序列化/反序列化统一API，供ServiceLoader动态加载实现
 */

/**
 * :: DeveloperApi ::
 * Protobuf序列化反序列化抽象接口，用于Spark UI相关状态数据的Protobuf序列化处理。
 * 实现类需要注册到服务发现机制中，由KVStoreProtobufSerializer通过ServiceLoader自动加载使用。
 * 
 * @tparam T 需要序列化/反序列化的实体类型
 * @since 3.4.0
 */
@DeveloperApi
@Unstable
trait ProtobufSerDe[T] {

  /**
   * 将指定类型的实体序列化为Protobuf字节数组
   * @param input 待序列化的输入实体对象
   * @return 序列化后的Protobuf字节数组
   */
  def serialize(input: T): Array[Byte]

  /**
   * 将Protobuf字节数组反序列化为指定类型的实体对象
   * @param bytes 输入的Protobuf字节数组
   * @return 反序列化后的实体对象
   */
  def deserialize(bytes: Array[Byte]): T
}
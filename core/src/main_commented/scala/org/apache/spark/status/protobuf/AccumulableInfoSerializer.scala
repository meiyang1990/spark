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

import java.util.{List => JList}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.status.api.v1.AccumulableInfo
import org.apache.spark.status.protobuf.Utils.{getOptional, getStringField, setStringField}
import org.apache.spark.util.Utils.weakIntern

/**
 * 累加器信息序列化工具类，负责将AccumulableInfo对象与Protobuf格式互相转换
 * 用于Spark状态存储的持久化与网络传输
 */
private[protobuf] object AccumulableInfoSerializer {

  /**
   * 将API层的AccumulableInfo对象序列化为Protobuf格式
   * @param input 待序列化的累加器信息对象
   * @return 序列化后的Protobuf累加器信息对象
   */
  def serialize(input: AccumulableInfo): StoreTypes.AccumulableInfo = {
    val builder = StoreTypes.AccumulableInfo.newBuilder()
      .setId(input.id)
    setStringField(input.name, builder.setName)
    setStringField(input.value, builder.setValue)
    input.update.foreach(builder.setUpdate)
    builder.build()
  }

  /**
   * 将批量Protobuf格式累加器信息反序列化为API层AccumulableInfo对象列表
   * @param updates Protobuf格式累加器信息列表
   * @return 反序列化后的API层AccumulableInfo数组缓冲区
   */
  def deserialize(updates: JList[StoreTypes.AccumulableInfo]): ArrayBuffer[AccumulableInfo] = {
    val accumulatorUpdates = new ArrayBuffer[AccumulableInfo](updates.size())
    // 遍历批量反序列化每个累加器信息
    updates.forEach { update =>
      accumulatorUpdates.append(new AccumulableInfo(
        id = update.getId,
        // 使用弱驻留池复用字符串，减少内存占用
        name = getStringField(update.hasName, () => weakIntern(update.getName)),
        update = getOptional(update.hasUpdate, update.getUpdate),
        value = getStringField(update.hasValue, update.getValue)))
    }
    accumulatorUpdates
  }
}
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

import java.util.{Map => JMap}

/**
 * Protobuf序列化工具类，为Spark状态信息Protobuf编解码提供通用辅助方法
 */
private[protobuf] object Utils {

  /**
   * 根据条件判断是否包装值为Option类型，用于处理Protobuf可选字段
   * @param condition 字段是否存在的条件
   * @param result 获取字段值的函数
   * @tparam T 字段值类型
   * @return 包装后的Option，条件满足返回Some，否则返回None
   */
  def getOptional[T](condition: Boolean, result: () => T): Option[T] = if (condition) {
    Some(result())
  } else {
    None
  }

  /**
   * 当输入字符串非空时，调用设置方法设置字符串字段，用于Protobuf字段写入
   * @param input 输入字符串
   * @param f 设置字段的回调函数
   */
  def setStringField(input: String, f: String => Any): Unit = {
    if (input != null) {
      f(input)
    }
  }

  /**
   * 根据条件获取字符串字段，不存在时返回null，用于Protobuf字段读取
   * @param condition 字段是否存在的条件
   * @param result 获取字段值的函数
   * @return 字段值或null（字段不存在）
   */
  def getStringField(condition: Boolean, result: () => String): String = if (condition) {
    result()
  } else {
    null
  }

  /**
   * 当输入Java Map非空时，批量写入Map字段，用于Protobuf Map类型处理
   * @param input 输入Java Map
   * @param putAllFunc 批量写入Map的回调函数
   * @tparam K Map键类型
   * @tparam V Map值类型
   */
  def setJMapField[K, V](input: JMap[K, V], putAllFunc: JMap[K, V] => Any): Unit = {
    if (input != null && !input.isEmpty) {
      putAllFunc(input)
    }
  }
}
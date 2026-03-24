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

package org.apache.spark.util

/**
 * 文件说明: Spark核心公共工具模块中的内存参数解析提取器，用于从字符串格式的内存配置中提取数值
 *
 * 提取器对象，用于解析JVM内存格式字符串（如"10g"），将其转换为以MB为单位的整数，
 * 支持的格式与Utils.memoryStringToMb保持一致，主要用于Spark配置参数的模式匹配解析。
 */
private[spark] object MemoryParam {
  /**
   * 提取器方法，用于模式匹配中解析内存参数字符串
   * @param str 输入的内存格式字符串，如"1g"、"1024m"
   * @return 解析成功返回Some(MB为单位的内存值)，解析失败返回None
   */
  def unapply(str: String): Option[Int] = {
    try {
      Some(Utils.memoryStringToMb(str))
    } catch {
      case e: NumberFormatException => None
    }
  }
}
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
 * 文件说明：整数参数提取工具类，用于从字符串中安全提取整数，供Spark内部配置解析使用
 * 
 * 提取器对象，用于将字符串解析为整数，支持模式匹配，解析失败时返回空
 */
private[spark] object IntParam {
  /**
   * 提取器方法，用于尝试将输入字符串解析为整数
   * @param str 待解析的输入字符串
   * @return 解析成功返回Some(整数结果)，解析失败返回None
   */
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}
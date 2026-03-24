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

package org.apache.spark.ui

import java.util.UUID

/**
 * 内容安全策略(CSP)随机数的线程本地存储工具类
 * 
 * 用于为每个Web请求生成唯一的CSP随机数，在Spark UI页面中用于标记信任的内联脚本和样式，
 * 符合内容安全策略规范，防止跨站脚本攻击(XSS)。每个请求对应一个线程，通过ThreadLocal隔离不同请求的随机数。
 */
private[spark] object CspNonce {

  // 线程本地存储当前请求的CSP随机数
  private val nonce = new ThreadLocal[String]

  /**
   * 生成新的随机数并存储到当前线程，返回生成的随机数
   * @return 生成的UUID格式随机字符串
   */
  def generate(): String = {
    val value = UUID.randomUUID().toString
    nonce.set(value)
    value
  }

  /**
   * 获取当前请求对应的CSP随机数
   * @return 当前线程存储的随机数
   */
  def get: String = nonce.get()

  /**
   * 请求完成后清除当前线程存储的随机数，避免内存泄漏
   */
  def clear(): Unit = nonce.remove()
}
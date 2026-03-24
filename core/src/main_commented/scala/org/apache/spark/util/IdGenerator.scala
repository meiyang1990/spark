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

import java.util.concurrent.atomic.AtomicInteger

/**
 * 文件级：Spark核心公共工具模块下的全局唯一ID生成器工具类
 * 用于生成全局唯一的递增ID，基于Java AtomicInteger实现线程安全的ID生成
 * 典型使用场景包括为RpcEndpoint等组件生成唯一名称标识，如BlockManager中的端点命名
 */
private[spark] class IdGenerator {
  /** 线程安全的原子计数器，维护当前ID的最新值 */
  private val id = new AtomicInteger
  /**
   * 获取下一个全局唯一ID
   * @return 递增后的唯一整数ID
   */
  def next: Int = id.incrementAndGet
}
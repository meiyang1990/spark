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

package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.SparkConf

/**
 * Spark 中所有广播实现的通用接口（支持多种广播实现）。
 * SparkContext 使用 BroadcastFactory 实现来为整个 Spark 作业创建特定的广播变量实例。
 */
private[spark] trait BroadcastFactory {

  /**
   * 初始化广播工厂。由 SparkContext 在启动时调用
   * @param isDriver 是否运行在 driver 端
   * @param conf Spark 配置对象
   */
  def initialize(isDriver: Boolean, conf: SparkConf): Unit

  /**
   * 创建新的广播变量。
   *
   * @param value 要广播的值
   * @param isLocal 是否运行在本地模式（单 JVM 进程）
   * @param id 代表该广播变量的唯一 ID
   * @param serializedOnly 如果为 true，不在 driver 上缓存未序列化的值
   * @return `Broadcast` 对象，一个在每台机器上缓存的只读变量
   */
  def newBroadcast[T: ClassTag](
      value: T,
      isLocal: Boolean,
      id: Long,
      serializedOnly: Boolean = false): Broadcast[T]

  /**
   * 删除指定 ID 的广播变量的所有持久化状态
   * @param id 广播变量 ID
   * @param removeFromDriver 是否从 driver 上删除状态
   * @param blocking 是否阻塞直到完全删除
   */
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit

  /** 停止广播工厂，释放相关资源 */
  def stop(): Unit
}

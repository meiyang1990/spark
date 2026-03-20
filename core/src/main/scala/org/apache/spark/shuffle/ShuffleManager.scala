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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle

import java.util.Locale

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.config
import org.apache.spark.util.Utils

/**
 * Shuffle 系统的可插拔接口。
 * ShuffleManager 在 Driver 和每个 Executor 的 SparkEnv 中创建，基于 spark.shuffle.manager 配置。
 * Driver 向其注册 Shuffle，Executor（或在 Driver 本地运行的任务）可以请求读写数据。
 *
 * 注意：
 * 1. 由 SparkEnv 实例化，构造函数可接受 SparkConf 和 boolean isDriver 参数
 * 2. 包含 ShuffleBlockResolver 方法，与 External Shuffle Service 交互。
 *    实现自定义 ShuffleManager 时需确保能与 External Shuffle Service 共存
 */
private[spark] trait ShuffleManager {

  /**
   * 向管理器注册 Shuffle 并获取一个句柄用于传递给任务。
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /**
   * 获取指定分区的写入器。
   * 在 Executor 上由 Map 任务调用。
   */
  def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]


  /**
   * 获取读取指定范围 Reduce 分区（startPartition 到 endPartition-1）的读取器，
   * 从该 Shuffle 的所有 Map 输出中读取。
   *
   * 在 Executor 上由 Reduce 任务调用。
   */
  final def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReader(handle, 0, Int.MaxValue, startPartition, endPartition, context, metrics)
  }

  /**
   * 获取读取指定范围 Reduce 分区（startPartition 到 endPartition-1）的读取器，
   * 从指定范围的 Map 输出（startMapIndex 到 endMapIndex-1）中读取。
   * 如果 endMapIndex=Int.MaxValue，实际值将在 getMapSizesByExecutorId 中替换为总 Map 输出数。
   *
   * 在 Executor 上由 Reduce 任务调用。
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * 从 ShuffleManager 中移除 Shuffle 的元数据。
   *
   * @return 成功移除返回 true，否则返回 false
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * 返回能够根据块坐标检索 Shuffle 数据块的解析器。
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** 关闭此 ShuffleManager */
  def stop(): Unit
}

/**
 * 用于根据 Spark 配置创建 ShuffleManager 的工具伴生对象。
 */
private[spark] object ShuffleManager {
  def create(conf: SparkConf, isDriver: Boolean): ShuffleManager = {
    Utils.instantiateSerializerOrShuffleManager[ShuffleManager](
      getShuffleManagerClassName(conf), conf, isDriver)
  }

  def getShuffleManagerClassName(conf: SparkConf): String = {
    // 简短名称映射到实际类名
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)

    val shuffleMgrName = conf.get(config.SHUFFLE_MANAGER)
    shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
  }
}


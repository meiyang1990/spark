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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleWriter}
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.checksum.RowBasedChecksum
import org.apache.spark.util.collection.ExternalSorter

/**
 * 基于 ExternalSorter 的 ShuffleWriter 实现
 *
 * 这是 SortShuffleManager 的核心写入器，使用 ExternalSorter 对数据进行排序和溢写。
 * 
 * 主要工作流程：
 * 1. 创建 ExternalSorter，根据是否需要 map 端聚合选择不同配置
 * 2. 将所有记录插入 sorter（可能触发内存到磁盘的溢写）
 * 3. 将排序后的数据写入分区文件
 * 4. 返回 MapStatus 供 Driver 追踪输出位置
 *
 * @tparam K 键类型
 * @tparam V 值类型（未聚合时）
 * @tparam C 聚合后的值类型
 * @param handle Shuffle 句柄，包含 Shuffle 依赖信息
 * @param mapId Map 任务 ID
 * @param context 任务上下文
 * @param writeMetrics Shuffle 写入指标收集器
 * @param shuffleExecutorComponents Shuffle 执行器组件，用于创建 MapOutputWriter
 */
private[spark] class SortShuffleWriter[K, V, C](
    handle: BaseShuffleHandle[K, V, C],
    mapId: Long,
    context: TaskContext,
    writeMetrics: ShuffleWriteMetricsReporter,
    shuffleExecutorComponents: ShuffleExecutorComponents)
  extends ShuffleWriter[K, V] with Logging {

  /** Shuffle 依赖，包含分区器、序列化器、聚合器等信息 */
  private val dep = handle.dependency

  /** BlockManager 用于获取 shuffle 服务地址 */
  private val blockManager = SparkEnv.get.blockManager

  /** 
   * 外部排序器，核心组件
   * 负责在内存中排序、必要时溢写到磁盘、最终合并输出
   */
  private var sorter: ExternalSorter[K, V, _] = null

  /**
   * 是否正在停止中
   * 
   * 由于 map 任务可能先调用 stop(success=true)，
   * 然后因异常再调用 stop(success=false)，
   * 此标志防止重复删除文件等操作。
   */
  private var stopping = false

  /** Map 任务状态，包含输出位置和各分区大小 */
  private var mapStatus: MapStatus = null

  /** 各分区数据长度，用于 MapStatus */
  private var partitionLengths: Array[Long] = _

  /**
   * 获取基于行的校验和数组
   * 用于数据完整性校验，每个分区一个校验和对象
   */
  def getRowBasedChecksums: Array[RowBasedChecksum] = {
    if (sorter != null) {
      sorter.getRowBasedChecksums
    } else {
      ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS
    }
  }

  /** 获取聚合校验值，用于快速校验整体数据完整性 */
  def getAggregatedChecksumValue: Long = {
    if (sorter != null) sorter.getAggregatedChecksumValue else 0
  }

  /**
   * 将记录写入 shuffle 输出
   * 
   * 核心流程：
   * 1. 根据是否需要 map 端聚合创建不同配置的 ExternalSorter
   *    - 需要 map 端聚合：使用 aggregator 和 keyOrdering
   *    - 不需要：只做分区，不排序（reduce 端会排序）
   * 2. 将所有记录插入 sorter（可能触发溢写）
   * 3. 创建 MapOutputWriter 写入分区数据
   * 4. 提交所有分区并获取分区长度
   * 5. 创建 MapStatus 记录输出位置
   */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      // 需要 map 端聚合时，传入 aggregator 和 keyOrdering
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering,
        dep.serializer, dep.rowBasedChecksums)
    } else {
      // 不需要 map 端聚合时，只做分区，不排序
      // 如果是 sortByKey 操作，排序会在 reduce 端进行
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None,
        dep.serializer, dep.rowBasedChecksums)
    }
    sorter.insertAll(records)

    // 创建 MapOutputWriter，不记录打开文件的时间
    // 因为打开单个文件通常太快无法准确测量（参见 SPARK-3570）
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      dep.shuffleId, mapId, dep.partitioner.numPartitions)
    sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter, writeMetrics)
    partitionLengths = mapOutputWriter.commitAllPartitions(sorter.getChecksums).getPartitionLengths
    mapStatus =
      MapStatus(blockManager.shuffleServerId, partitionLengths, mapId, getAggregatedChecksumValue)
  }

  /**
   * 关闭写入器
   * 
   * @param success map 任务是否成功完成
   * @return 成功时返回 Some(MapStatus)，失败时返回 None
   */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      // 清理 sorter，它可能有自己的中间文件
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }

  /** 获取各分区数据长度 */
  override def getPartitionLengths(): Array[Long] = partitionLengths
}

/**
 * SortShuffleWriter 伴生对象
 * 
 * 提供判断是否可以绕过 merge-sort 的工具方法
 */
private[spark] object SortShuffleWriter {
  /**
   * 判断是否应该绕过 merge-sort，直接写入单独的分区文件
   * 
   * 当分区数较少且不需要 map 端聚合时，可以直接为每个分区创建单独的缓冲区，
   * 避免排序和合并的开销。
   * 
   * @param conf Spark 配置
   * @param dep Shuffle 依赖
   * @return true 表示可以绕过 merge-sort
   */
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // 如果需要 map 端聚合，不能绕过排序
    if (dep.mapSideCombine) {
      false
    } else {
      // 分区数小于阈值时可以绕过
      val bypassMergeThreshold: Int = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}

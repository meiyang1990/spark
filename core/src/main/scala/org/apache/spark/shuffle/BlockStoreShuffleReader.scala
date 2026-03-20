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

import scala.collection

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * 从其他节点的 BlockStore 请求并读取 Shuffle 数据块。
 * 这是 Shuffle 读取端的核心实现，负责从远程节点拉取数据并进行聚合/排序。
 *
 * @param handle Shuffle 句柄，包含 Shuffle 的元信息
 * @param blocksByAddress 需要获取的数据块位置信息（BlockManagerId -> 数据块列表）
 * @param context 任务上下文
 * @param readMetrics Shuffle 读取指标报告器
 * @param serializerManager 序列化管理器
 * @param blockManager 块管理器
 * @param mapOutputTracker Map 输出跟踪器
 * @param shouldBatchFetch 是否启用批量获取连续块
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /**
   * 判断是否可以批量获取连续的 Shuffle 数据块。
   * 批量获取可以减少网络请求次数，提高性能，但需要满足以下条件：
   * - 序列化器支持重定位（支持合并序列化数据）
   * - 如果启用了压缩，压缩编解码器必须支持流拼接
   * - 不使用旧的 Shuffle 获取协议
   * - 未启用 IO 加密
   */
  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)
    // SPARK-34790: 批量获取连续块与 IO 加密不兼容
    val ioEncryption = conf.get(config.IO_ENCRYPTION_ENABLED)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol && !ioEncryption
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug("The feature tag of continuous shuffle block fetching is set to true, but " +
        "we can not enable the feature because other conditions are not satisfied. " +
        s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
        s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
        s"$useOldFetchProtocol, io encryption: $ioEncryption.")
    }
    doBatchFetch
  }

  /**
   * 读取此 Reduce 任务的所有键值对数据。
   * 
   * 读取流程：
   * 1. 通过 ShuffleBlockFetcherIterator 从远程节点拉取数据块
   * 2. 反序列化数据流为键值对迭代器
   * 3. 根据 ShuffleDependency 的配置进行聚合和/或排序
   * 4. 返回可中断的迭代器以支持任务取消
   */
  override def read(): Iterator[Product2[K, C]] = {
    // 创建数据块获取迭代器，负责从远程节点拉取 Shuffle 数据块
    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,
      // 注意：为了向后兼容，无后缀时使用 getSizeAsMb
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator

    val serializerInstance = dep.serializer.newInstance()

    // 将每个数据流反序列化为键值对迭代器
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // asKeyValueIterator 将键值对迭代器包装在 NextIterator 中，
      // 确保所有记录读取完毕后调用底层 InputStream 的 close()
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // 为每条记录更新任务指标，完成后合并 Shuffle 读取指标
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // 使用可中断迭代器以支持任务取消
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val resultIter: Iterator[Product2[K, C]] = {
      // 如果定义了键排序，则对输出进行排序
      if (dep.keyOrdering.isDefined) {
        // 创建 ExternalSorter 对数据进行排序
        val sorter: ExternalSorter[K, _, C] = if (dep.aggregator.isDefined) {
          if (dep.mapSideCombine) {
            // Map 端已预聚合，使用 combineCombiners 进行 Reduce 端聚合
            new ExternalSorter[K, C, C](context,
              Option(new Aggregator[K, C, C](identity,
                dep.aggregator.get.mergeCombiners,
                dep.aggregator.get.mergeCombiners)),
              ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
          } else {
            // Map 端未预聚合，使用完整的聚合器
            new ExternalSorter[K, Nothing, C](context,
              dep.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]],
              ordering = Some(dep.keyOrdering.get), serializer = dep.serializer)
          }
        } else {
          // 无聚合器，仅排序
          new ExternalSorter[K, C, C](context, ordering = Some(dep.keyOrdering.get),
            serializer = dep.serializer)
        }
        sorter.insertAllAndUpdateMetrics(interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]])
      } else if (dep.aggregator.isDefined) {
        // 无需排序，但有聚合器
        if (dep.mapSideCombine) {
          // Map 端已预聚合，使用 combineCombinersByKey 合并
          val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
          dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
        } else {
          // Map 端未预聚合，使用 combineValuesByKey 聚合
          val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
          dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
        }
      } else {
        // 既无排序也无聚合，直接返回原始迭代器
        interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      }
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // 如果聚合器或排序器消耗了之前的可中断迭代器，
        // 需要再次包装以确保支持任务取消
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}

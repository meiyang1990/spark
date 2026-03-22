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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading shuffle data.
 * Operations are not thread-safe.
 *
 * Shuffle 读取指标容器，用于收集和存储 Task 在 Shuffle Read 阶段的各项性能指标。
 *
 * 在 Spark 的 Shuffle 过程中，Map 端将数据按分区写出（Shuffle Write），
 * Reduce 端需要从各个 Map 输出中拉取属于自己分区的数据（Shuffle Read）。
 * 本类通过累加器收集各类指标，最终汇报给 Driver 用于 UI 展示和性能分析。
 *
 * 收集的指标涵盖：
 * - 本地/远程拉取的数据块数量和字节数
 * - 等待拉取数据的阻塞时间
 * - 读取的总记录数
 * - Push-based Shuffle 优化相关的合并块指标
 *
 * 注意：本类操作非线程安全，每个 Task 独立持有自己的实例。
 */
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable {
  // 记录从远程 Executor 拉取的数据块数量
  private[executor] val _remoteBlocksFetched = new LongAccumulator
  // 记录从本地（同一 Executor）读取的数据块数量
  private[executor] val _localBlocksFetched = new LongAccumulator
  // 记录从远程读取的总字节数
  private[executor] val _remoteBytesRead = new LongAccumulator
  // 记录内存不足时写入磁盘的远程读取字节数
  private[executor] val _remoteBytesReadToDisk = new LongAccumulator
  // 记录从本地磁盘读取的字节数
  private[executor] val _localBytesRead = new LongAccumulator
  // 记录等待拉取数据的阻塞总时间（毫秒）
  private[executor] val _fetchWaitTime = new LongAccumulator
  // 记录读取的总记录数
  private[executor] val _recordsRead = new LongAccumulator

  // 记录损坏的合并块分片数量（Push-based Shuffle 专用）
  private[executor] val _corruptMergedBlockChunks = new LongAccumulator
  // 记录合并块读取失败回退到原始块的次数（Push-based Shuffle 专用）
  private[executor] val _mergedFetchFallbackCount = new LongAccumulator
  // 记录从远程拉取的合并块数量（Push-based Shuffle 专用）
  private[executor] val _remoteMergedBlocksFetched = new LongAccumulator
  // 记录从本地读取的合并块数量（Push-based Shuffle 专用）
  private[executor] val _localMergedBlocksFetched = new LongAccumulator
  // 记录从远程拉取的合并块分片数量（Push-based Shuffle 专用）
  private[executor] val _remoteMergedChunksFetched = new LongAccumulator
  // 记录从本地读取的合并块分片数量（Push-based Shuffle 专用）
  private[executor] val _localMergedChunksFetched = new LongAccumulator
  // 记录从远程读取的合并块总字节数（Push-based Shuffle 专用）
  private[executor] val _remoteMergedBytesRead = new LongAccumulator
  // 记录从本地读取的合并块总字节数（Push-based Shuffle 专用）
  private[executor] val _localMergedBytesRead = new LongAccumulator
  // 记录远程普通请求的总耗时（Push-based Shuffle 专用）
  private[executor] val _remoteReqsDuration = new LongAccumulator
  // 记录远程合并块请求的总耗时（Push-based Shuffle 专用）
  private[executor] val _remoteMergedReqsDuration = new LongAccumulator

  /**
   * 获取当前 Task 拉取的远程数据块总数
   */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.sum

  /**
   * 获取当前 Task 读取的本地数据块总数
   */
  def localBlocksFetched: Long = _localBlocksFetched.sum

  /**
   * 获取当前 Task 从远程读取的总字节数
   */
  def remoteBytesRead: Long = _remoteBytesRead.sum

  /**
   * 获取当前 Task 远程读取并写入磁盘的总字节数
   */
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk.sum

  /**
   * 获取当前 Task 从本地磁盘读取的总字节数
   */
  def localBytesRead: Long = _localBytesRead.sum

  /**
   * 获取当前 Task 等待拉取远程 Shuffle 块的总阻塞时间
   */
  def fetchWaitTime: Long = _fetchWaitTime.sum

  /**
   * 获取当前 Task 从 Shuffle 读取的总记录数
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * 获取当前 Task 读取的总字节数（包含本地和远程）
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * 获取当前 Task 读取的总数据块数（包含本地和远程）
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  /**
   * 获取当前 Task 遇到的损坏合并块分片总数
   */
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks.sum

  /**
   * 获取当前 Task 合并块拉取回退的总次数
   */
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount.sum

  /**
   * 获取当前 Task 拉取的远程合并块总数
   */
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched.sum

  /**
   * 获取当前 Task 读取的本地合并块总数
   */
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched.sum

  /**
   * 获取当前 Task 拉取的远程合并块分片总数
   */
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched.sum

  /**
   * 获取当前 Task 读取的本地合并块分片总数
   */
  def localMergedChunksFetched: Long = _localMergedChunksFetched.sum

  /**
   * 获取当前 Task 从远程读取的合并块总字节数
   */
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead.sum

  /**
   * 获取当前 Task 从本地读取的合并块总字节数
   */
  def localMergedBytesRead: Long = _localMergedBytesRead.sum

  /**
   * 获取当前 Task 远程普通请求的总耗时（不含合并块请求）
   */
  def remoteReqsDuration: Long = _remoteReqsDuration.sum

  /**
   * 获取当前 Task 远程合并块请求的总耗时
   */
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration.sum

  /** 增加远程拉取数据块计数 */
  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  /** 增加本地读取数据块计数 */
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  /** 增加远程读取字节数 */
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  /** 增加远程读取写入磁盘字节数 */
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.add(v)
  /** 增加本地读取字节数 */
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  /** 增加拉取等待时间 */
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  /** 增加读取记录数 */
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  /** 增加损坏合并块分片计数 */
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks.add(v)
  /** 增加合并块拉取回退计数 */
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount.add(v)
  /** 增加远程拉取合并块计数 */
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched.add(v)
  /** 增加本地读取合并块计数 */
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched.add(v)
  /** 增加远程拉取合并块分片计数 */
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched.add(v)
  /** 增加本地读取合并块分片计数 */
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched.add(v)
  /** 增加远程读取合并块字节数 */
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit =
    _remoteMergedBytesRead.add(v)
  /** 增加本地读取合并块字节数 */
  private[spark] def incLocalMergedBytesRead(v: Long): Unit =
    _localMergedBytesRead.add(v)
  /** 增加远程普通请求耗时 */
  private[spark] def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.add(v)
  /** 增加远程合并块请求耗时 */
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration.add(v)

  /** 设置远程拉取数据块计数 */
  private[spark] def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
  /** 设置本地读取数据块计数 */
  private[spark] def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
  /** 设置远程读取字节数 */
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  /** 设置远程读取写入磁盘字节数 */
  private[spark] def setRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.setValue(v)
  /** 设置本地读取字节数 */
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  /** 设置拉取等待时间 */
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  /** 设置读取记录数 */
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
  /** 设置损坏合并块分片计数 */
  private[spark] def setCorruptMergedBlockChunks(v: Long): Unit =
    _corruptMergedBlockChunks.setValue(v)
  /** 设置合并块拉取回退计数 */
  private[spark] def setMergedFetchFallbackCount(v: Long): Unit =
    _mergedFetchFallbackCount.setValue(v)
  /** 设置远程拉取合并块计数 */
  private[spark] def setRemoteMergedBlocksFetched(v: Long): Unit =
    _remoteMergedBlocksFetched.setValue(v)
  /** 设置本地读取合并块计数 */
  private[spark] def setLocalMergedBlocksFetched(v: Long): Unit =
    _localMergedBlocksFetched.setValue(v)
  /** 设置远程拉取合并块分片计数 */
  private[spark] def setRemoteMergedChunksFetched(v: Long): Unit =
    _remoteMergedChunksFetched.setValue(v)
  /** 设置本地读取合并块分片计数 */
  private[spark] def setLocalMergedChunksFetched(v: Long): Unit =
    _localMergedChunksFetched.setValue(v)
  /** 设置远程读取合并块字节数 */
  private[spark] def setRemoteMergedBytesRead(v: Long): Unit =
    _remoteMergedBytesRead.setValue(v)
  /** 设置本地读取合并块字节数 */
  private[spark] def setLocalMergedBytesRead(v: Long): Unit =
    _localMergedBytesRead.setValue(v)
  /** 设置远程普通请求总耗时 */
  private[spark] def setRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.setValue(v)
  /** 设置远程合并块请求总耗时 */
  private[spark] def setRemoteMergedReqsDuration(v: Long): Unit =
    _remoteMergedReqsDuration.setValue(v)

  /**
   * 重置当前指标并合并所有临时 Shuffle Read 指标。
   * 一个 Task 可能存在多个 Shuffle 依赖，每个依赖独立收集临时指标，最终合并到本实例中统一汇报。
   */
  private[spark] def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit = {
    // 重置所有累加器为0
    _remoteBlocksFetched.setValue(0)
    _localBlocksFetched.setValue(0)
    _remoteBytesRead.setValue(0)
    _remoteBytesReadToDisk.setValue(0)
    _localBytesRead.setValue(0)
    _fetchWaitTime.setValue(0)
    _recordsRead.setValue(0)
    _corruptMergedBlockChunks.setValue(0)
    _mergedFetchFallbackCount.setValue(0)
    _remoteMergedBlocksFetched.setValue(0)
    _localMergedBlocksFetched.setValue(0)
    _remoteMergedChunksFetched.setValue(0)
    _localMergedChunksFetched.setValue(0)
    _remoteMergedBytesRead.setValue(0)
    _localMergedBytesRead.setValue(0)
    _remoteReqsDuration.setValue(0)
    _remoteMergedReqsDuration.setValue(0)
    // 遍历所有临时指标，累加合并到当前实例
    metrics.foreach { metric =>
      _remoteBlocksFetched.add(metric.remoteBlocksFetched)
      _localBlocksFetched.add(metric.localBlocksFetched)
      _remoteBytesRead.add(metric.remoteBytesRead)
      _remoteBytesReadToDisk.add(metric.remoteBytesReadToDisk)
      _localBytesRead.add(metric.localBytesRead)
      _fetchWaitTime.add(metric.fetchWaitTime)
      _recordsRead.add(metric.recordsRead)
      _corruptMergedBlockChunks.add(metric.corruptMergedBlockChunks)
      _mergedFetchFallbackCount.add(metric.mergedFetchFallbackCount)
      _remoteMergedBlocksFetched.add(metric.remoteMergedBlocksFetched)
      _localMergedBlocksFetched.add(metric.localMergedBlocksFetched)
      _remoteMergedChunksFetched.add(metric.remoteMergedChunksFetched)
      _localMergedChunksFetched.add(metric.localMergedChunksFetched)
      _remoteMergedBytesRead.add(metric.remoteMergedBytesRead)
      _localMergedBytesRead.add(metric.localMergedBytesRead)
      _remoteReqsDuration.add(metric.remoteReqsDuration)
      _remoteMergedReqsDuration.add(metric.remoteMergedReqsDuration)
    }
  }
}


/**
 * 单个 Shuffle 依赖的临时 Shuffle Read 指标收集器。
 *
 * 设计说明：一个 Task 可能依赖多个 Shuffle，每个 Shuffle 的 Reader 独立更新指标。
 * 为避免并发更新累加器的线程安全问题，每个 Shuffle 先使用本类收集指标，
 * Task 完成后再将所有临时指标统一合并到最终的 ShuffleReadMetrics 中。
 * 实现 ShuffleReadMetricsReporter 接口，供 ShuffleReader 调用更新。
 */
private[spark] class TempShuffleReadMetrics extends ShuffleReadMetricsReporter {
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _localBlocksFetched = 0L
  private[this] var _remoteBytesRead =
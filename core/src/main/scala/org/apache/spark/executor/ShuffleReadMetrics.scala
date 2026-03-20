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
 * Shuffle 读取指标类，用于收集和汇报 Task 在 Shuffle Read 阶段的各项性能数据。
 *
 * 在 Spark 的 Shuffle 过程中，Map 端将数据按 Partition 写出（Shuffle Write），
 * 而 Reduce 端需要从各个 Map 输出中拉取属于自己分区的数据（Shuffle Read）。
 * 本类记录的指标包括：
 * - 拉取的数据块数量（本地/远程）
 * - 拉取的字节数（本地/远程）
 * - 等待拉取的时间
 * - 读取的记录数
 * - Push-based Shuffle 相关的合并块指标
 *
 * 这些指标通过 Accumulator 机制收集，在 Task 完成或 Executor 心跳时汇报给 Driver，
 * 用于 Spark UI 展示和性能调优分析。
 *
 * 注意：本类的操作非线程安全，每个 Task 独立持有自己的实例。
 */
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable {
  // ==================== 基础 Shuffle Read 指标 ====================
  // 记录从远程 Executor 拉取的数据块数量
  private[executor] val _remoteBlocksFetched = new LongAccumulator
  // 记录从本地（同一 Executor）读取的数据块数量
  private[executor] val _localBlocksFetched = new LongAccumulator
  // 记录从远程读取的总字节数
  private[executor] val _remoteBytesRead = new LongAccumulator
  // 记录远程数据落盘的字节数（当内存不足时，拉取的数据会先写入磁盘）
  private[executor] val _remoteBytesReadToDisk = new LongAccumulator
  // 记录从本地磁盘读取的字节数
  private[executor] val _localBytesRead = new LongAccumulator
  // 记录等待拉取数据的阻塞时间（毫秒），是网络延迟的重要指标
  private[executor] val _fetchWaitTime = new LongAccumulator
  // 记录读取的总记录数
  private[executor] val _recordsRead = new LongAccumulator

  // ==================== Push-based Shuffle 相关指标 ====================
  // 以下指标用于 Spark 3.2+ 引入的 Push-based Shuffle 优化
  // 该优化允许 Map 端主动将数据推送到远程节点进行预合并，减少 Reduce 端的拉取开销

  // 遇到的损坏合并块分片数量
  private[executor] val _corruptMergedBlockChunks = new LongAccumulator
  // 合并块读取失败后回退到原始块的次数
  private[executor] val _mergedFetchFallbackCount = new LongAccumulator
  // 从远程拉取的合并块数量
  private[executor] val _remoteMergedBlocksFetched = new LongAccumulator
  // 从本地读取的合并块数量
  private[executor] val _localMergedBlocksFetched = new LongAccumulator
  // 从远程拉取的合并块分片数量
  private[executor] val _remoteMergedChunksFetched = new LongAccumulator
  // 从本地读取的合并块分片数量
  private[executor] val _localMergedChunksFetched = new LongAccumulator
  // 从远程读取的合并块总字节数
  private[executor] val _remoteMergedBytesRead = new LongAccumulator
  // 从本地读取的合并块总字节数
  private[executor] val _localMergedBytesRead = new LongAccumulator
  // 远程普通请求的总耗时
  private[executor] val _remoteReqsDuration = new LongAccumulator
  // 远程合并块请求的总耗时
  private[executor] val _remoteMergedReqsDuration = new LongAccumulator

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.sum

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Long = _localBlocksFetched.sum

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.sum

  /**
   * Total number of remotes bytes read to disk from the shuffle by this task.
   */
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk.sum

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.sum

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.sum

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  /**
   * Number of corrupt merged shuffle block chunks encountered by this task (remote or local).
   */
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks.sum

  /**
   * Number of times the task had to fallback to fetch original shuffle blocks for a merged
   * shuffle block chunk (remote or local).
   */
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount.sum

  /**
   * Number of remote merged blocks fetched.
   */
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched.sum

  /**
   * Number of local merged blocks fetched.
   */
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched.sum

  /**
   * Number of remote merged chunks fetched.
   */
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched.sum

  /**
   * Number of local merged chunks fetched.
   */
  def localMergedChunksFetched: Long = _localMergedChunksFetched.sum

  /**
   * Total number of remote merged bytes read.
   */
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead.sum

  /**
   * Total number of local merged bytes read.
   */
  def localMergedBytesRead: Long = _localMergedBytesRead.sum

  /**
   * Total time taken for remote requests to complete by this task. This doesn't include
   * duration of remote merged requests.
   */
  def remoteReqsDuration: Long = _remoteReqsDuration.sum

  /**
   * Total time taken for remote merged requests.
   */
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration.sum

  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks.add(v)
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount.add(v)
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched.add(v)
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched.add(v)
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched.add(v)
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched.add(v)
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit =
    _remoteMergedBytesRead.add(v)
  private[spark] def incLocalMergedBytesRead(v: Long): Unit =
    _localMergedBytesRead.add(v)
  private[spark] def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.add(v)
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration.add(v)

  private[spark] def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
  private[spark] def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  private[spark] def setRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.setValue(v)
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
  private[spark] def setCorruptMergedBlockChunks(v: Long): Unit =
    _corruptMergedBlockChunks.setValue(v)
  private[spark] def setMergedFetchFallbackCount(v: Long): Unit =
    _mergedFetchFallbackCount.setValue(v)
  private[spark] def setRemoteMergedBlocksFetched(v: Long): Unit =
    _remoteMergedBlocksFetched.setValue(v)
  private[spark] def setLocalMergedBlocksFetched(v: Long): Unit =
    _localMergedBlocksFetched.setValue(v)
  private[spark] def setRemoteMergedChunksFetched(v: Long): Unit =
    _remoteMergedChunksFetched.setValue(v)
  private[spark] def setLocalMergedChunksFetched(v: Long): Unit =
    _localMergedChunksFetched.setValue(v)
  private[spark] def setRemoteMergedBytesRead(v: Long): Unit =
    _remoteMergedBytesRead.setValue(v)
  private[spark] def setLocalMergedBytesRead(v: Long): Unit =
    _localMergedBytesRead.setValue(v)
  private[spark] def setRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration.setValue(v)
  private[spark] def setRemoteMergedReqsDuration(v: Long): Unit =
    _remoteMergedReqsDuration.setValue(v)

  /**
   * Resets the value of the current metrics (`this`) and merges all the independent
   * [[TempShuffleReadMetrics]] into `this`.
   *
   * 重置当前指标并合并所有临时指标。
   * 一个 Task 可能有多个 Shuffle 依赖，每个依赖使用独立的 TempShuffleReadMetrics 收集数据，
   * 在 Task 完成或心跳汇报时，调用此方法将所有临时指标合并到最终的 ShuffleReadMetrics 中。
   */
  private[spark] def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit = {
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
 * A temporary shuffle read metrics holder that is used to collect shuffle read metrics for each
 * shuffle dependency, and all temporary metrics will be merged into the [[ShuffleReadMetrics]] at
 * last.
 *
 * 临时 Shuffle Read 指标收集器。
 *
 * 设计背景：
 * 一个 Task 可能依赖多个 Shuffle（即有多个 ShuffleDependency），每个 Shuffle 依赖
 * 都有独立的 ShuffleReader。为了避免多个 Reader 在不同线程中并发更新同一个
 * ShuffleReadMetrics 导致的线程安全问题，Spark 采用了"先分后合"的策略：
 *
 * 1. 每个 ShuffleReader 持有一个独立的 TempShuffleReadMetrics 实例
 * 2. Reader 在读取过程中更新自己的临时指标（无需同步）
 * 3. Task 完成时，调用 TaskMetrics.mergeShuffleReadMetrics() 将所有临时指标
 *    合并到最终的 ShuffleReadMetrics 中
 *
 * 本类实现 ShuffleReadMetricsReporter 接口，供 ShuffleReader 调用增量方法更新指标。
 */
private[spark] class TempShuffleReadMetrics extends ShuffleReadMetricsReporter {
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _localBlocksFetched = 0L
  private[this] var _remoteBytesRead = 0L
  private[this] var _remoteBytesReadToDisk = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L
  private[this] var _corruptMergedBlockChunks = 0L
  private[this] var _mergedFetchFallbackCount = 0L
  private[this] var _remoteMergedBlocksFetched = 0L
  private[this] var _localMergedBlocksFetched = 0L
  private[this] var _remoteMergedChunksFetched = 0L
  private[this] var _localMergedChunksFetched = 0L
  private[this] var _remoteMergedBytesRead = 0L
  private[this] var _localMergedBytesRead = 0L
  private[this] var _remoteReqsDuration = 0L
  private[this] var _remoteMergedReqsDuration = 0L

  override def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
  override def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched += v
  override def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  override def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk += v
  override def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  override def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  override def incRecordsRead(v: Long): Unit = _recordsRead += v
  override def incCorruptMergedBlockChunks(v: Long): Unit = _corruptMergedBlockChunks += v
  override def incMergedFetchFallbackCount(v: Long): Unit = _mergedFetchFallbackCount += v
  override def incRemoteMergedBlocksFetched(v: Long): Unit = _remoteMergedBlocksFetched += v
  override def incLocalMergedBlocksFetched(v: Long): Unit = _localMergedBlocksFetched += v
  override def incRemoteMergedChunksFetched(v: Long): Unit = _remoteMergedChunksFetched += v
  override def incLocalMergedChunksFetched(v: Long): Unit = _localMergedChunksFetched += v
  override def incRemoteMergedBytesRead(v: Long): Unit = _remoteMergedBytesRead += v
  override def incLocalMergedBytesRead(v: Long): Unit = _localMergedBytesRead += v
  override def incRemoteReqsDuration(v: Long): Unit = _remoteReqsDuration += v
  override def incRemoteMergedReqsDuration(v: Long): Unit = _remoteMergedReqsDuration += v

  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def localBlocksFetched: Long = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead
  def corruptMergedBlockChunks: Long = _corruptMergedBlockChunks
  def mergedFetchFallbackCount: Long = _mergedFetchFallbackCount
  def remoteMergedBlocksFetched: Long = _remoteMergedBlocksFetched
  def localMergedBlocksFetched: Long = _localMergedBlocksFetched
  def remoteMergedChunksFetched: Long = _remoteMergedChunksFetched
  def localMergedChunksFetched: Long = _localMergedChunksFetched
  def remoteMergedBytesRead: Long = _remoteMergedBytesRead
  def localMergedBytesRead: Long = _localMergedBytesRead
  def remoteReqsDuration: Long = _remoteReqsDuration
  def remoteMergedReqsDuration: Long = _remoteMergedReqsDuration
}

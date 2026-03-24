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

package org.apache.spark.storage

import java.io.{InputStream, IOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CheckedInputStream
import javax.annotation.concurrent.GuardedBy

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.util.{Failure, Success}

import io.netty.util.internal.OutOfDirectMemoryError
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{MapOutputTracker, SparkException, TaskContext}
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.checksum.{Cause, ShuffleChecksumHelper}
import org.apache.spark.network.util.{NettyUtils, TransportConf}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.{Clock, CompletionIterator, SystemClock, TaskCompletionListener, Utils}

/**
 * Shuffle块拉取迭代器，用于在Shuffle读阶段从多个节点拉取指定Block数据。
 * 本地块从本地块管理器获取，远程块通过BlockTransferService异步拉取，实现了边拉取边处理的流水线模式。
 * 通过流量控制限制正在拉取的总字节数，避免使用过多内存。
 * 
 * @param context 任务上下文，用于更新度量信息
 * @param shuffleClient 用于拉取远程块的Shuffle客户端
 * @param blockManager 本地块管理器，用于读取本地块
 * @param mapOutputTracker Map输出跟踪器，推送式Shuffle拉取失败时用于回退到原始块拉取
 * @param blocksByAddress 按BlockManager地址分组的待拉取块列表，每个块包含大小和map索引，已排除零大小块
 * @param streamWrapper 用于包装返回输入流的函数
 * @param maxBytesInFlight 任意时刻允许正在拉取的最大字节数
 * @param maxReqsInFlight 任意时刻允许正在进行的最大请求数
 * @param maxBlocksInFlightPerAddress 单个远程地址同时拉取的最大块数量
 * @param maxReqSizeShuffleToMem 可以放在内存中的最大请求大小，超过则写入磁盘
 * @param maxAttemptsOnNettyOOM 因Netty直接内存不足重试的最大次数
 * @param detectCorrupt 是否检测拉取块的数据损坏
 * @param checksumEnabled 是否启用Shuffle校验和，启用后会诊断数据损坏原因
 * @param checksumAlgorithm 计算块校验和使用的算法
 * @param shuffleMetrics 用于报告Shuffle读度量
 * @param doBatchFetch 是否批量拉取同一executor上的连续Shuffle块
 * @param clock 时钟，用于计时
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    blockManager: BlockManager,
    mapOutputTracker: MapOutputTracker,
    blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    val maxReqSizeShuffleToMem: Long,
    maxAttemptsOnNettyOOM: Int,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    checksumEnabled: Boolean,
    checksumAlgorithm: String,
    shuffleMetrics: ShuffleReadMetricsReporter,
    doBatchFetch: Boolean,
  clock: Clock = new SystemClock())
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  // 将单个远程请求大小限制为maxBytesInFlight/5，以便支持同时从最多5个节点并行拉取
  // 避免被单个节点的大请求阻塞所有其他拉取
  private val targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)

  /**
   * 总共需要拉取的块数量
   */
  private[this] var numBlocksToFetch = 0

  /**
   * 调用方已处理完成的块数量，当numBlocksProcessed等于numBlocksToFetch时迭代器耗尽
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTimeNs = System.nanoTime()

  /** Host本地待拉取块，已排除零大小块 */
  private[this] val hostLocalBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()

  /**
   * 保存拉取结果的队列，将BlockTransferService提供的异步模型转换为迭代器同步模型
   */
  private[this] val results = new LinkedBlockingDeque[FetchResult]()

  /**
   * 当前正在处理的FetchResult，跟踪这个变量以便在处理当前缓冲区抛出运行时异常时能够释放缓冲区
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * 待发送拉取请求队列，逐步从队列中取出请求发送，确保正在传输的字节数不超过maxBytesInFlight限制
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * 第一次出队时无法发送的拉取请求队列，当满足拉取限制条件后会再次尝试处理这些请求
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** 当前正在传输的总字节数 */
  private[this] var bytesInFlight = 0L

  /** 当前正在进行的请求总数 */
  private[this] var reqsInFlight = 0

  /** 当前每个地址正在传输的块数量 */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * 因Netty OOM重试的计数，超过maxAttemptsOnNettyOOM后停止重试直接抛出异常
   */
  private[this] val blockOOMRetryCounts = new HashMap[String, Int]

  /**
   * 解压失败的损坏块集合，保证每个损坏块最多重试一次
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  /**
   * 迭代器是否仍然活跃，如果isZombie为true，回调接口不会再将拉取到的块放入结果队列
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * 存储用于拉取远程大块的临时文件集合，清理阶段会删除这些文件，防止磁盘泄漏
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  private[this] val onCompleteCallback = new ShuffleFetchCompletionListener(this)

  private[this] val pushBasedFetchHelper = new PushBasedFetchHelper(
    this, shuffleClient, blockManager, mapOutputTracker, shuffleMetrics)

  initialize()

  /**
   * 统计并累加拉取等待时间到Shuffle度量中
   */
  private def withFetchWaitTimeTracked[T](f: => T): T = {
    val startFetchWait = System.nanoTime()
    val res = f
    val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
    shuffleMetrics.incFetchWaitTime(fetchWaitTime)
    res
  }

  /**
   * 释放当前结果缓冲区，并清空currentResult防止清理时重复释放
   */
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // 加密解密在其他层处理，这里不需要处理，因为Shuffle数据写入时已经加密，网络传输也是加密状态
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * 将迭代器标记为zombie，释放所有尚未反序列化的缓冲区
   */
  private[storage] def cleanup(): Unit = {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // 释放结果队列中的所有缓冲区
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(blockId, mapIndex, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            if (pushBasedFetchHelper.isLocalPushMergedBlockAddress(address) ||
              hostLocalBlocks.contains(blockId -> mapIndex)) {
              shuffleMetricsUpdate(blockId, buf, local = true)
            } else {
              shuffleMetricsUpdate(blockId, buf, local = false)
            }
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning(log"Failed to cleanup shuffle fetch temp file ${MDC(PATH, file.path())}")
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest): Unit = {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // 为每个blockId创建块信息索引映射
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val deferredBlocks = new ArrayBuffer[String]()
    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address
    val requestStartTime = clock.nanoTime()

    @inline def enqueueDeferredFetchRequestIfNecessary(): Unit = {
      if (remainingBlocks.isEmpty && deferredBlocks.nonEmpty) {
        val blocks = deferredBlocks.map { blockId =>
          val (size, mapIndex) = infoMap(blockId)
          FetchBlockInfo(BlockId(blockId), size, mapIndex)
        }
        results.put(DeferFetchRequestResult(FetchRequest(address, blocks)))
        deferredBlocks.clear()
      }
    }

    @inline def updateMergedReqsDuration(wasReqForMergedChunks: Boolean = false): Unit = {
      if (remainingBlocks.isEmpty) {
        val durationMs = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - requestStartTime)
        if (wasReqForMergedChunks) {
          shuffleMetrics.incRemoteMergedReqsDuration(durationMs)
        }
        shuffleMetrics.incRemoteReqsDuration(durationMs)
      }
    }

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // 只有迭代器未被标记为zombie时才将结果加入队列
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // 增加引用计数，因为会传递给其他线程使用，使用后需要释放
            buf.retain()
            remainingBlocks -= blockId
            blockOOMRetryCounts.remove(blockId)
            updateMergedReqsDuration(BlockId(blockId).isShuffleChunk)
            results.put(SuccessFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
            enqueueDeferredFetchRequestIfNecessary()
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        ShuffleBlockFetcherIterator.this.synchronized {
          logError(log"Failed to get block(s) from " +
            log"${MDC(HOST, req.address.host)}:${MDC(PORT, req.address.port)}", e)
          e match {
            // 捕获Netty直接内存OOM错误，尽早设置全局标记，暂停后续拉取直到有足够空闲内存
            // 将同一请求中因OOM失败的块打包延迟重试，减少并发连接和远程服务器压力
            case _: OutOfDirectMemoryError
                if blockOOMRetryCounts.getOrElseUpdate(blockId, 0) < maxAttemptsOnNettyOOM =>
              if (!isZombie) {
                val failureTimes = blockOOMRetryCounts(blockId)
                blockOOMRetryCounts(blockId) += 1
                if (isNettyOOMOnShuffle.compareAndSet(false, true)) {
                  // OOM只日志一次，避免日志泛滥
                  logInfo(log"Block ${MDC(BLOCK_ID, blockId)} has failed " +
                    log"${MDC(FAILURES, failureTimes)} times due to Netty OOM, will retry")
                }
                remainingBlocks -= blockId
                deferredBlocks += blockId
                enqueueDeferredFetchRequestIfNecessary()
              }

            case _ =>
              val block = BlockId(blockId)
              if (block.isShuffleChunk) {
                remainingBlocks -= blockId
                updateMergedReqsDuration(wasReqForMergedChunks = true)
                results.put(FallbackOnPushMergedFailureResult(
                  block, address, infoMap(blockId)._1, remainingBlocks.isEmpty))
              } else {
                results.putFirst(FailureFetchResult(block, infoMap(blockId)._2, address, e))
              }
          }
        }
      }
    }

    // 当请求大小超过阈值时，直接将远程Shuffle块拉取到磁盘，避免占用过多内存
    // 数据已经在网络层加密压缩，可直接写入文件
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  /**
   * 按拉取模式分区块，分为本地、主机本地、推送合并本地、远程四类，为远程块构造拉取请求
   * 供初始化和推送合并块回退流程使用
   */
  private[this] def partitionBlocksByFetchMode(
      blocksByAddress: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])],
      localBlocks: mutable.LinkedHashSet[(BlockId, Int)],
      hostLocalBlocksByExecutor:
        mutable.LinkedHashMap[BlockManagerId, collection.Seq[(BlockId, Long, Int)]],
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): ArrayBuffer[FetchRequest] = {
    logDebug(s"maxBytesInFlight: $maxBytesInFlight, targetRemoteRequestSize: "
      + s"$targetRemoteRequestSize, maxBlocksInFlightPerAddress: $maxBlocksInFlightPerAddress")

    // 将块分区到不同拉取模式，远程块拆分到多个FetchRequest，限制每个请求大小避免超出内存限制
    val collectedRemoteRequests = new ArrayBuffer[FetchRequest]
    var localBlockBytes = 0L
    var hostLocalBlockBytes = 0L
    var numHostLocalBlocks = 0
    var pushMergedLocalBlockBytes = 0L
    val prevNumBlocksToFetch = numBlocksToFetch

    val fallback = FallbackStorage.FALLBACK_BLOCK_MANAGER_ID.executorId
    val localExecIds = Set(blockManager.blockManagerId.executorId, fallback)
    for ((address, blockInfos) <- blocksByAddress) {
      checkBlockSizes(blockInfos)
      if (pushBasedFetchHelper.isPushMergedShuffleBlockAddress(address)) {
        // 当前地址是推送合并块或其Shuffle块所在地址
        if (address.host == blockManager.blockManagerId.host) {
          numBlocksToFetch += blockInfos.size
          pushMergedLocalBlocks ++= block
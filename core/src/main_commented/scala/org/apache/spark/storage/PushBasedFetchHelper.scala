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

import java.util.concurrent.TimeUnit

import scala.collection
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.MapOutputTracker
import org.apache.spark.MapOutputTracker.SHUFFLE_PUSH_MAP_ID
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.shuffle.{BlockStoreClient, MergedBlockMeta, MergedBlocksMetaListener}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.storage.BlockManagerId.SHUFFLE_MERGER_IDENTIFIER
import org.apache.spark.storage.ShuffleBlockFetcherIterator._

/**
 * 文件概要: 推送式shuffle获取辅助类，为ShuffleBlockFetcherIterator提供推送合并块的元数据获取和块拉取功能，
 * 支持在推送合并shuffle失败时回退到拉取原始单个块，优化大规模shuffle场景下的IO性能。
 * 
 * 辅助工具类，封装所有推送式shuffle相关功能，负责获取推送合并块元数据和shuffle块分片，
 * 推送合并块将多个属于同一reduce分区的shuffle块合并为大分片，由外部shuffle服务完成合并，降低shuffleIO次数。
 */
private class PushBasedFetchHelper(
   private val iterator: ShuffleBlockFetcherIterator,
   private val shuffleClient: BlockStoreClient,
   private val blockManager: BlockManager,
   private val mapOutputTracker: MapOutputTracker,
   private val shuffleMetrics: ShuffleReadMetricsReporter) extends Logging {

  // 记录辅助类初始化时间，用于统计获取合并目录耗时
  private[this] val startTimeNs = System.nanoTime()

  // 本地shuffle合并器对应的BlockManagerId，用于标识本地推送合并块的地址
  private[storage] val localShuffleMergerBlockMgrId = BlockManagerId(
    SHUFFLE_MERGER_IDENTIFIER, blockManager.blockManagerId.host,
    blockManager.blockManagerId.port, blockManager.blockManagerId.topologyInfo)

  /**
   * 存储shuffle分片位图的哈希表，键为分片ID，值为位图记录该分片包含的map块ID列表
   */
  private[this] val chunksMetaMap = new mutable.HashMap[ShuffleBlockChunkId, RoaringBitmap]()

  /**
   * 判断给定地址是否为推送合并块地址
   * @param address 待判断的BlockManagerId地址
   * @return 如果是推送合并块返回true，否则返回false
   */
  def isPushMergedShuffleBlockAddress(address: BlockManagerId): Boolean = {
    SHUFFLE_MERGER_IDENTIFIER == address.executorId
  }

  /**
   * 判断给定地址是否为远程推送合并块地址
   * @param address 待判断的BlockManagerId地址
   * @return 如果是远程推送合并块返回true，否则返回false
   */
  def isRemotePushMergedBlockAddress(address: BlockManagerId): Boolean = {
    isPushMergedShuffleBlockAddress(address) && address.host != blockManager.blockManagerId.host
  }

  /**
   * 判断给定地址是否为本地推送合并块地址
   * @param address 待判断的BlockManagerId地址
   * @return 如果是本地推送合并块返回true，否则返回false
   */
  def isLocalPushMergedBlockAddress(address: BlockManagerId): Boolean = {
    isPushMergedShuffleBlockAddress(address) && address.host == blockManager.blockManagerId.host
  }

  /**
   * 处理已完成获取的分片，从元数据缓存中移除指定分片
   * @param blockId 要移除的shuffle分片ID
   */
  def removeChunk(blockId: ShuffleBlockChunkId): Unit = {
    chunksMetaMap.remove(blockId)
  }

  /**
   * 添加已获取元数据的分片到缓存，供后续处理使用
   * @param blockId 分片ID
   * @param chunkMeta 分片对应的位图元数据
   */
  def addChunk(blockId: ShuffleBlockChunkId, chunkMeta: RoaringBitmap): Unit = {
    chunksMetaMap(blockId) = chunkMeta
  }

  /**
   * 根据分片ID获取对应的位图元数据
   * @param blockId 分片ID
   * @return 包含位图的Option，如果不存在返回None
   */
  def getRoaringBitMap(blockId: ShuffleBlockChunkId): Option[RoaringBitmap] = {
    chunksMetaMap.get(blockId)
  }

  /**
   * 获取指定shuffle分片中包含的map块数量
   * @param blockId 分片ID
   * @return 分片中包含的map块总数，不存在返回0
   */
  def getShuffleChunkCardinality(blockId: ShuffleBlockChunkId): Int = {
    getRoaringBitMap(blockId).map(_.getCardinality).getOrElse(0)
  }

  /**
   * 从远程合并块元数据响应解析出所有待获取的shuffle分片信息
   * @param shuffleId shuffle ID
   * @param shuffleMergeId shuffle合并ID
   * @param reduceId reduce分区ID
   * @param blockSize 合并块总大小
   * @param bitmaps 每个分片对应的位图数组
   * @return 待获取分片信息列表，包含分片ID、估算大小、映射ID
   */
  def createChunkBlockInfosFromMetaResponse(
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      blockSize: Long,
      bitmaps: Array[RoaringBitmap]): ArrayBuffer[(BlockId, Long, Int)] = {
    // 估算每个分片的平均大小，用于后续网络IO调度
    val approxChunkSize = blockSize / bitmaps.length
    val blocksToFetch = new ArrayBuffer[(BlockId, Long, Int)]()
    for (i <- bitmaps.indices) {
      val blockChunkId = ShuffleBlockChunkId(shuffleId, shuffleMergeId, reduceId, i)
      chunksMetaMap.put(blockChunkId, bitmaps(i))
      logDebug(s"adding block chunk $blockChunkId of size $approxChunkSize")
      blocksToFetch += ((blockChunkId, approxChunkSize, SHUFFLE_PUSH_MAP_ID))
    }
    blocksToFetch
  }

  /**
   * 向远程外部shuffle服务发送推送合并块元数据获取请求
   * @param req 仅包含推送合并块元数据请求的获取请求
   */
  def sendFetchMergedStatusRequest(req: FetchRequest): Unit = {
    // 构建大小映射表，键为(shuffleId, reduceId)，值为块大小
    val sizeMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, _) =>
        val shuffleBlockId = blockId.asInstanceOf[ShuffleMergedBlockId]
        ((shuffleBlockId.shuffleId, shuffleBlockId.reduceId), size)
    }.toMap
    val address = req.address
    // 创建元数据获取回调监听器，处理成功/失败结果
    val mergedBlocksMetaListener = new MergedBlocksMetaListener {
      override def onSuccess(shuffleId: Int, shuffleMergeId: Int, reduceId: Int,
          meta: MergedBlockMeta): Unit = {
        logDebug(s"Received the meta of push-merged block for ($shuffleId, $shuffleMergeId," +
          s" $reduceId) from ${req.address.host}:${req.address.port}")
        try {
          // 将获取成功的元数据结果加入迭代器结果队列等待处理
          iterator.addToResultsQueue(PushMergedRemoteMetaFetchResult(shuffleId, shuffleMergeId,
            reduceId, sizeMap((shuffleId, reduceId)), meta.readChunkBitmaps(), address))
        } catch {
          case exception: Exception =>
            // 解析元数据失败，记录错误并返回失败结果
            logError(log"Failed to parse the meta of push-merged block for (" +
              log"${MDC(SHUFFLE_ID, shuffleId)}, ${MDC(SHUFFLE_MERGE_ID, shuffleMergeId)}, " +
              log"${MDC(REDUCE_ID, reduceId)}) from ${MDC(HOST, req.address.host)}" +
              log":${MDC(PORT, req.address.port)}", exception)
            iterator.addToResultsQueue(
              PushMergedRemoteMetaFailedFetchResult(shuffleId, shuffleMergeId, reduceId,
                address))
        }
      }

      override def onFailure(shuffleId: Int, shuffleMergeId: Int, reduceId: Int,
          exception: Throwable): Unit = {
        // 获取元数据失败，记录错误并返回失败结果
        logError(log"Failed to get the meta of push-merged block for " +
          log"(${MDC(SHUFFLE_ID, shuffleId)}, ${MDC(REDUCE_ID, reduceId)}) " +
          log"from ${MDC(HOST, req.address.host)}:${MDC(PORT, req.address.port)}", exception)
        iterator.addToResultsQueue(
          PushMergedRemoteMetaFailedFetchResult(shuffleId, shuffleMergeId, reduceId, address))
      }
    }
    // 为每个请求块发送异步元数据获取请求
    req.blocks.foreach { block =>
      val shuffleBlockId = block.blockId.asInstanceOf[ShuffleMergedBlockId]
      shuffleClient.getMergedBlockMeta(address.host, address.port, shuffleBlockId.shuffleId,
        shuffleBlockId.shuffleMergeId, shuffleBlockId.reduceId, mergedBlocksMetaListener)
    }
  }

  /**
   * 拉取所有本地推送合并块的元数据
   * @param pushMergedLocalBlocks 需要拉取的本地推送合并块集合
   */
  def fetchAllPushMergedLocalBlocks(
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): Unit = {
    if (pushMergedLocalBlocks.nonEmpty) {
      // 如果存在本地目录管理器，继续拉取块
      blockManager.hostLocalDirManager.foreach(fetchPushMergedLocalBlocks(_, pushMergedLocalBlocks))
    }
  }

  /**
   * 获取本地推送合并块的存储目录，然后拉取所有指定的本地合并块
   * @param hostLocalDirManager 主机本地目录管理器
   * @param pushMergedLocalBlocks 需要拉取的本地推送合并块集合
   */
  private def fetchPushMergedLocalBlocks(
      hostLocalDirManager: HostLocalDirManager,
      pushMergedLocalBlocks: mutable.LinkedHashSet[BlockId]): Unit = {
    // 先从缓存获取shuffle合并器的本地目录
    val cachedPushedMergedDirs = hostLocalDirManager.getCachedHostLocalDirsFor(
      SHUFFLE_MERGER_IDENTIFIER)
    if (cachedPushedMergedDirs.isDefined) {
      logDebug(s"Fetch the push-merged-local blocks with cached merged dirs: " +
        s"${cachedPushedMergedDirs.get.mkString(", ")}")
      // 缓存命中，直接逐个拉取块
      pushMergedLocalBlocks.foreach { blockId =>
        fetchPushMergedLocalBlock(blockId, cachedPushedMergedDirs.get,
          localShuffleMergerBlockMgrId)
      }
    } else {
      // 缓存未命中，异步从外部shuffle服务获取目录
      logDebug(s"Asynchronous fetch the push-merged-local blocks without cached merged " +
        s"dirs from the external shuffle service")
      hostLocalDirManager.getHostLocalDirs(blockManager.blockManagerId.host,
        blockManager.externalShuffleServicePort, Array(SHUFFLE_MERGER_IDENTIFIER)) {
        case Success(dirs) =>
          // 获取目录成功，逐个拉取块
          logDebug(s"Fetched merged dirs in " +
            s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
          pushMergedLocalBlocks.foreach {
            blockId =>
              logDebug(s"Successfully fetched local dirs: " +
                s"${dirs.get(SHUFFLE_MERGER_IDENTIFIER).mkString(", ")}")
              fetchPushMergedLocalBlock(blockId, dirs(SHUFFLE_MERGER_IDENTIFIER),
                localShuffleMergerBlockMgrId)
          }
        case Failure(throwable) =>
          // 获取目录失败，回退拉取原始块
          logWarning(log"Error while fetching the merged dirs for push-merged-local " +
            log"blocks: ${MDC(BLOCK_IDS, pushMergedLocalBlocks.mkString(", "))}. " +
            log"Fetch the original blocks instead",
            throwable)
          pushMergedLocalBlocks.foreach {
            blockId =>
              iterator.addToResultsQueue(FallbackOnPushMergedFailureResult(
                blockId, localShuffleMergerBlockMgrId, 0, isNetworkReqDone = false))
          }
      }
    }
  }

  /**
   * 拉取单个本地推送合并块的元数据
   * @param blockId 待拉取的块ID
   * @param localDirs 推送合并shuffle文件存储的本地目录
   * @param blockManagerId 块管理器ID
   */
  private[this] def fetchPushMergedLocalBlock(
      blockId: BlockId,
      localDirs: Array[String],
      blockManagerId: BlockManagerId): Unit = {
    try {
      val shuffleBlockId = blockId.asInstanceOf[ShuffleMergedBlockId]
      // 从本地文件读取合并块元数据
      val chunksMeta = blockManager.getLocalMergedBlockMeta(shuffleBlockId, localDirs)
      // 将获取成功的元数据加入结果队列
      iterator.addToResultsQueue(PushMergedLocalMetaFetchResult(
        shuffleBlockId.shuffleId, shuffleBlockId.shuffleMergeId,
        shuffleBlockId.reduceId, chunksMeta.readChunkBitmaps(), localDirs))
    } catch {
      case e: Exception =>
        // 读取元数据异常，回退拉取原始块
        logWarning(log"Error occurred while fetching push-merged-local meta, " +
          log"prepare to fetch the original blocks", e)
        iterator.addToResultsQueue(
          FallbackOnPushMergedFailureResult(blockId, blockManagerId, 0, isNetworkReqDone = false))
    }
  }

  /**
   * 当推送合并块获取失败时，初始化回退流程，拉取原始单个map块
   * 回退场景包括：元数据获取失败、块读取失败、空缓冲区等，回退到拉取原始未合并的单个块
   * @param blockId 获取失败的推送合并块或分片ID
   * @param address 获取失败的块地址
   */
  def initiateFallbackFetchForPushMergedBlock(
      blockId: BlockId,
      address: BlockManagerId): Unit = {
    assert(blockId.isInstanceOf[ShuffleMergedBlockId] || blockId.isInstanceOf[ShuffleBlockChunkId])
    logWarning(log"Falling back to fetch the original blocks for push-merged block " +
      log"${MDC(BLOCK_ID, blockId)}")
    // 指标统计：增加回退次数
    shuffleMetrics.incMergedFetchFallbackCount(1)
    // 从map输出跟踪器获取原始块位置信息，按地址分组后交给迭代器处理
    val fallbackBlocksByAddr: Iterator[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])] =
      blockId match {
        // 失败的是整个合并块，回退拉取该reduce所有原始块
        case shuffleBlockId: ShuffleMergedBlockId =>
          iterator.decreaseNumBlocksToFetch(1)
          mapOutputTracker.getMapSizesForMergeResult(
            shuffleBlockId.shuffleId, shuffleBlockId.reduceId)
        // 失败的是单个分片，回退拉取该分片包含的所有原始块
        case _ =>
          val shuffleChunkId = blockId.asInstanceOf[ShuffleBlockChunkId]
          val chunkBitmap: RoaringBitmap = chunksMetaMap.remove(shuffleChunkId).get
          var blocksProcessed = 1
          // 如果是远程地址失败，将同一地址下所有待处理分片都提前回退，避免重复失败等待
          if (isRemotePushMergedBlockAddress(address)) {
            // 移除同一地址下所有待处理分片，合并位图统一回退
            val pendingShuffleChunks = iterator.removePendingChunks(shuffleChunkId, address)
            pendingShuffleChunks.foreach { pendingBlockId =>
              logInfo(
                log"Falling back immediately for shuffle chunk ${MDC(BLOCK_ID, pendingBlock
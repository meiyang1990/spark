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

import java.io.{File, FileNotFoundException}
import java.net.ConnectException
import java.util.concurrent.ExecutorService

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.util.control.NonFatal

import org.apache.spark.{SecurityManager, ShuffleDependency, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.annotation.Since
import org.apache.spark.executor.{CoarseGrainedExecutorBackend, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.BlockPushNonFatalFailure
import org.apache.spark.network.shuffle.BlockPushingListener
import org.apache.spark.network.shuffle.ErrorHandler.BlockPushErrorHandler
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.ShuffleBlockPusher._
import org.apache.spark.storage.{BlockId, BlockManagerId, ShufflePushBlockId}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 当启用 Push Shuffle 时，用于将 Shuffle 数据块推送到远程 Shuffle 服务。
 * 启用 Push Shuffle 后，Shuffle Writer 完成写入 Shuffle 文件后创建此类并启动数据块推送流程。
 *
 * @param conf Spark 配置
 */
@Since("3.2.0")
private[spark] class ShuffleBlockPusher(conf: SparkConf) extends Logging {
  // 允许推送的最大数据块大小
  private[this] val maxBlockSizeToPush = conf.get(SHUFFLE_MAX_BLOCK_SIZE_TO_PUSH)
  // 每批次推送的最大数据块大小
  private[this] val maxBlockBatchSize = conf.get(SHUFFLE_MAX_BLOCK_BATCH_SIZE_FOR_PUSH)
  // 在传输中的最大字节数
  private[this] val maxBytesInFlight = conf.get(REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024
  // 在传输中的最大请求数
  private[this] val maxReqsInFlight = conf.get(REDUCER_MAX_REQS_IN_FLIGHT)
  // 每个地址在传输中的最大数据块数
  private[this] val maxBlocksInFlightPerAddress = conf.get(REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS)
  private[shuffle] var bytesInFlight = 0L
  private[this] var reqsInFlight = 0
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()
  // 延迟的推送请求队列（按地址分组）
  private[this] val deferredPushRequests = new HashMap[BlockManagerId, Queue[PushRequest]]()
  private[this] val pushRequests = new Queue[PushRequest]
  private[this] val errorHandler = createErrorHandler()
  // 不可达的 BlockManager 集合
  private[shuffle] val unreachableBlockMgrs = new HashSet[BlockManagerId]()
  private[this] var shuffleId = -1
  private[this] var mapIndex = -1
  private[this] var shuffleMergeId = -1
  private[this] var pushCompletionNotified = false

  /**
   * 创建错误处理器。
   * 对于特定主机的连接异常，将停止向该主机推送数据块，继续向其他主机推送。
   * 只有当遇到 "Too Late" 或 "Invalid Block push" 时才停止所有推送。
   */
  private[shuffle] def createErrorHandler(): BlockPushErrorHandler = {
    new BlockPushErrorHandler() {
      override def shouldRetryError(t: Throwable): Boolean = {
        // 如果是从客户端发起的 FileNotFoundException，说明 Shuffle 文件已删除，停止推送
        if (t.getCause != null && t.getCause.isInstanceOf[FileNotFoundException]) {
          return false
        }
        // 如果数据块太晚、无效推送或非最新尝试，无需重试
        !(t.isInstanceOf[BlockPushNonFatalFailure] &&
          BlockPushNonFatalFailure.
            shouldNotRetryErrorCode(t.asInstanceOf[BlockPushNonFatalFailure].getReturnCode));
      }
    }
  }

  private[shuffle] def isPushCompletionNotified = pushCompletionNotified

  /**
   * 启动数据块推送流程。
   *
   * @param dataFile Map 任务生成的 Shuffle 数据文件
   * @param partitionLengths 各分区数据块大小数组
   * @param dep Shuffle 依赖，用于获取 shuffle ID 和远程 Shuffle 服务位置
   * @param mapIndex Shuffle Map 任务的索引
   */
  private[shuffle] def initiateBlockPush(
      dataFile: File,
      partitionLengths: Array[Long],
      dep: ShuffleDependency[_, _, _],
      mapIndex: Int): Unit = {
    val numPartitions = dep.partitioner.numPartitions
    val securityManager = new SecurityManager(conf)
    val transportConf = SparkTransportConf.fromSparkConf(
      conf, "shuffle", sslOptions = Some(securityManager.getRpcSSLOptions()))
    this.shuffleId = dep.shuffleId
    this.shuffleMergeId = dep.shuffleMergeId
    this.mapIndex = mapIndex
    val requests = prepareBlockPushRequests(numPartitions, mapIndex, dep.shuffleId,
      dep.shuffleMergeId, dataFile, partitionLengths, dep.getMergerLocs, transportConf)
    // 随机化 PushRequest 顺序，避免不同 Mapper 同时推送相同范围的 Shuffle 分区
    pushRequests ++= Utils.randomize(requests)
    if (pushRequests.isEmpty) {
      notifyDriverAboutPushCompletion()
    } else {
      submitTask(() => {
        tryPushUpToMax()
      })
    }
  }

  private[shuffle] def tryPushUpToMax(): Unit = {
    try {
      pushUpToMax()
    } catch {
      case NonFatal(e) =>
        logWarning("Failure during push so stopping the block push", e)
    }
  }

  /** 提交推送任务，单独方法便于测试 */
  protected def submitTask(task: Runnable): Unit = {
    if (BLOCK_PUSHER_POOL != null && !BLOCK_PUSHER_POOL.isShutdown) {
      BLOCK_PUSHER_POOL.execute(task)
    }
  }

  /**
   * 由于多个数据块推送线程可能为同一个 Mapper 调用 pushUpToMax，
   * 我们同步此方法以确保只有一个线程为给定 Mapper 推送数据块。
   * 这简化了共享状态的访问控制。
   *
   * 此代码与 ShuffleBlockFetcherIterator#fetchUpToMaxBytes 类似，
   * 都是控制 Shuffle 客户端/服务器之间的数据传输流量。
   */
  private def pushUpToMax(): Unit = synchronized {
    // 处理任何未完成的延迟推送请求
    if (deferredPushRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredPushRequests) {
        while (isRemoteBlockPushable(defReqQueue) &&
          !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred push request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          sendRequest(request)
          if (defReqQueue.isEmpty) {
            deferredPushRequests -= remoteAddress
          }
        }
      }
    }

    // 处理常规推送请求
    while (isRemoteBlockPushable(pushRequests)) {
      val request = pushRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring push request for $remoteAddress with ${request.blocks.size} blocks")
        deferredPushRequests.getOrElseUpdate(remoteAddress, new Queue[PushRequest]())
          .enqueue(request)
      } else {
        sendRequest(request)
      }
    }

    /** 检查是否可以推送更多远程数据块 */
    def isRemoteBlockPushable(pushReqQueue: Queue[PushRequest]): Boolean = {
      pushReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + pushReqQueue.front.size <= maxBytesInFlight))
    }

    /** 检查发送新推送请求是否会超过给定远程地址的最大数据块数限制 */
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: PushRequest): Boolean = {
      (numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0)
        + request.blocks.size) > maxBlocksInFlightPerAddress
    }
  }

  /**
   * 向远程 Shuffle 服务器推送数据块。
   * 回调监听器在当前批次部分传输完成后再次调用 pushUpToMax 触发下一批次推送。
   * 这样将 Map 任务与数据块推送流程解耦，大部分推送由 netty 客户端线程处理而非任务执行线程。
   */
  private def sendRequest(request: PushRequest): Unit = {
    bytesInFlight +=  request.size
    reqsInFlight += 1
    numBlocksInFlightPerAddress(request.address) = numBlocksInFlightPerAddress.getOrElseUpdate(
      request.address, 0) + request.blocks.length

    val sizeMap = request.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val address = request.address
    val blockIds = request.blocks.map(_._1.toString)
    val remainingBlocks = new HashSet[String]() ++= blockIds

    val blockPushListener = new BlockPushingListener {
      // 连接创建和数据块推送由 block-push-threads 处理
      // 不应在 netty eventloop 的回调中创建连接，因为：
      // 1. createConnection 会阻塞等待连接建立
      // 2. 实际连接创建会添加到另一个 eventloop 的任务队列，可能导致相互阻塞
      def handleResult(result: PushResult): Unit = {
        submitTask(() => {
          if (updateStateAndCheckIfPushMore(
            sizeMap(result.blockId), address, remainingBlocks, result)) {
            tryPushUpToMax()
          }
        })
      }

      override def onBlockPushSuccess(blockId: String, data: ManagedBuffer): Unit = {
        logTrace(s"Push for block $blockId to $address successful.")
        handleResult(PushResult(blockId, null))
      }

      override def onBlockPushFailure(blockId: String, exception: Throwable): Unit = {
        if (!errorHandler.shouldLogError(exception)) {
          logTrace(s"Pushing block $blockId to $address failed.", exception)
        } else {
          logWarning(log"Pushing block ${MDC(BLOCK_ID, blockId)} " +
            log"to ${MDC(HOST_PORT, address)} failed.", exception)
        }
        handleResult(PushResult(blockId, exception))
      }
    }
    // 除随机化推送请求顺序外，还随机化请求内数据块顺序，
    // 进一步降低 Shuffle 服务端推送数据块冲突的可能性
    val (blockPushIds, blockPushBuffers) = Utils.randomize(blockIds.zip(
      sliceReqBufferIntoBlockBuffers(request.reqBuffer, request.blocks.map(_._2)))).unzip
    SparkEnv.get.blockManager.blockStoreClient.pushBlocks(
      address.host, address.port, blockPushIds.toArray,
      blockPushBuffers.toArray, blockPushListener)
  }

  /**
   * 将代表 PushRequest 中所有连续数据块的 ManagedBuffer 加载到内存，
   * 并切片为多个较小缓冲区，每个代表一个数据块。
   *
   * 使用 NIO ByteBuffer 时，各数据块缓冲区共享从磁盘加载的内存缓冲区数据，
   * 因此内存中只保留一份数据块数据副本。
   *
   * @param reqBuffer 代表 Shuffle 数据文件中 PushRequest 所有连续块的 FileSegmentManagedBuffer
   * @param blockSizes 各数据块大小数组
   * @return 各数据块的内存缓冲区数组
   */
  private def sliceReqBufferIntoBlockBuffers(
      reqBuffer: ManagedBuffer,
      blockSizes: Seq[Int]): Array[ManagedBuffer] = {
    if (blockSizes.size == 1) {
      Array(reqBuffer)
    } else {
      val inMemoryBuffer = reqBuffer.nioByteBuffer()
      val blockOffsets = new Array[Int](blockSizes.size)
      var offset = 0
      for (index <- blockSizes.indices) {
        blockOffsets(index) = offset
        offset += blockSizes(index)
      }
      blockOffsets.zip(blockSizes).map {
        case (offset, size) =>
          new NioManagedBuffer(inMemoryBuffer.duplicate()
            .position(offset)
            .limit(offset + size).slice())
      }.toArray
    }
  }

  /**
   * 更新统计信息并根据上次推送结果决定是否继续推送。
   *
   * @param bytesPushed 已推送字节数
   * @param address 远程服务地址
   * @param remainingBlocks 剩余数据块
   * @param pushResult 上次推送结果
   * @return true 表示应继续推送；false 表示停止
   */
  private def updateStateAndCheckIfPushMore(
      bytesPushed: Long,
      address: BlockManagerId,
      remainingBlocks: HashSet[String],
      pushResult: PushResult): Boolean = synchronized {
    remainingBlocks -= pushResult.blockId
    bytesInFlight -= bytesPushed
    numBlocksInFlightPerAddress(address) -= 1
    if (remainingBlocks.isEmpty) {
      reqsInFlight -= 1
    }
    if (pushResult.failure != null && pushResult.failure.getCause.isInstanceOf[ConnectException]) {
      // 遇到 ConnectException 时，一次性移除该地址的所有数据块
      // 因为第一个块连接失败后，后续块也会失败
      if (!unreachableBlockMgrs.contains(address)) {
        var removed = 0
        unreachableBlockMgrs.add(address)
        removed += pushRequests.dequeueAll(req => req.address == address).length
        removed += deferredPushRequests.remove(address).map(_.length).getOrElse(0)
        logWarning(log"Received a ConnectException from ${MDC(HOST_PORT, address)}. " +
          log"Dropping ${MDC(NUM_REQUESTS, removed)} push-requests and " +
          log"not pushing any more blocks to this address.")
      }
    }
    if (pushResult.failure != null && !errorHandler.shouldRetryError(pushResult.failure)) {
      logDebug(s"Encountered an exception from $address which indicates that push needs to " +
        s"stop.")
      return false
    } else {
      if (reqsInFlight <= 0 && pushRequests.isEmpty && deferredPushRequests.isEmpty) {
        notifyDriverAboutPushCompletion()
      }
      remainingBlocks.isEmpty && (pushRequests.nonEmpty || deferredPushRequests.nonEmpty)
    }
  }

  /**
   * 通知 Driver 当前 Map 任务生成的所有数据块已推送完成。
   * 这使 DAGScheduler 能在足够的 Map 任务完成推送后尽快完成 Shuffle 合并，
   * 而非总是等待固定时间。
   */
  protected def notifyDriverAboutPushCompletion(): Unit = {
    assert(shuffleId >= 0 && mapIndex >= 0)
    if (!pushCompletionNotified) {
      SparkEnv.get.executorBackend match {
        case Some(cb: CoarseGrainedExecutorBackend) =>
          cb.notifyDriverAboutPushCompletion(shuffleId, shuffleMergeId, mapIndex)
        case Some(eb: ExecutorBackend) =>
          logWarning(log"Currently ${MDC(EXECUTOR_BACKEND, eb)} " +
            log"doesn't support push-based shuffle")
        case None =>
      }
      pushCompletionNotified = true
    }
  }

  /**
   * 将当前 Mapper 的 Shuffle 数据文件转换为 PushRequest 列表。
   * 连续的数据块分组到单个请求中以支持更高效的数据读取。
   * 同一 Shuffle 的所有 Mapper 收到相同的目标 BlockManagerId 列表，
   * 并以一致方式将 Shuffle 分区范围映射到各个目标位置。
   * 长度为 0 和过大的数据块会被跳过。
   *
   * @param numPartitions Shuffle 文件中的分区数
   * @param partitionId 当前 Mapper 的索引
   * @param shuffleId 当前 Shuffle 的 ID
   * @param shuffleMergeId 用于唯一标识不确定 Stage 尝试的 Shuffle 合并过程
   * @param dataFile Shuffle 数据文件
   * @param partitionLengths Shuffle 数据文件中各数据块大小数组
   * @param mergerLocs 推送目标位置列表
   * @param transportConf 用于创建 FileSegmentManagedBuffer 的传输配置
   * @return 随机排序的 PushRequest 列表
   */
  private[shuffle] def prepareBlockPushRequests(
      numPartitions: Int,
      partitionId: Int,
      shuffleId: Int,
      shuffleMergeId: Int,
      dataFile: File,
      partitionLengths: Array[Long],
      mergerLocs: Seq[BlockManagerId],
      transportConf: TransportConf): Seq[PushRequest] = {
    var offset = 0L
    var currentReqSize = 0
    var currentReqOffset = 0L
    var currentMergerId = 0
    val numMergers = mergerLocs.length
    val requests = new ArrayBuffer[PushRequest]
    var blocks = new ArrayBuffer[(BlockId, Int)]
    for (reduceId <- 0 until numPartitions) {
      val blockSize = partitionLengths(reduceId)
      logDebug(
        s"Block ${ShufflePushBlockId(shuffleId, shuffleMergeId, partitionId,
          reduceId)} is of size $blockSize")
      // 跳过长度为 0 和过大的数据块
      if (blockSize > 0) {
        val mergerId = math.min(math.floor(reduceId * 1.0 / numPartitions * numMergers),
          numMergers - 1).asInstanceOf[Int]
        // 当超过最大批次大小、数据块数限制、目标服务不同或数据块过大时，开始新的 PushRequest
        if (currentReqSize + blockSize <= maxBlockBatchSize
          && blocks.size < maxBlocksInFlightPerAddress
          && mergerId == currentMergerId && blockSize <= maxBlockSizeToPush) {
          // 将当前数据块添加到当前批次
          currentReqSize += blockSize.toInt
        } else {
          if (blocks.nonEmpty) {
            // 将之前的批次转换为 PushRequest
            requests += PushRequest(mergerLocs(currentMergerId), blocks.toSeq,
              createRequestBuffer(transportConf, dataFile, currentReqOffset, currentReqSize))
            blocks = new ArrayBuffer[(BlockId, Int)]
          }
          // 开始新批次
          currentReqSize = 0
          // 设置为 -1 以区分初始值和即将开始新批次的情况
          currentReqOffset = -1
          currentMergerId = mergerId
        }
        // 只推送大小限制内的数据块
        if (blockSize <= maxBlockSizeToPush) {
          val blockSizeInt = blockSize.toInt
          blocks += ((ShufflePushBlockId(shuffleId, shuffleMergeId, partitionId,
            reduceId), blockSizeInt))
          // 仅当当前数据块是请求中的第一个时更新 currentReqOffset
          if (currentReqOffset == -1) {
            currentReqOffset = offset
          }
          if (currentReqSize == 0) {
            currentReqSize += blockSizeInt
          }
        }
      }
      offset += blockSize
    }
    // 添加最后一个请求
    if (blocks.nonEmpty) {
      requests += PushRequest(mergerLocs(currentMergerId), blocks.toSeq,
        createRequestBuffer(transportConf, dataFile, currentReqOffset, currentReqSize))
    }
    requests.toSeq
  }

  /** 创建请求数据缓冲区 */
  protected def createRequestBuffer(
      conf: TransportConf,
      dataFile: File,
      offset: Long,
      length: Long): ManagedBuffer = {
    new FileSegmentManagedBuffer(conf, dataFile, offset, length)
  }
}

private[spark] object ShuffleBlockPusher {

  /**
   * 向远程 Shuffle 服务推送数据块的请求
   *
   * @param address 远程 Shuffle 服务位置
   * @param blocks 数据块 ID 和大小列表
   * @param reqBuffer Shuffle 数据文件中对应连续数据块的数据块
   */
  private[spark] case class PushRequest(
    address: BlockManagerId,
    blocks: Seq[(BlockId, Int)],
    reqBuffer: ManagedBuffer) {
    val size = blocks.map(_._2).sum
  }

  /**
   * 数据块推送结果
   *
   * @param blockId 数据块 ID
   * @param failure 推送失败时的异常；成功时为 null
   */
  private case class PushResult(blockId: String, failure: Throwable)

  /** 数据块推送线程池 */
  private val BLOCK_PUSHER_POOL: ExecutorService = {
    val conf = SparkEnv.get.conf
    if (Utils.isPushBasedShuffleEnabled(conf,
        isDriver = SparkContext.DRIVER_IDENTIFIER == SparkEnv.get.executorId)) {
      val numThreads = conf.get(SHUFFLE_NUM_PUSH_THREADS)
        .getOrElse(conf.getInt(SparkLauncher.EXECUTOR_CORES, 1))
      ThreadUtils.newDaemonFixedThreadPool(numThreads, "shuffle-block-push-thread")
    } else {
      null
    }
  }

  /** 停止 Shuffle 推送线程池 */
  private[spark] def stop(): Unit = {
    if (BLOCK_PUSHER_POOL != null) {
      BLOCK_PUSHER_POOL.shutdown()
    }
  }
}

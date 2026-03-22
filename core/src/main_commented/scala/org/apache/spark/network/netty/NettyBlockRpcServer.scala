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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockBatchId, ShuffleBlockId, StorageLevel}

/**
 * 基于Netty实现的块数据RPC服务器，为Spark块传输协议提供远程服务端处理能力
 * 
 * 核心职责：处理来自远程节点的块数据请求，包括打开块、获取shuffle块、上传块等操作，
 * 采用one-for-one策略管理流传输，每个Netty Chunk对应一个Spark级别的shuffle块，
 * 支持对BlockManager管理的任意块进行打开和上传操作。
 * 
 * @param appId 当前Spark应用ID
 * @param serializer 序列化器，用于序列化/反序列化块元数据
 * @param blockManager 本地块数据管理器，提供块数据读写接口
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  // 采用one-for-one策略的流管理器，管理所有块传输流
  private val streamManager = new OneForOneStreamManager()

  /**
   * 处理接收到的RPC请求，根据消息类型分发到不同处理逻辑
   * @param client 传输客户端，代表本次连接
   * @param rpcMessage RPC消息字节缓存
   * @param responseContext RPC响应回调接口
   */
  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = try {
      // 解码块传输消息
      BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    } catch {
      case e: IllegalArgumentException if e.getMessage.startsWith("Unknown message type") =>
        logWarning(log"This could be a corrupted RPC message (capacity: " +
          log"${MDC(RPC_MESSAGE_CAPACITY, rpcMessage.capacity())}) " +
          log"from ${MDC(SOCKET_ADDRESS, client.getSocketAddress)}. " +
          log"Please use `spark.authenticate.*` configurations " +
          log"in case of security incidents.")
        throw e

      case _: IndexOutOfBoundsException | _: NegativeArraySizeException =>
        // 对于损坏的缓冲，Netty可能抛出非IOException异常，直接忽略该消息并打警告日志
        logWarning(log"Ignored a corrupted RPC message (capacity: " +
          log"${MDC(RPC_MESSAGE_CAPACITY, rpcMessage.capacity())}) " +
          log"from ${MDC(SOCKET_ADDRESS, client.getSocketAddress)}. " +
          log"Please use `spark.authenticate.*` configurations " +
          log"in case of security incidents.")
        return
    }
    logTrace(s"Received request: $message")

    message match {
      // 处理打开多个块的请求
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length
        val blocks = (0 until blocksNum).map { i =>
          val blockId = BlockId.apply(openBlocks.blockIds(i))
          assert(!blockId.isInstanceOf[ShuffleBlockBatchId],
            "Continuous shuffle block fetching only works for new fetch protocol.")
          blockManager.getLocalBlockData(blockId)
        }
        // 在流管理器注册待传输的块流
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        // 返回流句柄给客户端，客户端后续拉取流数据
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)

      // 处理获取shuffle块的请求
      case fetchShuffleBlocks: FetchShuffleBlocks =>
        val blocks = fetchShuffleBlocks.mapIds.zipWithIndex.flatMap { case (mapId, index) =>
          if (!fetchShuffleBlocks.batchFetchEnabled) {
            // 非批量获取，逐个获取每个reduce对应的shuffle块
            fetchShuffleBlocks.reduceIds(index).map { reduceId =>
              blockManager.getLocalBlockData(
                ShuffleBlockId(fetchShuffleBlocks.shuffleId, mapId, reduceId))
            }
          } else {
            // 批量获取，一次性获取一个map的连续reduce区间块
            val startAndEndId = fetchShuffleBlocks.reduceIds(index)
            if (startAndEndId.length != 2) {
              throw SparkException.internalError("Invalid shuffle fetch request when batch mode " +
                s"is enabled: $fetchShuffleBlocks", category = "NETWORK")
            }
            Array(blockManager.getLocalBlockData(
              ShuffleBlockBatchId(
                fetchShuffleBlocks.shuffleId, mapId, startAndEndId(0), startAndEndId(1))))
          }
        }

        val numBlockIds = if (fetchShuffleBlocks.batchFetchEnabled) {
          fetchShuffleBlocks.mapIds.length
        } else {
          fetchShuffleBlocks.reduceIds.map(_.length).sum
        }

        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $numBlockIds buffers")
        responseContext.onSuccess(
          new StreamHandle(streamId, numBlockIds).toByteBuffer)

      // 处理上传单个块到本地的请求
      case uploadBlock: UploadBlock =>
        // 反序列化存储级别和类型标签元数据
        val (level, classTag) = deserializeMetadata(uploadBlock.metadata)
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")
        // 将块数据存入本地块管理器
        val blockStored = blockManager.putBlockData(blockId, data, level, classTag)
        if (blockStored) {
          // 存储成功，返回空响应
          responseContext.onSuccess(ByteBuffer.allocate(0))
        } else {
          // 存储失败，返回异常
          val exception = SparkException.internalError(
            s"Upload block for $blockId failed. This mostly happens " +
            "when there is not sufficient space available to store the block.",
            category = "NETWORK")
          responseContext.onFailure(exception)
        }

      // 处理获取执行器本地磁盘目录的请求
      case getLocalDirs: GetLocalDirsForExecutors =>
        val isIncorrectAppId = getLocalDirs.appId != appId
        val execNum = getLocalDirs.execIds.length
        if (isIncorrectAppId || execNum != 1) {
          // 请求参数不合法，返回异常
          val errorMsg = "Invalid GetLocalDirsForExecutors request: " +
            s"${if (isIncorrectAppId) s"incorrect application id: ${getLocalDirs.appId};"}" +
            s"${if (execNum != 1) s"incorrect executor number: $execNum (expected 1);"}"
          responseContext.onFailure(
            SparkException.internalError(errorMsg, category = "NETWORK"))
        } else {
          val expectedExecId = blockManager.asInstanceOf[BlockManager].executorId
          val actualExecId = getLocalDirs.execIds.head
          if (actualExecId != expectedExecId) {
            // 执行器ID不匹配，返回异常
            responseContext.onFailure(SparkException.internalError(
              s"Invalid executor id: $actualExecId, expected $expectedExecId.",
              category = "NETWORK"))
          } else {
            // 返回当前执行器的本地磁盘目录
            responseContext.onSuccess(new LocalDirsForExecutors(
              Map(actualExecId -> blockManager.getLocalDiskDirs).asJava).toByteBuffer)
          }
        }

      // 处理shuffle块损坏诊断请求
      case diagnose: DiagnoseCorruption =>
        val cause = blockManager.diagnoseShuffleBlockCorruption(
          ShuffleBlockId(diagnose.shuffleId, diagnose.mapId, diagnose.reduceId ),
          diagnose.checksum,
          diagnose.algorithm)
        // 返回损坏原因诊断结果
        responseContext.onSuccess(new CorruptionCause(cause).toByteBuffer)
    }
  }

  /**
   * 处理流式上传块请求，返回流回调接口处理后续数据
   * @param client 传输客户端
   * @param messageHeader 请求消息头
   * @param responseContext 响应回调
   * @return 处理流式数据的回调对象
   */
  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level, classTag) = deserializeMetadata(message.metadata)
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // 立即返回回调接口，流数据后续在Netty线程中通过回调处理
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }

  /**
   * 反序列化块元数据，解析出存储级别和类型标签
   * @param metadata 元数据字节数组
   * @return (存储级别, 类型标签)
   */
  private def deserializeMetadata[T](metadata: Array[Byte]): (StorageLevel, ClassTag[T]) = {
    serializer
      .newInstance()
      .deserialize(ByteBuffer.wrap(metadata))
      .asInstanceOf[(StorageLevel, ClassTag[T])]
  }

  override def getStreamManager(): StreamManager = streamManager
}
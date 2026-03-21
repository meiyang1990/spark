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

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap, Map => JMap}

import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Success, Try}

import com.codahale.metrics.{Metric, MetricSet}

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.ExecutorDeadException
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockTransferListener, DownloadFileManager, OneForOneBlockFetcher, RetryingBlockTransferor}
import org.apache.spark.network.shuffle.protocol.{UploadBlock, UploadBlockStream}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.storage.BlockManagerMessages.IsExecutorAlive
import org.apache.spark.util.Utils

/**
 * 【学习笔记】NettyBlockTransferService - 基于 Netty 的 Spark 块传输服务实现。
 *
 * <p>核心职责：
 * 1. 负责集群内 Executor 之间的数据块（Block）存取传输。
 * 2. 对接 Shuffle 数据读取（Fetch）和写回（Upload）操作。
 * 3. 封装 Netty 的 TransportServer 和 ClientFactory，提供统一的块传输接口。
 *
 * <p>工作原理：
 * - 初始化：通过 SparkTransportConf 配置 Netty 服务端（绑定端口）和客户端（连接池）。
 * - 传输机制：支持小块数据的 RPC 通信，以及大块 Shuffle 数据的流式传输（Streaming）。
 * - 容错处理：集成 RetryingBlockTransferor 实现传输过程中的自动重试，确保 Shuffle 阶段稳定性。
 */
private[spark] class NettyBlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    serializerManager: SerializerManager,
    bindAddress: String,
    override val hostName: String,
    _port: Int,
    numCores: Int,
    driverEndPointRef: RpcEndpointRef = null)
  extends BlockTransferService with Logging {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = serializerManager.getSerializer(scala.reflect.classTag[Any], false)
  private val authEnabled = securityManager.isAuthenticationEnabled()

  private[this] var transportContext: TransportContext = _
  private[this] var server: TransportServer = _

  // 初始化 Netty 网络传输上下文
  override def init(blockDataManager: BlockDataManager): Unit = {
    // NettyBlockRpcServer 处理来自其他节点的 RPC 请求
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    
    // 初始化传输配置
    this.transportConf = SparkTransportConf.fromSparkConf(
      conf,
      "shuffle",
      numCores,
      sslOptions = Some(securityManager.getRpcSSLOptions()))
    
    // 启用鉴权（安全配置）
    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }
    
    // 构建 Netty 传输上下文，负责创建 Server 和 Client 工厂
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    
    if (hostName.equals(bindAddress)) {
      logger.info("Server created on {}:{}",
        MDC(LogKeys.HOST, hostName), MDC(LogKeys.PORT, server.getPort))
    } else {
      logger.info("Server created on {} {}:{}", MDC(LogKeys.HOST, hostName),
        MDC(LogKeys.BIND_ADDRESS, bindAddress), MDC(LogKeys.PORT, server.getPort))
    }
  }

  // 尝试绑定端口，支持端口冲突时的端口轮询策略
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  /**
   * 拉取数据块，整合了重试机制。
   */
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace(s"Fetch blocks from $host:$port (executor id $execId)")
    }
    try {
      val maxRetries = transportConf.maxIORetries()
      // 定义如何启动一次块抓取任务
      val blockFetchStarter = new RetryingBlockTransferor.BlockTransferStarter {
        override def createAndStart(blockIds: Array[String],
            listener: BlockTransferListener): Unit = {
          assert(listener.isInstanceOf[BlockFetchingListener],
            s"Expecting a BlockFetchingListener, but got ${listener.getClass}")
          try {
            val client = clientFactory.createClient(host, port, maxRetries > 0)
            // 核心：使用 OneForOneBlockFetcher 对每个块进行拉取
            new OneForOneBlockFetcher(client, appId, execId, blockIds,
              listener.asInstanceOf[BlockFetchingListener], transportConf, tempFileManager).start()
          } catch {
            case e: IOException =>
              // 如果发生 I/O 异常，检查远程 Executor 是否已挂掉
              Try {
                driverEndPointRef.askSync[Boolean](IsExecutorAlive(execId))
              } match {
                case Success(v) if v == false =>
                  throw ExecutorDeadException(s"The relative remote executor(Id: $execId)," +
                    " which maintains the block data to fetch is dead.")
                case _ => throw e
              }
          }
        }
      }

      // 如果配置了重试，使用 RetryingBlockTransferor 包裹 starter
      if (maxRetries > 0) {
        new RetryingBlockTransferor(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logger.error("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
  }

  override def port: Int = server.getPort

  /**
   * 将数据块上传到指定的 Executor 节点。
   * 支持流式上传（大块数据）或 RPC 上传（小块数据）。
   */
  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)

    // 序列化元数据（StorageLevel 和 ClassTag）
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    // 逻辑判定：块大小超过阈值或为 Shuffle 数据，采用流式传输
    val asStream = (blockData.size() > conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM) ||
      blockId.isShuffle)
    val callback = new RpcResponseCallback {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (logger.isTraceEnabled) {
          logger.trace(s"Successfully uploaded block $blockId${if (asStream) " as stream" else ""}")
        }
        result.success((): Unit)
      }

      override def onFailure(e: Throwable): Unit = {
        if (asStream) {
          logger.error(s"Error while uploading {} as stream", e, MDC(LogKeys.BLOCK_ID, blockId))
        } else {
          logger.error(s"Error while uploading {}", e, MDC(LogKeys.BLOCK_ID, blockId))
        }
        result.failure(e)
      }
    }
    
    // 执行真正的传输
    if (asStream) {
      // 流式上传：封装头部信息，发送 ManagedBuffer 流
      val streamHeader = new UploadBlockStream(blockId.name, metadata).toByteBuffer
      client.uploadStream(new NioManagedBuffer(streamHeader), blockData, callback)
    } else {
      // RPC 上传：序列化为字节数组后发送
      val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

      client.sendRpc(new UploadBlock(appId, execId, blockId.name, metadata, array).toByteBuffer,
        callback)
    }

    result.future
  }

  // 服务销毁时回收 Netty 相关资源
  override def close(): Unit = {
    if (server != null) {
      server.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    if (transportContext != null) {
      transportContext.close()
    }
  }
}

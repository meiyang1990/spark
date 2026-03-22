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
 * 基于Netty实现的Spark块数据传输服务，负责集群节点间Shuffle块数据的拉取与上传。
 * 核心职责：为Executor之间的数据块交换提供高性能的网络传输能力，集成身份认证和重试容错机制，是Spark Shuffle阶段的核心网络组件。
 *
 * @param conf Spark配置对象
 * @param securityManager 安全管理器，用于身份认证
 * @param serializerManager 序列化管理器，用于元数据序列化
 * @param bindAddress 服务端绑定地址
 * @param hostName 对外公告的主机名
 * @param _port 初始绑定端口，0表示随机端口
 * @param numCores 当前节点可用核心数，用于配置Netty线程池
 * @param driverEndPointRef Driver端Rpc端点引用，用于检查远程Executor存活状态
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

  /**
   * 初始化Netty块传输服务，启动服务端并创建客户端工厂
   * @param blockDataManager 块数据管理器，处理本地块数据的读写请求
   */
  override def init(blockDataManager: BlockDataManager): Unit = {
    // 创建RPC请求处理器，处理远端节点的块读写请求
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    
    // 从Spark配置构建Netty传输配置
    this.transportConf = SparkTransportConf.fromSparkConf(
      conf,
      "shuffle",
      numCores,
      sslOptions = Some(securityManager.getRpcSSLOptions()))
    
    // 若启用身份认证，添加认证Bootstrap
    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }
    
    // 创建传输上下文，初始化服务端和客户端工厂
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

  /**
   * 在指定端口启动Netty服务端，支持端口冲突自动重试
   * @param bootstraps 服务端启动拦截器列表
   * @return 启动完成的TransportServer实例
   */
  private def createServer(bootstraps: List[TransportServerBootstrap]): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  /**
   * 从远端Executor拉取指定块数据，集成自动重试容错机制
   * @param host 远端节点主机
   * @param port 远端服务端口
   * @param execId 远端Executor ID
   * @param blockIds 需要拉取的块ID数组
   * @param listener 拉取结果回调监听器
   * @param tempFileManager 临时文件管理器，用于处理大文件落地
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
      // 定义单次块拉取任务的启动逻辑
      val blockFetchStarter = new RetryingBlockTransferor.BlockTransferStarter {
        override def createAndStart(blockIds: Array[String],
            listener: BlockTransferListener): Unit = {
          assert(listener.isInstanceOf[BlockFetchingListener],
            s"Expecting a BlockFetchingListener, but got ${listener.getClass}")
          try {
            val client = clientFactory.createClient(host, port, maxRetries > 0)
            // 使用OneForOneBlockFetcher逐个拉取块数据
            new OneForOneBlockFetcher(client, appId, execId, blockIds,
              listener.asInstanceOf[BlockFetchingListener], transportConf, tempFileManager).start()
          } catch {
            case e: IOException =>
              // IO异常时检查远端Executor是否已经死亡
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

      // 配置了重试则使用RetryingBlockTransferor包装，支持失败重试
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
   * 将本地块数据上传到指定远端Executor
   * 根据块大小自动选择RPC直传或流式传输，大块数据/Shuffle块使用流式传输
   * @param hostname 远端节点主机
   * @param port 远端服务端口
   * @param execId 目标Executor ID
   * @param blockId 上传块ID
   * @param blockData 块数据缓冲区
   * @param level 块存储级别
   * @param classTag 块数据类型标记
   * @return 上传完成的异步Future
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

    // 序列化存储级别和类型标记元数据
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    // 判断是否使用流式传输：块大小超过阈值 或者 是Shuffle块
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
    
    // 根据策略选择传输方式发起上传请求
    if (asStream) {
      // 流式上传：先发送流头，再传输块数据本体
      val streamHeader = new UploadBlockStream(blockId.name, metadata).toByteBuffer
      client.uploadStream(new NioManagedBuffer(streamHeader), blockData, callback)
    } else {
      // RPC上传：将整个块序列化一次性发送
      val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

      client.sendRpc(new UploadBlock(appId, execId, blockId.name, metadata, array).toByteBuffer,
        callback)
    }

    result.future
  }

  /**
   * 关闭服务，释放所有Netty相关资源
   */
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
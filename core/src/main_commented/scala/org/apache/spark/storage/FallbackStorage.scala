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

import java.io.DataInputStream
import java.nio.ByteBuffer

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.BlockManagerMessages.RemoveShuffle
import org.apache.spark.util.Utils

/**
 * 存储下线降级使用的 fallback 存储，用于将节点下线时的 shuffle 数据迁移到HDFS等远程存储，供其他节点读取
 * 存储下线是节点缩容/退役过程中的数据迁移阶段，此机制保证数据不会丢失
 */
private[storage] class FallbackStorage(conf: SparkConf) extends Logging {
  require(conf.contains("spark.app.id"))
  require(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined)

  // Fallback存储根路径，配置来自 STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH
  private val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
  // 创建对应Hadoop配置
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  // 获取对应文件系统实例
  private val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
  // 当前应用ID，用于隔离不同应用的数据
  private val appId = conf.getAppId

  // Visible for testing
  /**
   * 将指定shuffle块从本地块管理器复制到fallback存储
   * @param shuffleBlockInfo 待复制的shuffle块信息
   * @param bm 源块管理器
   */
  def copy(
      shuffleBlockInfo: ShuffleBlockInfo,
      bm: BlockManager): Unit = {
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId

    bm.migratableResolver match {
      case r: IndexShuffleBlockResolver =>
        val indexFile = r.getIndexFile(shuffleId, mapId)

        if (indexFile.exists()) {
          // 文件名哈希，用于二级目录散列，避免单目录文件过多
          val hash = JavaUtils.nonNegativeHash(indexFile.getName)
          // 将本地索引文件复制到fallback存储对应目录
          fallbackFileSystem.copyFromLocalFile(
            new Path(Utils.resolveURI(indexFile.getAbsolutePath)),
            new Path(fallbackPath, s"$appId/$shuffleId/$hash/${indexFile.getName}"))

          val dataFile = r.getDataFile(shuffleId, mapId)
          if (dataFile.exists()) {
            val hash = JavaUtils.nonNegativeHash(dataFile.getName)
            // 将本地数据文件复制到fallback存储对应目录
            fallbackFileSystem.copyFromLocalFile(
              new Path(Utils.resolveURI(dataFile.getAbsolutePath)),
              new Path(fallbackPath, s"$appId/$shuffleId/$hash/${dataFile.getName}"))
          }

          // 向块管理器主节点报告块状态，更新块位置信息为fallback存储
          val reduceId = NOOP_REDUCE_ID
          val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, reduceId)
          FallbackStorage.reportBlockStatus(bm, indexBlockId, indexFile.length)
          if (dataFile.exists) {
            val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, reduceId)
            FallbackStorage.reportBlockStatus(bm, dataBlockId, dataFile.length)
          }
        }
      case r =>
        logWarning(log"Unsupported Resolver: ${MDC(CLASS_NAME, r.getClass.getName)}")
    }
  }

  /**
   * 检查fallback存储中是否存在指定文件
   * @param shuffleId shuffle ID
   * @param filename 文件名
   * @return 是否存在
   */
  def exists(shuffleId: Int, filename: String): Boolean = {
    val hash = JavaUtils.nonNegativeHash(filename)
    fallbackFileSystem.exists(new Path(fallbackPath, s"$appId/$shuffleId/$hash/$filename"))
  }
}

/**
 * Fallback存储的RPC端点引用，用于处理来自块管理器主节点的RPC请求
 * 仅处理移除shuffle的清理请求，其他请求不处理
 */
private[storage] class FallbackStorageRpcEndpointRef(conf: SparkConf, hadoopConf: Configuration)
    extends RpcEndpointRef(conf) {
  // scalastyle:off executioncontextglobal
  import scala.concurrent.ExecutionContext.Implicits.global
  // scalastyle:on executioncontextglobal
  override def address: RpcAddress = null
  override def name: String = "fallback"
  override def send(message: Any): Unit = {}
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    message match {
      case RemoveShuffle(shuffleId) =>
        // 收到移除shuffle请求，清理对应fallback存储中的数据
        FallbackStorage.cleanUp(conf, hadoopConf, Some(shuffleId))
      case _ => // no-op
    }
    Future{true.asInstanceOf[T]}
  }
}

/**
 * Fallback存储全局工具对象，提供创建、注册、读取、清理fallback存储的通用能力
 */
private[spark] object FallbackStorage extends Logging {
  /** Fallback块管理器ID占位符，代表存储在fallback的块所属的虚拟块管理器 */
  val FALLBACK_BLOCK_MANAGER_ID: BlockManagerId = BlockManagerId("fallback", "remote", 7337)

  /**
   * 根据配置创建FallbackStorage实例，如果未配置则返回None
   * @param conf Spark配置
   * @return 可选的FallbackStorage实例
   */
  def getFallbackStorage(conf: SparkConf): Option[FallbackStorage] = {
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
      Some(new FallbackStorage(conf))
    } else {
      None
    }
  }

  /**
   * 如果配置了fallback存储，向块管理器主节点注册虚拟fallback块管理器
   * @param master 块管理器主节点
   * @param conf Spark配置
   * @param hadoopConf Hadoop配置
   */
  def registerBlockManagerIfNeeded(
      master: BlockManagerMaster,
      conf: SparkConf,
      hadoopConf: Configuration): Unit = {
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined) {
      master.registerBlockManager(
        FALLBACK_BLOCK_MANAGER_ID, Array.empty[String], 0, 0,
        new FallbackStorageRpcEndpointRef(conf, hadoopConf))
    }
  }

  /**
   * 清理fallback存储中当前应用的数据，可指定清理单个shuffle的数据
   * @param conf Spark配置
   * @param hadoopConf Hadoop配置
   * @param shuffleId 可选的shuffle ID，为None时清理整个应用目录
   */
  def cleanUp(conf: SparkConf, hadoopConf: Configuration, shuffleId: Option[Int] = None): Unit = {
    if (conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).isDefined &&
        conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_CLEANUP) &&
        conf.contains("spark.app.id")) {
      // 构建需要清理的路径：根目录/appId/[shuffleId]
      val fallbackPath = shuffleId.foldLeft(
        new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get, conf.getAppId)
      ) { case (path, shuffleId) => new Path(path, shuffleId.toString) }
      val fallbackUri = fallbackPath.toUri
      val fallbackFileSystem = FileSystem.get(fallbackUri, hadoopConf)
      // 当前应用的fallback目录可能还未创建，先检查存在性
      if (fallbackFileSystem.exists(fallbackPath)) {
        if (fallbackFileSystem.delete(fallbackPath, true)) {
          logInfo(log"Succeed to clean up: ${MDC(URI, fallbackUri)}")
        } else {
          // 权限问题等可能导致清理失败，仅打警告不中断流程
          logWarning(log"Failed to clean up: ${MDC(URI, fallbackUri)}")
        }
      }
    }
  }

  /**
   * 向块管理器主节点报告新的块状态，更新块位置到fallback块管理器
   * @param blockManager 源块管理器
   * @param blockId 块ID
   * @param dataLength 块大小
   */
  private def reportBlockStatus(blockManager: BlockManager, blockId: BlockId, dataLength: Long) = {
    assert(blockManager.master != null)
    blockManager.master.updateBlockInfo(
      FALLBACK_BLOCK_MANAGER_ID, blockId, StorageLevel.DISK_ONLY, memSize = 0, dataLength)
  }

  /**
   * 从fallback存储读取指定块，封装为ManagedBuffer返回
   * @param conf Spark配置
   * @param blockId 待读取块ID
   * @return 块数据封装的ManagedBuffer
   */
  def read(conf: SparkConf, blockId: BlockId): ManagedBuffer = {
    logInfo(log"Read ${MDC(BLOCK_ID, blockId)}")
    val fallbackPath = new Path(conf.get(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH).get)
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val fallbackFileSystem = FileSystem.get(fallbackPath.toUri, hadoopConf)
    val appId = conf.getAppId

    // 解析块ID得到shuffleId、mapId、reduce范围
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId", category = "STORAGE")
    }

    // 打开索引文件，查找对应reduce范围的数据偏移量
    val name = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val hash = JavaUtils.nonNegativeHash(name)
    val indexFile = new Path(fallbackPath, s"$appId/$shuffleId/$hash/$name")
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    Utils.tryWithResource(fallbackFileSystem.open(indexFile)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        index.skip(start)
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        // 根据偏移量从数据文件读取对应范围的数据
        val name = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
        val hash = JavaUtils.nonNegativeHash(name)
        val dataFile = new Path(fallbackPath, s"$appId/$shuffleId/$hash/$name")
        val size = nextOffset - offset
        logDebug(s"To byte array $size")
        val array = new Array[Byte](size.toInt)
        val startTimeNs = System.nanoTime()
        Utils.tryWithResource(fallbackFileSystem.open(dataFile)) { f =>
          f.seek(offset)
          f.readFully(array)
          logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
        }
        // 封装为NIO ManagedBuffer返回
        new NioManagedBuffer(ByteBuffer.wrap(array))
      }
    }
  }
}
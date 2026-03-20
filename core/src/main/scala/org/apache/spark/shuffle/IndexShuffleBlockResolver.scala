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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.collection.mutable.ArrayBuffer

import com.google.common.cache.CacheBuilder

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExecutorDiskUtils, MergedBlockMeta}
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

/**
 * 创建并维护 Shuffle 数据块在逻辑块和物理文件位置之间的映射关系。
 * 来自同一 Map 任务的 Shuffle 数据块存储在单个合并的数据文件中，
 * 数据块在数据文件中的偏移量存储在单独的索引文件中。
 *
 * 数据文件命名规则：使用 reduce ID 为 0 的 shuffleBlockId 名称，添加 ".data" 后缀
 * 索引文件命名规则：使用 reduce ID 为 0 的 shuffleBlockId 名称，添加 ".index" 后缀
 *
 * 注意：对此文件格式的修改需与 ExternalShuffleBlockResolver#getSortBasedShuffleBlockData() 保持同步。
 */
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    // var 用于测试
    var _blockManager: BlockManager,
    val taskIdMapsForShuffle: ConcurrentMap[Int, OpenHashSet[Long]])
  extends ShuffleBlockResolver
  with Logging with MigratableResolver {

  def this(conf: SparkConf) = {
    this(conf, null, new ConcurrentHashMap[Int, OpenHashSet[Long]]())
  }

  def this(conf: SparkConf, _blockManager: BlockManager) = {
    this(conf, _blockManager, new ConcurrentHashMap[Int, OpenHashSet[Long]]())
  }

  def this(conf: SparkConf, taskIdMapsForShuffle: ConcurrentHashMap[Int, OpenHashSet[Long]]) = {
    this(conf, null, taskIdMapsForShuffle)
  }

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = {
    val securityManager = new SecurityManager(conf)
    SparkTransportConf.fromSparkConf(
      conf, "shuffle", sslOptions = Some(securityManager.getRpcSSLOptions()))
  }

  private val remoteShuffleMaxDisk: Option[Long] =
    conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_DISK_SIZE)

  private val checksumEnabled = conf.get(config.SHUFFLE_CHECKSUM_ENABLED)
  private lazy val algorithm = conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM)

  def getDataFile(shuffleId: Int, mapId: Long): File = getDataFile(shuffleId, mapId, None)

  /**
   * 获取本地存储的 Shuffle 文件列表，用于数据块迁移。
   */
  override def getStoredShuffles(): Seq[ShuffleBlockInfo] = {
    val allBlocks = blockManager.diskBlockManager.getAllBlocks()
    allBlocks.flatMap {
      case ShuffleIndexBlockId(shuffleId, mapId, _)
        if Option(shuffleIdsToSkip.getIfPresent(shuffleId)).isEmpty =>
        Some(ShuffleBlockInfo(shuffleId, mapId))
      case _ =>
        None
    }
  }

  /** 需要跳过的 Shuffle ID 缓存，用于过滤已标记的 Shuffle */
  private val shuffleIdsToSkip =
    CacheBuilder.newBuilder().maximumSize(1000).build[java.lang.Integer, java.lang.Boolean]()

  override def addShuffleToSkip(shuffleId: ShuffleId): Unit = {
    shuffleIdsToSkip.put(shuffleId, true)
  }

  /** 获取本地存储的所有 Shuffle 数据总字节数 */
  private def getShuffleBytesStored(): Long = {
    val shuffleFiles: Seq[File] = getStoredShuffles().map {
      si => getDataFile(si.shuffleId, si.mapId)
    }
    shuffleFiles.map(_.length()).sum
  }

  /** 创建临时文件，后续将重命名为最终结果文件 */
  def createTempFile(file: File): File = {
    blockManager.diskBlockManager.createTempFileWith(file)
  }

  /**
   * 获取 Shuffle 数据文件。
   *
   * @param dirs 如果为 None，使用 disk manager 的本地目录；否则从指定目录读取
   */
   def getDataFile(shuffleId: Int, mapId: Long, dirs: Option[Array[String]]): File = {
    val blockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, blockId.name)))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  /**
   * 获取 Shuffle 索引文件。
   *
   * @param dirs 如果为 None，使用 disk manager 的本地目录；否则从指定目录读取
   */
  def getIndexFile(
      shuffleId: Int,
      mapId: Long,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, blockId.name)))
      .getOrElse(blockManager.diskBlockManager.getFile(blockId))
  }

  private def getMergedBlockDataFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedDataBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  private def getMergedBlockIndexFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedIndexBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  private def getMergedBlockMetaFile(
      appId: String,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      dirs: Option[Array[String]] = None): File = {
    blockManager.diskBlockManager.getMergedShuffleFile(
      ShuffleMergedMetaBlockId(appId, shuffleId, shuffleMergeId, reduceId), dirs)
  }

  /**
   * 删除一个 Map 任务输出的数据文件和索引文件。
   */
  def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists() && !file.delete()) {
      logWarning(log"Error deleting data ${MDC(PATH, file.getPath())}")
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists() && !file.delete()) {
      logWarning(log"Error deleting index ${MDC(PATH, file.getPath())}")
    }

    if (checksumEnabled) {
      file = getChecksumFile(shuffleId, mapId, algorithm)
      if (file.exists() && !file.delete()) {
        logWarning(log"Error deleting checksum ${MDC(PATH, file.getPath())}")
      }
    }
  }

  /**
   * 检查索引文件和数据文件是否匹配。
   * 如果匹配，返回数据文件中各分区的长度数组；否则返回 null。
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // 索引文件应有 (blocks + 1) 个 long 作为偏移量
    if (index.length() != (blocks + 1) * 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    // 读取各数据块的长度
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // 将偏移量转换为各数据块的长度
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // 数据文件大小应与索引文件记录一致
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * 以流的方式写入 Shuffle 数据块，用于数据块迁移。
   * ShuffleBlockBatchId 必须包含 ShuffleIndexBlock 表示的完整范围。
   * 要求调用者在数据块写入失败时删除相关的 shuffle 索引块。
   */
  override def putShuffleBlockAsStream(blockId: BlockId, serializerManager: SerializerManager):
      StreamCallbackWithID = {
    // 检查是否超过最大 Shuffle 文件存储限制
    remoteShuffleMaxDisk.foreach { maxBytes =>
      val bytesUsed = getShuffleBytesStored()
      if (maxBytes < bytesUsed) {
        throw SparkException.internalError(
          s"Not storing remote shuffles $bytesUsed exceeds $maxBytes", category = "SHUFFLE")
      }
    }
    val file = blockId match {
      case ShuffleIndexBlockId(shuffleId, mapId, _) =>
        getIndexFile(shuffleId, mapId)
      case ShuffleDataBlockId(shuffleId, mapId, _) =>
        getDataFile(shuffleId, mapId)
      case _ =>
        throw SparkException.internalError(s"Unexpected shuffle block transfer $blockId as " +
          s"${blockId.getClass().getSimpleName()}", category = "SHUFFLE")
    }
    val fileTmp = createTempFile(file)

    // Shuffle 数据块直接通过网络发送，无需 serializerManager.wrapStream() 处理。
    // 即如果原始数据已加密，写入文件时仍保持加密状态。
    val channel = Channels.newChannel(new FileOutputStream(fileTmp))

    new StreamCallbackWithID {

      override def getID: String = blockId.name

      override def onData(streamId: String, buf: ByteBuffer): Unit = {
        while (buf.hasRemaining) {
          channel.write(buf)
        }
      }

      override def onComplete(streamId: String): Unit = {
        logTrace(s"Done receiving shuffle block $blockId, now storing on local disk.")
        channel.close()
        val diskSize = fileTmp.length()
        this.synchronized {
          if (file.exists()) {
            file.delete()
          }
          if (!fileTmp.renameTo(file)) {
            throw SparkCoreErrors.failedRenameTempFileError(fileTmp, file)
          }
        }
        blockId match {
          case ShuffleIndexBlockId(shuffleId, mapId, _) =>
            val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
              shuffleId, _ => new OpenHashSet[Long](8)
            )
            mapTaskIds.synchronized { mapTaskIds.add(mapId) }

          case ShuffleDataBlockId(shuffleId, mapId, _) =>
            val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
              shuffleId, _ => new OpenHashSet[Long](8)
            )
            mapTaskIds.synchronized { mapTaskIds.add(mapId) }

          case _ => // Unreachable
        }
        blockManager.reportBlockStatus(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0, diskSize))
      }

      override def onFailure(streamId: String, cause: Throwable): Unit = {
        // 框架处理连接本身，我们只需做本地清理
        logWarning(log"Error while uploading ${MDC(BLOCK_ID, blockId)}", cause)
        channel.close()
        fileTmp.delete()
      }
    }
  }

  /**
   * 获取用于迁移的索引和数据块。
   */
  def getMigrationBlocks(shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)] = {
    try {
      val shuffleId = shuffleBlockInfo.shuffleId
      val mapId = shuffleBlockInfo.mapId
      // 加载索引块
      val indexFile = getIndexFile(shuffleId, mapId)
      val indexBlockId = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
      val indexFileSize = indexFile.length()
      val indexBlockData = new FileSegmentManagedBuffer(
        transportConf, indexFile, 0, indexFileSize)

      // 加载数据块
      val dataFile = getDataFile(shuffleId, mapId)
      val dataBlockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
      val dataBlockData = new FileSegmentManagedBuffer(
        transportConf, dataFile, 0, dataFile.length())

      // 确保索引文件存在
      if (!indexFile.exists()) {
        throw SparkException.internalError("Index file is deleted already.", category = "SHUFFLE")
      }
      if (dataFile.exists()) {
        List((dataBlockId, dataBlockData), (indexBlockId, indexBlockData))
      } else {
        List((indexBlockId, indexBlockData))
      }
    } catch {
      case _: Exception => // 如果无法加载块则忽略
        logWarning(log"Failed to resolve shuffle block " +
          log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}. " +
          log"This is expected to occur if a block is removed after decommissioning has started.")
        List.empty[(BlockId, ManagedBuffer)]
    }
  }


  /**
   * 以原子操作提交数据文件和元数据文件，使用现有文件或用新文件替换。
   * 注意：如果使用现有文件，元数据参数（`lengths`, `checksums`）将被更新为现有值。
   *
   * 有两种元数据文件：
   *
   * - 索引文件
   *   包含每个数据块的偏移量，以及文件末尾的最终偏移量。
   *   [[getBlockData]] 使用它来确定每个数据块的起止位置。
   *
   * - 校验和文件（可选）
   *   包含每个数据块的校验和，用于诊断数据块损坏的原因。
   *   空的 `checksums` 表示禁用校验和功能。
   */
  def writeMetadataFileAndCommit(
      shuffleId: Int,
      mapId: Long,
      lengths: Array[Long],
      checksums: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = createTempFile(indexFile)

    val checksumEnabled = checksums.nonEmpty
    val (checksumFileOpt, checksumTmpOpt) = if (checksumEnabled) {
      assert(lengths.length == checksums.length,
        "The size of partition lengths and checksums should be equal")
      val checksumFile = getChecksumFile(shuffleId, mapId, algorithm)
      (Some(checksumFile), Some(createTempFile(checksumFile)))
    } else {
      (None, None)
    }

    try {
      val dataFile = getDataFile(shuffleId, mapId)
      // 每个 Executor 只有一个 IndexShuffleBlockResolver，此同步确保以下检查和重命名是原子的
      this.synchronized {
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // 同一任务的另一次尝试已成功写入 Map 输出，使用现有分区长度并删除临时输出
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (checksumEnabled) {
            val existingChecksums = getChecksums(checksumFileOpt.get, checksums.length)
            if (existingChecksums != null) {
              System.arraycopy(existingChecksums, 0, checksums, 0, lengths.length)
            } else {
              // 可能之前任务尝试成功写入了索引和数据文件，但校验和文件写入失败
              // 当前任务尝试可以自行写入缺失的校验和文件
              writeMetadataFile(checksums, checksumTmpOpt.get, checksumFileOpt.get, false)
            }
          }
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          // 这是首次成功写入此任务的 Map 输出，用我们写入的文件覆盖现有索引和数据文件
          val offsets = lengths.scanLeft(0L)(_ + _)
          writeMetadataFile(offsets, indexTmp, indexFile, true)

          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw SparkCoreErrors.failedRenameTempFileError(dataTmp, dataFile)
          }

          // 写入校验和文件
          checksumTmpOpt.zip(checksumFileOpt).foreach { case (checksumTmp, checksumFile) =>
            try {
              writeMetadataFile(checksums, checksumTmp, checksumFile, false)
            } catch {
              case e: Exception =>
                // 索引和数据文件已成功存储后，不值得因校验和失败
                // 校验和仅用于边缘错误情况的最佳努力
                logError("Failed to write checksum file", e)
            }
          }
        }
      }
    } finally {
      logDebug(s"Shuffle index for mapId $mapId: ${lengths.mkString("[", ",", "]")}")
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(log"Failed to delete temporary index file at " +
          log"${MDC(PATH, indexTmp.getAbsolutePath)}")
      }
      checksumTmpOpt.foreach { checksumTmp =>
        if (checksumTmp.exists()) {
          try {
            if (!checksumTmp.delete()) {
              logError(log"Failed to delete temporary checksum file at " +
                log"${MDC(LogKeys.PATH, checksumTmp.getAbsolutePath)}")
            }
          } catch {
            case e: Exception =>
              // 与索引删除不同，校验和文件错误不传播，因为校验和仅是最佳努力
              logError(log"Failed to delete temporary checksum file " +
                log"at ${MDC(PATH, checksumTmp.getAbsolutePath)}", e)
          }
        }
      }
    }
  }

  /**
   * 写入元数据文件（索引或校验和）。元数据先写入临时文件，最后重命名为目标文件以避免脏写。
   *
   * @param metaValues 元数据值数组
   * @param tmpFile 临时文件
   * @param targetFile 目标文件
   * @param propagateError 是否传播文件操作错误。与索引文件不同，校验和仅是最佳努力，不会因此失败整个任务
   */
  private def writeMetadataFile(
      metaValues: Array[Long],
      tmpFile: File,
      targetFile: File,
      propagateError: Boolean): Unit = {
    val out = new DataOutputStream(
      new BufferedOutputStream(
        new FileOutputStream(tmpFile)
      )
    )
    Utils.tryWithSafeFinally {
      metaValues.foreach(out.writeLong)
    } {
      out.close()
    }

    if (targetFile.exists()) {
      targetFile.delete()
    }

    if (!tmpFile.renameTo(targetFile)) {
      if (propagateError) {
        throw SparkCoreErrors.failedRenameTempFileError(tmpFile, targetFile)
      } else {
        logWarning(log"fail to rename file ${MDC(TEMP_FILE, tmpFile)} " +
          log"to ${MDC(TARGET_PATH, targetFile)}")
      }
    }
  }

  /**
   * 仅用于读取本地合并数据块数据。
   * 在这种情况下，需要一次识别合并 Shuffle 文件中的所有 chunk，
   * 以便 ShuffleBlockFetcherIterator 知道如何将本地合并 Shuffle 文件作为多个 chunk 消费。
   */
  override def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer] = {
    val indexFile =
      getMergedBlockIndexFile(conf.getAppId, blockId.shuffleId, blockId.shuffleMergeId,
        blockId.reduceId, dirs)
    val dataFile = getMergedBlockDataFile(conf.getAppId, blockId.shuffleId,
      blockId.shuffleMergeId, blockId.reduceId, dirs)
    // 加载所有索引以识别指定合并 Shuffle 文件中的所有 chunk
    val size = indexFile.length.toInt
    val offsets = Utils.tryWithResource {
      new DataInputStream(Files.newInputStream(indexFile.toPath))
    } { dis =>
      val buffer = ByteBuffer.allocate(size)
      dis.readFully(buffer.array)
      buffer.asLongBuffer
    }
    // chunk 数量 = 索引数 - 1
    val numChunks = size / 8 - 1
    for (index <- 0 until numChunks) yield {
      new FileSegmentManagedBuffer(transportConf, dataFile,
        offsets.get(index),
        offsets.get(index + 1) - offsets.get(index))
    }
  }

  /**
   * 仅用于读取本地合并数据块元数据。
   */
  override def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta = {
    val indexFile =
      getMergedBlockIndexFile(conf.getAppId, blockId.shuffleId,
        blockId.shuffleMergeId, blockId.reduceId, dirs)
    val size = indexFile.length.toInt
    val numChunks = (size / 8) - 1
    val metaFile = getMergedBlockMetaFile(conf.getAppId, blockId.shuffleId,
      blockId.shuffleMergeId, blockId.reduceId, dirs)
    val chunkBitMaps = new FileSegmentManagedBuffer(transportConf, metaFile, 0L, metaFile.length)
    new MergedBlockMeta(numChunks, chunkBitMaps)
  }

  private[shuffle] def getChecksums(checksumFile: File, blockNum: Int): Array[Long] = {
    if (!checksumFile.exists()) return null
    val checksums = new ArrayBuffer[Long]
    // 读取各数据块的校验和
    var in: DataInputStream = null
    try {
      in = new DataInputStream(new NioBufferedFileInputStream(checksumFile))
      while (checksums.size < blockNum) {
        checksums += in.readLong()
      }
    } catch {
      case _: IOException | _: EOFException =>
        return null
    } finally {
      in.close()
    }

    checksums.toArray
  }

  /**
   * 获取 Shuffle 校验和文件。
   *
   * @param dirs 如果为 None，使用 disk manager 的本地目录；否则从指定目录读取
   */
  def getChecksumFile(
      shuffleId: Int,
      mapId: Long,
      algorithm: String,
      dirs: Option[Array[String]] = None): File = {
    val blockId = ShuffleChecksumBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    val fileName = ShuffleChecksumHelper.getChecksumFileName(blockId.name, algorithm)
    dirs
      .map(d =>
        new File(ExecutorDiskUtils.getFilePath(d, blockManager.subDirsPerLocalDir, fileName)))
      .getOrElse(blockManager.diskBlockManager.getFile(fileName))
  }

  /**
   * 根据 blockId 获取对应的 Shuffle 数据块内容。
   * 支持单个 ShuffleBlockId 和批量 ShuffleBlockBatchId。
   */
  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw SparkException.internalError(
          s"unexpected shuffle block id format: $blockId", category = "SHUFFLE")
    }
    // 数据块实际上是单个 Map 输出文件的一个范围，
    // 所以先找到合并文件，再从索引获取偏移量
    val indexFile = getIndexFile(shuffleId, mapId, dirs)

    // SPARK-22982: 如果其他代码错误地使用我们的文件描述符向前 seek，
    // 此代码会获取错误的偏移量（可能导致 reducer 收到其他 reducer 的数据）。
    // 这里添加的位置检查在 SPARK-22982 调试期间很有帮助，
    // 保留它们有助于防止此类问题再次发生。
    val channel = Files.newByteChannel(indexFile.toPath)
    channel.position(startReduceId * 8L)
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      val startOffset = in.readLong()
      channel.position(endReduceId * 8L)
      val endOffset = in.readLong()
      val actualPosition = channel.position()
      val expectedPosition = endReduceId * 8L + 8
      if (actualPosition != expectedPosition) {
        throw SparkException.internalError(s"SPARK-22982: Incorrect channel position after index" +
          s" file reads: expected $expectedPosition but actual position was $actualPosition.",
          category = "SHUFFLE")
      }
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(shuffleId, mapId, dirs),
        startOffset,
        endOffset - startOffset)
    } finally {
      in.close()
    }
  }

  override def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    Seq(
      ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID),
      ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    )
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // 与磁盘存储交互时使用的 no-op reduce ID。
  // 磁盘存储目前期望 put 操作关联到 (map, reduce) 对，
  // 但在 sort shuffle 中，多个 reduce 的输出合并到单个文件中。
  val NOOP_REDUCE_ID = 0
}

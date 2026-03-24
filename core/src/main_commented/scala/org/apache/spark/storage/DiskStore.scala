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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable.ListBuffer

import com.google.common.io.Closeables
import io.netty.channel.DefaultFileRegion

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.{AbstractFileRegion, JavaUtils}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * 文件级磁盘存储模块，负责将Spark块持久化到本地磁盘，提供块的读写、删除、移动等操作，是BlockManager的磁盘存储层
 */
private[spark] class DiskStore(
    conf: SparkConf,
    diskManager: DiskBlockManager,
    securityManager: SecurityManager) extends Logging {

  private val minMemoryMapBytes = conf.get(config.STORAGE_MEMORY_MAP_THRESHOLD)
  private val maxMemoryMapBytes = conf.get(config.MEMORY_MAP_LIMIT_FOR_TESTS)
  // 存储块ID与块大小的映射关系，用于快速查询块大小
  private val blockSizes = new ConcurrentHashMap[BlockId, Long]()

  // 是否开启外部shuffle服务拉取缓存RDD功能
  private val shuffleServiceFetchRddEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED) &&
    conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)

  /**
   * 根据块ID查询存储在磁盘上的块大小
   * @param blockId 块ID
   * @return 块大小（字节）
   */
  def getSize(blockId: BlockId): Long = blockSizes.get(blockId)

  /**
   * 写入块到磁盘，通过回调函数执行实际写入操作，如果块已存在会尝试删除后重新写入
   * @param blockId 待写入的块ID
   * @param writeFunc 实际写入逻辑的回调函数，接收可写字节通道作为参数
   * @throws IllegalStateException 如果块已存在且无法删除时抛出异常
   */
  def put(blockId: BlockId)(writeFunc: WritableByteChannel => Unit): Unit = {
    if (contains(blockId)) {
      logWarning(log"Block ${MDC(BLOCK_ID, blockId)} is already present in the disk store")
      try {
        diskManager.getFile(blockId).delete()
      } catch {
        case e: Exception =>
          throw SparkException.internalError(
            s"Block $blockId is already present in the disk store and could not delete it $e",
            category = "STORAGE")
      }
    }
    logDebug(s"Attempting to put block $blockId")
    val startTimeNs = System.nanoTime()
    val file = diskManager.getFile(blockId)

    // SPARK-37618: 如果开启了外部shuffle服务拉取缓存RDD，需要将文件设置为全局可读，
    // 因为在安全环境下shuffle服务运行用户与文件所有者不在同一用户组，必须开放全局读权限
    if (shuffleServiceFetchRddEnabled) {
      diskManager.createWorldReadableFile(file)
    }
    // 使用带计数功能的可写通道，统计写入字节数
    val out = new CountingWritableChannel(openForWrite(file))
    var threwException: Boolean = true
    try {
      writeFunc(out)
      // 保存块大小到映射表
      blockSizes.put(blockId, out.getCount)
      threwException = false
    } finally {
      try {
        out.close()
      } catch {
        case ioe: IOException =>
          if (!threwException) {
            threwException = true
            throw ioe
          }
      } finally {
         // 写入过程出现异常，清理已写入的垃圾数据
         if (threwException) {
          remove(blockId)
        }
      }
    }
    logDebug(s"Block ${file.getName} stored as ${Utils.bytesToString(file.length())} file" +
      s" on disk in ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
  }

  /**
   * 将分块字节缓冲区直接写入磁盘块
   * @param blockId 目标块ID
   * @param bytes 待写入的分块字节数据
   */
  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { channel =>
      bytes.writeFully(channel)
    }
  }

  /**
   * 根据块ID读取磁盘上的块数据
   * @param blockId 待读取的块ID
   * @return 封装后的块数据对象
   */
  def getBytes(blockId: BlockId): BlockData = {
    getBytes(diskManager.getFile(blockId.name), getSize(blockId))
  }

  /**
   * 根据文件和块大小读取块数据，处理加密和未加密两种情况
   * @param f 存储块的文件
   * @param blockSize 块大小（加密场景为解密后的大小）
   * @return 封装后的块数据对象
   */
  def getBytes(f: File, blockSize: Long): BlockData = securityManager.getIOEncryptionKey() match {
    case Some(key) =>
      // 加密块无法使用内存映射，返回专门的加密块数据对象处理解密和读取
      new EncryptedBlockData(f, blockSize, conf, key)

    case _ =>
      new DiskBlockData(minMemoryMapBytes, maxMemoryMapBytes, f, blockSize)
  }

  /**
   * 删除磁盘上的指定块
   * @param blockId 待删除的块ID
   * @return 删除成功返回true，文件不存在或删除失败返回false
   */
  def remove(blockId: BlockId): Boolean = {
    blockSizes.remove(blockId)
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(log"Error deleting ${MDC(PATH, file.getPath())}")
      }
      ret
    } else {
      false
    }
  }

  /**
   * 将已有文件移动到磁盘存储作为新块，用于避免重复写入直接复用已有文件
   * @param sourceFile 源文件路径
   * @param blockSize 块大小（加密场景为解密后的大小）
   * @param targetBlockId 目标块ID
   */
  def moveFileToBlock(sourceFile: File, blockSize: Long, targetBlockId: BlockId): Unit = {
    blockSizes.put(targetBlockId, blockSize)
    val targetFile = diskManager.getFile(targetBlockId.name)
    logDebug(s"${sourceFile.getPath()} -> ${targetFile.getPath()}")
    Utils.moveFile(sourceFile, targetFile)
  }

  /**
   * 检查磁盘存储是否包含指定块
   * @param blockId 待检查的块ID
   * @return 包含返回true，否则返回false
   */
  def contains(blockId: BlockId): Boolean = diskManager.containsBlock(blockId)

  /**
   * 打开文件用于写入，处理IO加密逻辑
   * @param file 待打开的文件
   * @return 可写字节通道，如果开启加密则返回加密包装通道
   */
  private def openForWrite(file: File): WritableByteChannel = {
    val out = new FileOutputStream(file).getChannel()
    try {
      // 如果开启IO加密，包装为加密通道
      securityManager.getIOEncryptionKey().map { key =>
        CryptoStreamUtils.createWritableChannel(out, conf, key)
      }.getOrElse(out)
    } catch {
      case e: Exception =>
        Closeables.close(out, true)
        file.delete()
        throw e
    }
  }

}

/**
 * 未加密磁盘块数据封装，提供多种格式输出块数据，支持内存映射优化读取
 * @param minMemoryMapBytes 启用内存映射的最小字节阈值，小于该阈值直接读取
 * @param maxMemoryMapBytes 内存映射允许的最大字节阈值，大于该阈值禁止内存映射
 * @param file 存储块数据的文件
 * @param blockSize 块大小（字节）
 */
private class DiskBlockData(
    minMemoryMapBytes: Long,
    maxMemoryMapBytes: Long,
    file: File,
    blockSize: Long) extends BlockData {

  override def toInputStream(): InputStream = new FileInputStream(file)

  /**
  * 返回适配Netty的文件区域包装，用于零拷贝网络传输
  */
  override def toNetty(): AnyRef = new DefaultFileRegion(file, 0, size)

  /**
   * 返回适配SSL场景的Netty包装，SSL不支持零拷贝需要先读取到内存
   */
  override def toNettyForSsl(): AnyRef =
    toChunkedByteBuffer(ByteBuffer.allocate).toNettyForSsl

  override def toChunkedByteBuffer(allocator: (Int) => ByteBuffer): ChunkedByteBuffer = {
    Utils.tryWithResource(open()) { channel =>
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        // 按照最大内存映射限制切分块
        val chunkSize = math.min(remaining, maxMemoryMapBytes)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(channel, chunk)
        // 切换缓冲区为读模式
        chunk.flip()
        chunks += chunk
      }
      new ChunkedByteBuffer(chunks.toArray)
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    require(blockSize < maxMemoryMapBytes,
      s"can't create a byte buffer of size $blockSize" +
      s" since it exceeds ${Utils.bytesToString(maxMemoryMapBytes)}.")
    Utils.tryWithResource(open()) { channel =>
      if (blockSize < minMemoryMapBytes) {
        // 小文件直接读取到堆内存，不使用内存映射减少 overhead
        val buf = ByteBuffer.allocate(blockSize.toInt)
        JavaUtils.readFully(channel, buf)
        buf.flip()
        buf
      } else {
        // 大文件使用内存映射提高读取效率
        channel.map(MapMode.READ_ONLY, 0, file.length)
      }
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = {}

  private def open() = new FileInputStream(file).getChannel
}

/**
 * 加密磁盘块数据封装，读取时自动解密处理，不支持内存映射
 * @param file 存储加密块的文件
 * @param blockSize 解密后的块大小（字节）
 * @param conf Spark配置对象
 * @param key IO加密密钥
 */
private[spark] class EncryptedBlockData(
    file: File,
    blockSize: Long,
    conf: SparkConf,
    key: Array[Byte]) extends BlockData {

  override def toInputStream(): InputStream = Channels.newInputStream(open())

  override def toNetty(): Object = new ReadableChannelFileRegion(open(), blockSize)

  override def toNettyForSsl(): AnyRef =
    toChunkedByteBuffer(ByteBuffer.allocate).toNettyForSsl

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val source = open()
    try {
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        // 按最大数组长度限制切分块
        val chunkSize = math.min(remaining, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(source, chunk)
        chunk.flip()
        chunks += chunk
      }

      new ChunkedByteBuffer(chunks.toArray)
    } finally {
      source.close()
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    // 该方法被块传输服务用于块复制，会将整个块读入内存发送给远程执行器，
    // 因此只支持大小不超过Java数组最大长度的块
    assert(blockSize <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
      "Block is too large to be wrapped in a byte buffer.")
    val dst = ByteBuffer.allocate(blockSize.toInt)
    val in = open()
    try {
      JavaUtils.readFully(in, dst)
      dst.flip()
      dst
    } finally {
      Closeables.close(in, true)
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = { }

  /**
   * 打开加密通道，自动包装解密逻辑
   * @return 解密后的可读字节通道
   */
  private def open(): ReadableByteChannel = {
    val channel = new FileInputStream(file).getChannel()
    try {
      CryptoStreamUtils.createReadableChannel(channel, conf, key)
    } catch {
      case e: Exception =>
        Closeables.close(channel, true)
        throw e
    }
  }
}

/**
 * 加密块的ManagedBuffer实现，用于网络传输时适配Spark网络协议
 * @param blockData 加密块数据对象
 */
private[spark] class EncryptedManagedBuffer(
    val blockData: EncryptedBlockData) extends ManagedBuffer {

  // 返回解密后数据的大小
  override def size(): Long = blockData.size

  override def nioByteBuffer(): ByteBuffer = blockData.toByteBuffer()

  override def convertToNetty(): AnyRef = blockData.toNetty()

  override def convertToNettyForSsl(): AnyRef = blockData.toNettyForSsl()

  override def createInputStream(): InputStream = blockData.toInputStream()

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this
}

/**
 * 适用于加密块的Netty AbstractFileRegion实现，通过分步缓冲读取解密后数据传输
 * @param source 解密后的可读字节通道
 * @param blockSize 解密后数据总大小
 */
private class ReadableChannelFileRegion(source: ReadableByteChannel, blockSize: Long)
  extends AbstractFileRegion {

  private var _transferred = 0L

  // 使用64KB直接缓冲区做传输缓存
  private val buffer = Platform.allocateDirectBuffer(64 * 1024)
  buffer.flip()

  override def count(): Long = blockSize

  override def position(): Long = 0

  override def transferred(): Long = _transferred

  override def transferTo(target: WritableByteChannel, pos: Long): Long = {
    assert(pos == transferred(), "Invalid position.")

    var written = 0L
    var lastWrite = -1L
    // 循环读取源数据并写入目标通道，直到缓冲区没有更多数据可写
    while (lastWrite != 0) {
      if (!buffer.hasRemaining()) {
        // 缓冲区读完，清空重新从源读取
        buffer.clear()
        source.read(buffer)
        buffer.flip()
      }
      if (buffer.hasRemaining()) {
        // 写入目标通道，累计写入字节数
        lastWrite = target.write(buffer)
        written += lastWrite
      } else {
        lastWrite = 0
      }
    }

    _transferred += written
    written
  }

  override def deallocate(): Unit = source.close()
}

/**
 * 带字节计数功能的WritableByteChannel包装，用于统计写入总字节数，记录块大小
 * @param sink 底层可写字节通道
 */
private class CountingWritableChannel(sink: WritableByteChannel) extends WritableByteChannel {

  private var count = 0L

  /**
   * 返回累计写入的总字节数
   * @return 写入字节总数
   */
  def getCount: Long = count

  override def write(src: ByteBuffer): Int = {
    val written = sink.write(src)
    if (written > 0) {
      count += written
    }
    written
  }

  override def isOpen(): Boolean = sink.isOpen()

  override def close(): Unit = sink.close()

}
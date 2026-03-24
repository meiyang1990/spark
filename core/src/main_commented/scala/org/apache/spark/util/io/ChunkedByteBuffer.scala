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

package org.apache.spark.util.io

import java.io.{Externalizable, File, FileInputStream, InputStream, ObjectInput, ObjectOutput}
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import com.google.common.primitives.UnsignedBytes
import io.netty.handler.stream.ChunkedStream

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.util.{ByteArrayWritableChannel, LimitedInputStream}
import org.apache.spark.storage.{EncryptedManagedBuffer, StorageUtils}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils

/**
 * 分块存储的只读字节缓冲区，数据物理存储为多个独立块而非单个连续数组，避免大块内存分配问题
 *
 * @param chunks 存储数据的ByteBuffer数组，每个块初始position必须为0。
 *               数组所有权转移给ChunkedByteBuffer，调用方若需要在其他地方使用需要自行复制
 */
private[spark] class ChunkedByteBuffer(var chunks: Array[ByteBuffer]) extends Externalizable {
  require(chunks != null, "chunks must not be null")
  require(!chunks.contains(null), "chunks must not contain null")
  require(chunks.forall(_.position() == 0), "chunks' positions must be 0")

  // 写操作的块大小配置，从Spark环境读取配置，默认值使用配置默认值
  private val bufferWriteChunkSize =
    Option(SparkEnv.get).map(_.conf.get(config.BUFFER_WRITE_CHUNK_SIZE))
      .getOrElse(config.BUFFER_WRITE_CHUNK_SIZE.defaultValue.get).toInt

  private[this] var disposed: Boolean = false

  /**
   * 缓冲区总字节大小，使用var是为了序列化支持（无参构造后需要赋值）
   */
  private var _size: Long = chunks.map(_.limit().asInstanceOf[Long]).sum

  def size: Long = _size

  def this() = {
    this(Array.empty[ByteBuffer])
  }

  def this(byteBuffer: ByteBuffer) = {
    this(Array(byteBuffer))
  }

  /**
   * 将整个缓冲区的数据写入指定通道
   */
  def writeFully(channel: WritableByteChannel): Unit = {
    for (bytes <- getChunks()) {
      val originalLimit = bytes.limit()
      while (bytes.hasRemaining) {
        // 对于堆内ByteBuffer，Java NIO写入时会自动复制到临时直接缓冲区，该缓冲区按线程缓存且会持续增长
        // 若不限制单次写入大小，可能导致直接内存泄漏（直到线程退出才释放）
        // 这里通过固定大小分片限制单次写入，从而限制缓存的直接缓冲区大小
        // 详情参考 http://www.evanjones.ca/java-bytebuffer-leak.html
        val ioSize = Math.min(bytes.remaining(), bufferWriteChunkSize)
        bytes.limit(bytes.position() + ioSize)
        channel.write(bytes)
        bytes.limit(originalLimit)
      }
    }
  }

  /**
   * 序列化方法，尽可能零拷贝写入ObjectOutput
   */
  override def writeExternal(out: ObjectOutput): Unit = {
    // 保留原有的分块结构
    out.writeInt(chunks.length)
    val chunksCopy = getChunks()
    chunksCopy.foreach(buffer => out.writeInt(buffer.limit()))
    chunksCopy.foreach(Utils.writeByteBuffer(_, out))
  }

  override def readExternal(in: ObjectInput): Unit = {
    val chunksNum = in.readInt()
    val indices = 0 until chunksNum
    val chunksSize = indices.map(_ => in.readInt())
    val chunks = new Array[ByteBuffer](chunksNum)

    // 默认反序列化为堆内缓冲区，如果后续需要保留原堆内/堆外属性，需要在序列化时记录每个块的isDirect属性
    indices.foreach { i =>
      val chunkSize = chunksSize(i)
      chunks(i) = {
        val arr = new Array[Byte](chunkSize)
        in.readFully(arr, 0, chunkSize)
        ByteBuffer.wrap(arr)
      }
    }
    this.chunks = chunks
    this._size = chunks.map(_.limit().toLong).sum
  }

  /**
   * 封装为自定义Netty FileRegion，支持传输超过2GB的数据
   */
  def toNetty: ChunkedByteBufferFileRegion = {
    new ChunkedByteBufferFileRegion(this, bufferWriteChunkSize)
  }

  /**
   * 封装为Netty ChunkedStream，兼容SSL加密传输场景
   */
  def toNettyForSsl: ChunkedStream = {
    new ChunkedStream(toInputStream(), bufferWriteChunkSize)
  }

  /**
   * 将缓冲区所有数据复制到新的字节数组
   *
   * @throws UnsupportedOperationException 当缓冲区大小超过最大数组长度限制时抛出
   */
  def toArray: Array[Byte] = {
    if (size >= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      throw new UnsupportedOperationException(
        s"cannot call toArray because buffer size ($size bytes) exceeds maximum array size")
    }
    val byteChannel = new ByteArrayWritableChannel(size.toInt)
    writeFully(byteChannel)
    byteChannel.close()
    byteChannel.getData
  }

  /**
   * 将缓冲区转换为单个ByteBuffer。单块时直接复用原有缓冲区不复制，多块时复制数据到新缓冲区。
   * 建议仅在调用方不需要管理底层内存时使用该方法。
   *
   * @throws UnsupportedOperationException 当缓冲区大小超过ByteBuffer最大限制时抛出
   */
  def toByteBuffer: ByteBuffer = {
    if (chunks.length == 1) {
      chunks.head.duplicate()
    } else {
      ByteBuffer.wrap(toArray)
    }
  }

  /**
   * 创建读取当前分块缓冲区数据的输入流
   *
   * @param dispose 如果为true，流读取结束后会调用dispose()方法关闭缓冲区 backing的内存映射文件
   */
  def toInputStream(dispose: Boolean = false): InputStream = {
    new ChunkedByteBufferInputStream(this, dispose)
  }

  /**
   * 获取所有 backing 块的重复副本（不复制数据，仅复制ByteBuffer对象）
   */
  def getChunks(): Array[ByteBuffer] = {
    chunks.map(_.duplicate())
  }

  /**
   * 复制整个分块缓冲区，所有 backing 数据都复制到新缓冲区，新缓冲区与原缓冲区无共享资源
   *
   * @param allocator 分配新ByteBuffer的函数
   */
  def copy(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val copiedChunks = getChunks().map { chunk =>
      val newChunk = allocator(chunk.limit())
      newChunk.put(chunk)
      newChunk.flip()
      newChunk
    }
    new ChunkedByteBuffer(copiedChunks)
  }

  /**
   * 尝试清理缓冲区中直接内存或内存映射缓冲区，详见[[StorageUtils.dispose]]
   */
  def dispose(): Unit = {
    if (!disposed) {
      chunks.foreach(StorageUtils.dispose)
      disposed = true
    }
  }

}

/**
 * ChunkedByteBuffer工厂类，提供从不同数据源创建分块缓冲区的方法
 */
private[spark] object ChunkedByteBuffer {
  // 默认分块大小 1MB
  private val CHUNK_BUFFER_SIZE: Int = 1024 * 1024
  // 最小分块大小 1KB
  private val MINIMUM_CHUNK_BUFFER_SIZE: Int = 1024

  /**
   * 从Spark ManagedBuffer创建分块ByteBuffer，根据缓冲区类型选择不同实现
   */
  def fromManagedBuffer(data: ManagedBuffer): ChunkedByteBuffer = {
    data match {
      case f: FileSegmentManagedBuffer =>
        fromFile(f.getFile, f.getOffset, f.getLength)
      case e: EncryptedManagedBuffer =>
        e.blockData.toChunkedByteBuffer(ByteBuffer.allocate _)
      case other =>
        new ChunkedByteBuffer(other.nioByteBuffer())
    }
  }

  /**
   * 从整个文件创建分块ByteBuffer
   */
  def fromFile(file: File): ChunkedByteBuffer = {
    fromFile(file, 0, file.length())
  }

  /**
   * 从文件指定偏移和长度创建分块ByteBuffer
   */
  private def fromFile(
      file: File,
      offset: Long,
      length: Long): ChunkedByteBuffer = {
    // 这里不使用内存映射，因为如果将缓冲区放入内存存储，Spark目前不支持内存映射缓冲区的生命周期管理
    // 内存映射缓冲区会和其他内存管理组件冲突，详见SPARK-25422
    val is = new FileInputStream(file)
    is.skipNBytes(offset)
    val in = new LimitedInputStream(is, length)
    val chunkSize = math.min(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH, length).toInt
    val out = new ChunkedByteBufferOutputStream(chunkSize, ByteBuffer.allocate _)
    Utils.tryWithSafeFinally {
      in.transferTo(out)
    } {
      in.close()
      out.close()
    }
    out.toChunkedByteBuffer
  }

  /**
   * 根据预估数据大小估算合适的分块大小，避免分块过大浪费内存或分块过小导致过多分段
   */
  def estimateBufferChunkSize(estimatedSize: Long = -1): Int = {
    if (estimatedSize < 0) {
      CHUNK_BUFFER_SIZE
    } else {
      Math.max(Math.min(estimatedSize, CHUNK_BUFFER_SIZE).toInt, MINIMUM_CHUNK_BUFFER_SIZE)
    }
  }
}

/**
 * 从ChunkedByteBuffer读取数据的输入流实现
 *
 * @param chunkedByteBuffer 要读取的分块缓冲区
 * @param dispose 如果为true，流关闭时会调用分块缓冲区的dispose方法释放内存映射资源
 */
private[spark] class ChunkedByteBufferInputStream(
    var chunkedByteBuffer: ChunkedByteBuffer,
    dispose: Boolean)
  extends InputStream {

  // 过滤掉空块，因为read方法假设所有块都是非空的
  private[this] var chunks = chunkedByteBuffer.getChunks().filter(_.hasRemaining).iterator
  private[this] var currentChunk: ByteBuffer = {
    if (chunks.hasNext) {
      chunks.next()
    } else {
      null
    }
  }

  override def available(): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      currentChunk.remaining
    } else {
      0
    }
  }

  override def read(): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      UnsignedBytes.toInt(currentChunk.get())
    } else {
      close()
      -1
    }
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (currentChunk != null && !currentChunk.hasRemaining && chunks.hasNext) {
      currentChunk = chunks.next()
    }
    if (currentChunk != null && currentChunk.hasRemaining) {
      val amountToGet = math.min(currentChunk.remaining(), length)
      currentChunk.get(dest, offset, amountToGet)
      amountToGet
    } else {
      close()
      -1
    }
  }

  override def skip(bytes: Long): Long = {
    if (currentChunk != null) {
      val amountToSkip = math.min(bytes, currentChunk.remaining).toInt
      currentChunk.position(currentChunk.position() + amountToSkip)
      if (currentChunk.remaining() == 0) {
        if (chunks.hasNext) {
          currentChunk = chunks.next()
        } else {
          close()
        }
      }
      amountToSkip
    } else {
      0L
    }
  }

  override def close(): Unit = {
    if (chunkedByteBuffer != null && dispose) {
      chunkedByteBuffer.dispose()
    }
    chunkedByteBuffer = null
    chunks = null
    currentChunk = null
  }
}
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

import java.io.OutputStream
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.StorageUtils

/**
 * 文件: ChunkedByteBufferOutputStream.scala
 * 所属模块: Spark core 公共IO工具模块
 * 核心职责: 实现分块内存输出流，将写入的数据切分为固定大小的内存块存储，避免大块内存分配开销和内存碎片，
 *          最终可以转换为ChunkedByteBuffer，用于Spark块管理中存储序列化后的中间结果和数据块。
 *
 * 一个分块字节缓冲输出流，将数据写入固定大小的字节数组块中
 *
 * @param chunkSize 每个内存块的大小，单位字节
 * @param allocator 分配指定大小ByteBuffer的函数，支持自定义分配方式（堆内/堆外内存）
 */
private[spark] class ChunkedByteBufferOutputStream(
    chunkSize: Int,
    allocator: Int => ByteBuffer)
  extends OutputStream {

  // 标记是否已经调用过toChunkedByteBuffer方法，该方法只能调用一次
  private[this] var toChunkedByteBufferWasCalled = false

  // 存储所有已分配内存块的数组缓冲
  private val chunks = new ArrayBuffer[ByteBuffer]

  /** 最后一个块的索引，当块数组为空时初始化为-1 */
  private[this] var lastChunkIndex = -1

  /**
   * 最后一个块中下一个可写入位置的偏移量
   *
   * 如果该值等于chunkSize，表示下一次写入需要分配新块。该值永远不会为0。
   */
  private[this] var position = chunkSize
  // 当前输出流已写入的总字节数
  private[this] var _size = 0L
  // 标记输出流是否已关闭
  private[this] var closed: Boolean = false

  /** 获取当前已写入的总字节数 */
  def size: Long = _size

  /** 关闭输出流，标记无法再写入 */
  override def close(): Unit = {
    if (!closed) {
      super.close()
      closed = true
    }
  }

  /** 写入单个字节到输出流 */
  override def write(b: Int): Unit = {
    require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
    // 空间不足时分配新块
    allocateNewChunkIfNeeded()
    // 写入字节到当前最后一个块
    chunks(lastChunkIndex).put(b.toByte)
    position += 1
    _size += 1
  }

  /** 批量写入字节数组的指定范围到输出流 */
  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    require(!closed, "cannot write to a closed ChunkedByteBufferOutputStream")
    var written = 0
    // 循环写入直到所有请求字节都写完
    while (written < len) {
      allocateNewChunkIfNeeded()
      // 计算当前块可以写入的最大字节数
      val thisBatch = math.min(chunkSize - position, len - written)
      chunks(lastChunkIndex).put(bytes, written + off, thisBatch)
      written += thisBatch
      position += thisBatch
    }
    _size += len
  }

  /**
   * 如果当前块已满，分配新的内存块
   */
  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    if (position == chunkSize) {
      chunks += allocator(chunkSize)
      lastChunkIndex += 1
      position = 0
    }
  }

  /**
   * 将写入的数据组装为ChunkedByteBuffer，用于后续读取
   * 最后一个不满的块会重新分配精确大小的缓冲，避免空间浪费
   *
   * @return 组装完成的分块字节缓冲对象
   */
  def toChunkedByteBuffer: ChunkedByteBuffer = {
    require(closed, "cannot call toChunkedByteBuffer() unless close() has been called")
    require(!toChunkedByteBufferWasCalled, "toChunkedByteBuffer() can only be called once")
    toChunkedByteBufferWasCalled = true
    if (lastChunkIndex == -1) {
      // 没有写入任何数据，返回空分块缓冲
      new ChunkedByteBuffer(Array.empty[ByteBuffer])
    } else {
      // 复制前n-1个块到结果数组，对最后一个块做裁剪处理以节省空间
      // 不直接使用原缓冲并修改limit的原因是：块管理器仍然会存储整个原始大小的块，导致空间浪费
      val ret = new Array[ByteBuffer](chunks.size)
      for (i <- 0 until chunks.size - 1) {
        ret(i) = chunks(i)
        // 翻转缓冲，准备读取
        ret(i).flip()
      }
      if (position == chunkSize) {
        // 最后一个块刚好写满，直接使用
        ret(lastChunkIndex) = chunks(lastChunkIndex)
        ret(lastChunkIndex).flip()
      } else {
        // 最后一个块不满，分配精确大小的新缓冲，复制数据后释放原缓冲
        ret(lastChunkIndex) = allocator(position)
        chunks(lastChunkIndex).flip()
        ret(lastChunkIndex).put(chunks(lastChunkIndex))
        ret(lastChunkIndex).flip()
        StorageUtils.dispose(chunks(lastChunkIndex))
      }
      new ChunkedByteBuffer(ret)
    }
  }

}
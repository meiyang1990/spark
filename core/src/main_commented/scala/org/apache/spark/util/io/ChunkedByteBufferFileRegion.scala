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

import java.nio.channels.WritableByteChannel

import org.apache.spark.network.util.AbstractFileRegion


/**
 * 文件级注释：将分块字节缓冲区包装为Netty的FileRegion实现，解决Netty无法直接发送超过2GB的ByteBuf问题
 * 虽然该实现不依赖实际文件存储，但可以利用Netty对大FileRegion的支持完成超大数据块的单次网络传输
 */
private[io] class ChunkedByteBufferFileRegion(
    private val chunkedByteBuffer: ChunkedByteBuffer,
    private val ioChunkSize: Int) extends AbstractFileRegion {

  // 记录已传输的字节总数
  private var _transferred: Long = 0
  // 复制原始分块数组，可自由修改每个缓冲区的position/limit等属性
  private val chunks = chunkedByteBuffer.getChunks()
  // 计算所有分块的总字节数
  private val size = chunks.foldLeft(0L) { _ + _.remaining() }

  /** 释放资源方法，本实现无需额外释放资源 */
  protected def deallocate: Unit = {}

  override def count(): Long = size

  // 返回总数据在底层文件的起始偏移，此处数据不基于文件，固定返回0
  override def position(): Long = 0

  override def transferred(): Long = _transferred

  // 当前正在处理的分块索引
  private var currentChunkIdx = 0

  /**
   * 将数据传输到目标可写字节通道
   * @param target 目标输出通道
   * @param position 开始传输的位置，必须等于已传输字节数
   * @return 本次传输的字节数
   */
  def transferTo(target: WritableByteChannel, position: Long): Long = {
    assert(position == _transferred)
    // 所有数据已经传输完成，返回0
    if (position == size) return 0L
    var keepGoing = true
    var written = 0L
    var currentChunk = chunks(currentChunkIdx)
    while (keepGoing) {
      while (currentChunk.hasRemaining && keepGoing) {
        // 计算本次IO操作的传输大小，不超过配置的IO块大小和当前分块剩余大小
        val ioSize = Math.min(currentChunk.remaining(), ioChunkSize)
        // 保存原限制，传输后恢复
        val originalLimit = currentChunk.limit()
        // 修改缓冲区限制，限定本次传输范围
        currentChunk.limit(currentChunk.position() + ioSize)
        // 执行实际写入操作
        val thisWriteSize = target.write(currentChunk)
        // 恢复原限制
        currentChunk.limit(originalLimit)
        // 累计本次写入字节数
        written += thisWriteSize
        // 通道未接受全部数据，停止传输，等待Netty下次调度
        if (thisWriteSize < ioSize) {
          keepGoing = false
        }
      }
      if (keepGoing) {
        // 当前分块传输完成，切换到下一分块
        currentChunkIdx += 1
        // 所有分块传输完成，结束循环
        if (currentChunkIdx == chunks.length) {
          keepGoing = false
        } else {
          currentChunk = chunks(currentChunkIdx)
        }
      }
    }
    // 更新总传输字节数
    _transferred += written
    written
  }
}
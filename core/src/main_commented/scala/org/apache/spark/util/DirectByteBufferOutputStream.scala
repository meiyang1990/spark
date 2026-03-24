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

package org.apache.spark.util

import java.io.OutputStream
import java.nio.ByteBuffer

import org.apache.spark.SparkException
import org.apache.spark.storage.StorageUtils
import org.apache.spark.unsafe.Platform

/**
 * 基于堆外直接内存实现的可自动扩容输出流，用于向直接字节缓冲区写入数据
 * 随着数据写入，缓冲区容量会自动扩容，最终可转换为只读ByteBuffer输出
 * 
 * @param capacity 直接字节缓冲区的初始容量
 */
private[spark] class DirectByteBufferOutputStream(capacity: Int) extends OutputStream {
  // 当前存储数据的直接字节缓冲区
  private[this] var buffer = Platform.allocateDirectBuffer(capacity)

  def this() = this(32)

  override def write(b: Int): Unit = {
    // 检查流是否已关闭
    checkNotClosed()
    // 确保缓冲区有足够空间容纳新增数据
    ensureCapacity(buffer.position() + 1)
    // 写入单个字节
    buffer.put(b.toByte)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    checkNotClosed()
    ensureCapacity(buffer.position() + len)
    // 批量写入字节数组指定区间的数据
    buffer.put(b, off, len)
  }

  private def ensureCapacity(minCapacity: Int): Unit = {
    // 如果需要的最小容量超过当前容量，则进行扩容
    if (minCapacity > buffer.capacity()) grow(minCapacity)
  }

  /**
   * 将当前缓冲区扩容至不小于minCapacity，旧缓冲区会被释放，引用失效
   */
  private def grow(minCapacity: Int): Unit = {
    val oldCapacity = buffer.capacity()
    // 默认按原容量2倍扩容
    var newCapacity = oldCapacity << 1
    // 如果2倍扩容后仍不满足最小需求，则直接使用最小需求作为新容量
    if (newCapacity < minCapacity) newCapacity = minCapacity
    // 保存旧缓冲区引用
    val oldBuffer = buffer
    // 切换旧缓冲区为读模式，准备复制数据
    oldBuffer.flip()
    // 分配新的更大容量直接缓冲区
    val newBuffer = Platform.allocateDirectBuffer(newCapacity)
    // 复制旧缓冲区数据到新缓冲区
    newBuffer.put(oldBuffer)
    // 释放旧缓冲区的堆外内存，避免等待GC
    StorageUtils.dispose(oldBuffer)
    // 更新当前缓冲区引用为新缓冲区
    buffer = newBuffer
  }

  private def checkNotClosed(): Unit = {
    // 如果缓冲区引用为null说明流已关闭，抛出内部错误
    if (buffer == null) {
      throw SparkException.internalError(
        "Cannot call methods on a closed DirectByteBufferOutputStream")
    }
  }

  /**
   * 重置缓冲区位置，可复用当前缓冲区写入新数据
   */
  def reset(): Unit = {
    checkNotClosed()
    // 清除缓冲区状态，重置位置为0，限制为容量
    buffer.clear()
  }

  /**
   * 获取当前已写入数据的大小
   * @return 已写入字节数
   */
  def size(): Int = {
    checkNotClosed()
    buffer.position()
  }

  /**
   * 将当前写入的数据转换为只读ByteBuffer返回
   * 注意：调用此方法后，后续对close()/write()/reset()的调用都会使返回的缓冲区失效
   * @return 包含当前所有写入数据的只读ByteBuffer
   */
  def toByteBuffer: ByteBuffer = {
    checkNotClosed()
    // 复制当前缓冲区的位置和标记信息
    val outputBuffer = buffer.duplicate()
    // 切换为读模式，限制设为当前位置，位置重置为0
    outputBuffer.flip()
    outputBuffer
  }

  override def close(): Unit = {
    // 主动释放直接缓冲区内存，不等待GC回收，降低内存压力
    StorageUtils.dispose(buffer)
    // 将缓冲区引用置空，表示流已关闭
    buffer = null
  }

}
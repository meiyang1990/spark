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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * 文件级：Spark核心工具模块提供的零拷贝字节缓冲区输出流，用于将ByteArrayOutputStream的数据零拷贝转换为ByteBuffer
 *
 * 提供零拷贝方式将ByteArrayOutputStream中已写入的数据转换为ByteBuffer，避免不必要的数据拷贝
 */
private[spark] class ByteBufferOutputStream(capacity: Int) extends ByteArrayOutputStream(capacity) {

  /**
   * 默认构造方法，使用初始容量32创建输出流
   */
  def this() = this(32)

  /**
   * 获取当前已写入的字节数
   * @return 已写入字节总数
   */
  def getCount(): Int = count

  // 标记流是否已关闭，禁止写入后转换
  private[this] var closed: Boolean = false

  /**
   * 写入单个字节到输出流，检查流关闭状态
   * @param b 待写入字节
   */
  override def write(b: Int): Unit = {
    require(!closed, "cannot write to a closed ByteBufferOutputStream")
    super.write(b)
  }

  /**
   * 批量写入字节数组到输出流，检查流关闭状态
   * @param b 待写入字节数组
   * @param off 起始偏移量
   * @param len 写入长度
   */
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    require(!closed, "cannot write to a closed ByteBufferOutputStream")
    super.write(b, off, len)
  }

  /**
   * 重置输出流，清空已写入数据，检查流关闭状态
   */
  override def reset(): Unit = {
    require(!closed, "cannot reset a closed ByteBufferOutputStream")
    super.reset()
  }

  /**
   * 关闭输出流，关闭后禁止写入，仅可转换为ByteBuffer
   */
  override def close(): Unit = {
    if (!closed) {
      super.close()
      closed = true
    }
  }

  /**
   * 将已写入的数据零拷贝包装为ByteBuffer，仅在流关闭后可调用
   * @return 包装了当前缓冲区数据的ByteBuffer
   */
  def toByteBuffer: ByteBuffer = {
    require(closed, "can only call toByteBuffer() after ByteBufferOutputStream has been closed")
    ByteBuffer.wrap(buf, 0, count)
  }
}
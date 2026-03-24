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

import java.io.InputStream
import java.nio.ByteBuffer

/**
 * 文件级注释：ByteBuffer输入流实现，将NIO ByteBuffer包装为标准Java IO InputStream，供Spark内部需要使用InputStream接口的场景使用
 * 包装NIO的ByteBuffer，提供标准Java IO输入流接口，方便通过InputStream方式读取ByteBuffer中的数据
 * 仅在Spark核心工具类内部可见
 */
/**
 * Reads data from a ByteBuffer.
 */
private[spark]
class ByteBufferInputStream(private var buffer: ByteBuffer)
  extends InputStream {

  /**
   * 从ByteBuffer读取单个字节，实现InputStream标准读取方法
   * @return 读取到的无符号字节值，流结束返回-1
   */
  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  /**
   * 读取多个字节填充到整个字节数组，实现InputStream标准批量读取方法
   * @param dest 目标字节数组
   * @return 实际读取的字节数，流结束返回-1
   */
  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  /**
   * 从指定位置开始读取指定长度的字节到目标数组，实现InputStream标准批量读取方法
   * @param dest 目标字节数组
   * @param offset 目标数组起始偏移
   * @param length 需要读取的字节数
   * @return 实际读取的字节数，流结束返回-1
   */
  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      // 不超过缓冲区剩余容量和请求长度，取较小值
      val amountToGet = math.min(buffer.remaining(), length)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  /**
   * 跳过指定数量的字节，移动缓冲区位置
   * @param bytes 需要跳过的字节数
   * @return 实际跳过的字节数
   */
  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      // 不超过缓冲区剩余容量，取较小值
      val amountToSkip = math.min(bytes, buffer.remaining).toInt
      // 更新缓冲区当前位置，完成跳过
      buffer.position(buffer.position() + amountToSkip)
      if (buffer.remaining() == 0) {
        cleanUp()
      }
      amountToSkip
    } else {
      0L
    }
  }

  /**
   * 清理缓冲区引用，释放对底层缓冲区引用，读取完毕后释放内存引用帮助GC回收
   */
  private def cleanUp(): Unit = {
    if (buffer != null) {
      buffer = null
    }
  }
}
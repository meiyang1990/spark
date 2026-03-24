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

import java.io.{EOFException, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

/**
 * 可序列化的ByteBuffer包装类，通过Java序列化实现ByteBuffer的序列化，
 * 方便在样例类消息中传递ByteBuffer对象
 *
 * @param buffer 被包装的原始ByteBuffer对象，标记为transient避免默认序列化
 */
private[spark]
class SerializableBuffer(@transient var buffer: ByteBuffer) extends Serializable {
  def value: ByteBuffer = buffer

  /**
   * 自定义Java反序列化方法，从输入流中读取字节数据重建ByteBuffer
   * @param in Java对象输入流
   */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    // 读取缓冲区总长度
    val length = in.readInt()
    // 分配指定长度的堆内存缓冲区
    buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    // 将输入流包装为可读取字节的通道
    val channel = Channels.newChannel(in)
    // 循环读取直到填满整个缓冲区
    while (amountRead < length) {
      // 从通道读取数据到缓冲区
      val ret = channel.read(buffer)
      // 提前读到流末尾，抛出异常
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      // 累加已读取字节数
      amountRead += ret
    }
    // 重置缓冲区位置指针到开头，方便后续读取
    buffer.rewind() // Allow us to read it later
  }

  /**
   * 自定义Java序列化方法，将ByteBuffer内容写入输出流
   * @param out Java对象输出流
   */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 先写出缓冲区总长度
    out.writeInt(buffer.limit())
    // 将缓冲区内容通过通道写入输出流，校验是否完整写出
    if (Channels.newChannel(out).write(buffer) != buffer.limit()) {
      throw new IOException("Could not fully write buffer to output stream")
    }
    // 重置缓冲区位置指针到开头，方便后续重复写入
    buffer.rewind() // Allow us to write it again later
  }
}
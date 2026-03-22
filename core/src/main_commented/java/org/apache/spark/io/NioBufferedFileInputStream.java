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
package org.apache.spark.io;

import org.apache.spark.storage.StorageUtils;
import org.apache.spark.unsafe.Platform;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * 基于NIO直接内存实现的带缓冲文件输入流
 * <p>
 * 相比JDK自带的BufferedInputStream，避免了Java堆内存和Native内存之间的数据拷贝开销，
 * 提升大文件读取性能。JDK自带的ChannelInputStream不支持数据缓冲，该实现弥补了这一不足。
 */
public final class NioBufferedFileInputStream extends InputStream {

  // 垃圾回收清理器，用于自动释放不再使用的直接内存资源
  private static final Cleaner CLEANER = Cleaner.create();
  // 默认缓冲区大小，单位字节
  private static final int DEFAULT_BUFFER_SIZE_BYTES = 8192;

  // 清理器注册的清理句柄，用于触发资源释放
  private final Cleaner.Cleanable cleanable;

  // NIO直接内存缓冲区，存储读取到的文件数据
  private final ByteBuffer byteBuffer;

  // 文件通道，用于底层文件读取
  private final FileChannel fileChannel;

  /**
   * 构造方法，指定文件和自定义缓冲区大小创建NIO缓冲输入流
   *
   * @param file 待读取的文件
   * @param bufferSizeInBytes 缓冲区大小，单位字节
   * @throws IOException 文件打开或初始化失败时抛出IO异常
   */
  public NioBufferedFileInputStream(File file, int bufferSizeInBytes) throws IOException {
    byteBuffer = Platform.allocateDirectBuffer(bufferSizeInBytes);
    fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    byteBuffer.flip();
    this.cleanable = CLEANER.register(this, new ResourceCleaner(fileChannel, byteBuffer));
  }

  /**
   * 构造方法，使用默认缓冲区大小创建NIO缓冲输入流
   *
   * @param file 待读取的文件
   * @throws IOException 文件打开或初始化失败时抛出IO异常
   */
  public NioBufferedFileInputStream(File file) throws IOException {
    this(file, DEFAULT_BUFFER_SIZE_BYTES);
  }

  /**
   * 当缓冲区数据耗尽时，从文件读取新的数据填充缓冲区
   *
   * @return true 成功填充了数据；false 已到达文件末尾
   * @throws IOException 文件读取失败时抛出IO异常
   */
  private boolean refill() throws IOException {
    if (!byteBuffer.hasRemaining()) {
      byteBuffer.clear();
      int nRead = 0;
      while (nRead == 0) {
        nRead = fileChannel.read(byteBuffer);
      }
      byteBuffer.flip();
      if (nRead < 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized int read() throws IOException {
    if (!refill()) {
      return -1;
    }
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public synchronized int read(byte[] b, int offset, int len) throws IOException {
    if (offset < 0 || len < 0 || offset + len < 0 || offset + len > b.length) {
      throw new IndexOutOfBoundsException();
    }
    if (!refill()) {
      return -1;
    }
    len = Math.min(len, byteBuffer.remaining());
    byteBuffer.get(b, offset, len);
    return len;
  }

  @Override
  public synchronized int available() throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (n <= 0L) {
      return 0L;
    }
    if (byteBuffer.remaining() >= n) {
      // 缓冲区剩余数据足够跳过，直接移动缓冲区指针
      byteBuffer.position(byteBuffer.position() + (int) n);
      return n;
    }
    long skippedFromBuffer = byteBuffer.remaining();
    long toSkipFromFileChannel = n - skippedFromBuffer;
    // 清空缓冲区所有已读数据
    byteBuffer.position(0);
    byteBuffer.flip();
    return skippedFromBuffer + skipFromFileChannel(toSkipFromFileChannel);
  }

  /**
   * 在文件通道上直接跳过指定字节数
   *
   * @param n 需要跳过的字节数
   * @return 实际跳过的字节数
   * @throws IOException 文件位置操作失败时抛出IO异常
   */
  private long skipFromFileChannel(long n) throws IOException {
    long currentFilePosition = fileChannel.position();
    long size = fileChannel.size();
    if (n > size - currentFilePosition) {
      // 跳过长度超过文件剩余大小，直接跳到文件末尾
      fileChannel.position(size);
      return size - currentFilePosition;
    } else {
      // 直接移动文件指针跳过指定长度
      fileChannel.position(currentFilePosition + n);
      return n;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      // 触发资源清理，关闭文件通道并释放直接缓冲区
      this.cleanable.clean();
    } catch (UncheckedIOException re) {
      if (re.getCause() != null) {
        throw re.getCause();
      } else {
        throw re;
      }
    }
  }

  /**
   * 资源清理记录，用于 Cleaner 自动回收未关闭的直接缓冲区和文件通道
   *
   * @param fileChannel 需要关闭的文件通道
   * @param byteBuffer 需要释放的直接字节缓冲区
   */
  private record ResourceCleaner(
      FileChannel fileChannel,
      ByteBuffer byteBuffer) implements Runnable {
    @Override
    public void run() {
      try {
        fileChannel.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } finally {
        // 无论关闭是否成功，都要释放直接缓冲区内存
        StorageUtils.dispose(byteBuffer);
      }
    }
  }
}
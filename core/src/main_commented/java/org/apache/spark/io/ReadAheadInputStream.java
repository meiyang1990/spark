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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.util.ThreadUtils;

/**
 * 带异步预读功能的输入流实现，通过双缓冲机制避免读操作被磁盘I/O阻塞。
 * 维护两个缓冲区：当前活动缓冲区（供读调用返回数据）和预读缓冲区（异步从底层输入流读取数据），
 * 当活动缓冲区耗尽时交换两个缓冲区，可直接使用预读好的数据消除I/O阻塞延迟。
 */
public class ReadAheadInputStream extends InputStream {

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(ReadAheadInputStream.class);

  private ReentrantLock stateChangeLock = new ReentrantLock();

  @GuardedBy("stateChangeLock")
  // 当前供读取操作返回数据的活动缓冲区
  private ByteBuffer activeBuffer;

  @GuardedBy("stateChangeLock")
  // 异步预读数据的预读缓冲区
  private ByteBuffer readAheadBuffer;

  @GuardedBy("stateChangeLock")
  // 是否已到达底层输入流末尾
  private boolean endOfStream;

  @GuardedBy("stateChangeLock")
  // 异步读是否正在进行中
  private boolean readInProgress;

  @GuardedBy("stateChangeLock")
  // 是否因底层输入流读取异常导致预读中止
  private boolean readAborted;

  @GuardedBy("stateChangeLock")
  // 预读过程中捕获的异常
  private Throwable readException;

  @GuardedBy("stateChangeLock")
  // close方法是否已被调用
  private boolean isClosed;

  @GuardedBy("stateChangeLock")
  // close方法是否正在关闭底层输入流，仅当isClosed为true时有效
  private boolean isUnderlyingInputStreamBeingClosed;

  @GuardedBy("stateChangeLock")
  // 是否有预读任务正在运行
  private boolean isReading;

  // 是否有读取线程正在等待预读完成
  private AtomicBoolean isWaiting = new AtomicBoolean(false);

  private final InputStream underlyingInputStream;

  private final ExecutorService executorService =
      ThreadUtils.newDaemonSingleThreadExecutor("read-ahead");

  // 异步读完成条件变量，用于通知等待的读取线程
  private final Condition asyncReadComplete = stateChangeLock.newCondition();

  /**
   * 创建预读输入流，指定底层输入流和缓冲区大小
   *
   * @param inputStream 底层输入流
   * @param bufferSizeInBytes 缓冲区大小（字节），活动缓冲区和预读缓冲区各分配该大小
   */
  public ReadAheadInputStream(
      InputStream inputStream, int bufferSizeInBytes) {
    JavaUtils.checkArgument(bufferSizeInBytes > 0,
        "bufferSizeInBytes should be greater than 0, but the value is " + bufferSizeInBytes);
    activeBuffer = ByteBuffer.allocate(bufferSizeInBytes);
    readAheadBuffer = ByteBuffer.allocate(bufferSizeInBytes);
    this.underlyingInputStream = inputStream;
    activeBuffer.flip();
    readAheadBuffer.flip();
  }

  /**
   * 检查是否已到达流末尾：两个缓冲区都耗尽且已到流结束
   */
  private boolean isEndOfStream() {
    return (!activeBuffer.hasRemaining() && !readAheadBuffer.hasRemaining() && endOfStream);
  }

  /**
   * 检查预读是否已中止，如果中止则抛出对应异常
   * @throws IOException 预读过程中发生的IO异常
   */
  private void checkReadException() throws IOException {
    if (readAborted) {
      if (readException == null) throw new NullPointerException("readException is not captured.");
      if (readException instanceof IOException ie) throw ie;
      if (readException instanceof Error error) throw error;
      if (readException instanceof RuntimeException re) throw re;
      throw new IOException(readException);
    }
  }

  /**
   * 触发异步任务，从底层输入流读取数据到预读缓冲区
   * @throws IOException 预读启动失败时抛出IO异常
   */
  private void readAsync() throws IOException {
    stateChangeLock.lock();
    final byte[] arr = readAheadBuffer.array();
    try {
      // 已到流末尾或已有异步读在进行则跳过
      if (endOfStream || readInProgress) {
        return;
      }
      checkReadException();
      // 重置预读缓冲区准备写入新数据
      readAheadBuffer.position(0);
      readAheadBuffer.flip();
      readInProgress = true;
    } finally {
      stateChangeLock.unlock();
    }
    // 提交异步读任务到线程池
    executorService.execute(() -> {
      stateChangeLock.lock();
      try {
        if (isClosed) {
          readInProgress = false;
          return;
        }
        // 标记当前正在读，避免关闭过程直接关闭底层流
        isReading = true;
      } finally {
        stateChangeLock.unlock();
      }

      // 释放锁后安全读取预读缓冲区，不会产生竞态条件：
      // 1. 活动缓冲区还有数据时，读取线程不会碰预读缓冲区
      // 2. 第一次读或活动缓冲区已耗尽时，读取线程会等待异步读完成
      int read = 0;
      int off = 0, len = arr.length;
      Throwable exception = null;
      try {
        // 尝试填满预读缓冲区，如果有读取线程等待则提前返回
        do {
          read = underlyingInputStream.read(arr, off, len);
          if (read <= 0) break;
          off += read;
          len -= read;
        } while (len > 0 && !isWaiting.get());
      } catch (Throwable ex) {
        exception = ex;
        if (ex instanceof Error error) {
          // Error直接抛出，确保能被未捕获异常处理器处理
          throw error;
        }
      } finally {
        stateChangeLock.lock();
        // 设置预读缓冲区的有效数据长度
        readAheadBuffer.limit(off);
        if (read < 0 || (exception instanceof EOFException)) {
          // 到达流末尾
          endOfStream = true;
        } else if (exception != null) {
          // 读取异常，标记预读中止
          readAborted = true;
          readException = exception;
        }
        readInProgress = false;
        // 通知等待的读取线程预读完成
        signalAsyncReadComplete();
        stateChangeLock.unlock();
        // 检查是否需要关闭底层流
        closeUnderlyingInputStreamIfNecessary();
      }
    });
  }

  /**
   * 如果已调用close且当前无预读任务，则关闭底层输入流
   */
  private void closeUnderlyingInputStreamIfNecessary() {
    boolean needToCloseUnderlyingInputStream = false;
    stateChangeLock.lock();
    try {
      isReading = false;
      if (isClosed && !isUnderlyingInputStreamBeingClosed) {
        // 之前因为正在读无法关闭，现在可以关闭了
        needToCloseUnderlyingInputStream = true;
      }
    } finally {
      stateChangeLock.unlock();
    }
    if (needToCloseUnderlyingInputStream) {
      try {
        underlyingInputStream.close();
      } catch (IOException e) {
        logger.warn("{}", e, MDC.of(LogKeys.ERROR, e.getMessage()));
      }
    }
  }

  /**
   * 发送异步读完成信号，唤醒等待的线程
   */
  private void signalAsyncReadComplete() {
    stateChangeLock.lock();
    try {
      asyncReadComplete.signalAll();
    } finally {
      stateChangeLock.unlock();
    }
  }

  /**
   * 阻塞等待异步预读完成，处理中断和异常
   * @throws IOException 等待中断或预读异常时抛出IO异常
   */
  private void waitForAsyncReadComplete() throws IOException {
    stateChangeLock.lock();
    isWaiting.set(true);
    try {
      // 循环等待避免虚假唤醒
      while (readInProgress) {
        asyncReadComplete.await();
      }
    } catch (InterruptedException e) {
      InterruptedIOException iio = new InterruptedIOException(e.getMessage());
      iio.initCause(e);
      throw iio;
    } finally {
      isWaiting.set(false);
      stateChangeLock.unlock();
    }
    checkReadException();
  }

  @Override
  public int read() throws IOException {
    if (activeBuffer.hasRemaining()) {
      // 快速路径：直接从活动缓冲区读取一个字节
      return activeBuffer.get() & 0xFF;
    } else {
      byte[] oneByteArray = new byte[1];
      return read(oneByteArray, 0, 1) == -1 ? -1 : oneByteArray[0] & 0xFF;
    }
  }

  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    // 参数合法性检查
    if (offset < 0 || len < 0 || len > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }

    if (!activeBuffer.hasRemaining()) {
      // 活动缓冲区已耗尽，加锁切换到预读好的缓冲区
      stateChangeLock.lock();
      try {
        // 等待异步预读完成
        waitForAsyncReadComplete();
        if (!readAheadBuffer.hasRemaining()) {
          // 第一次读取，触发预读并等待完成
          readAsync();
          waitForAsyncReadComplete();
          if (isEndOfStream()) {
            return -1;
          }
        }
        // 交换缓冲区，将预读好的数据作为新的活动缓冲区
        swapBuffers();
        // 交换完成后触发下一次异步预读
        readAsync();
      } finally {
        stateChangeLock.unlock();
      }
    }
    // 从活动缓冲区读取最多len字节数据
    len = Math.min(len, activeBuffer.remaining());
    activeBuffer.get(b, offset, len);

    return len;
  }

  /**
   * 交换活动缓冲区和预读缓冲区
   */
  private void swapBuffers() {
    ByteBuffer temp = activeBuffer;
    activeBuffer = readAheadBuffer;
    readAheadBuffer = temp;
  }

  @Override
  public int available() throws IOException {
    stateChangeLock.lock();
    // 避免整数溢出，计算两个缓冲区剩余可用字节总和
    try {
      return (int) Math.min(Integer.MAX_VALUE,
          (long) activeBuffer.remaining() + readAheadBuffer.remaining());
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0L) {
      return 0L;
    }
    if (n <= activeBuffer.remaining()) {
      // 仅需跳过活动缓冲区中部分数据，直接修改position即可
      activeBuffer.position((int) n + activeBuffer.position());
      return n;
    }
    stateChangeLock.lock();
    long skipped;
    try {
      skipped = skipInternal(n);
    } finally {
      stateChangeLock.unlock();
    }
    return skipped;
  }

  /**
   * skip操作内部实现，要求调用者已持有stateChangeLock锁
   * @param n 需要跳过的字节数
   * @return 实际跳过的字节数
   * @throws IOException IO异常时抛出
   */
  private long skipInternal(long n) throws IOException {
    assert (stateChangeLock.isLocked());
    waitForAsyncReadComplete();
    if (isEndOfStream()) {
      return 0;
    }
    if (available() >= n) {
      // 可以直接从两个内部缓冲区完成跳过
      int toSkip = (int) n;
      // 先耗尽活动缓冲区剩余数据
      toSkip -= activeBuffer.remaining();
      assert(toSkip > 0); // 活动缓冲区已处理了部分跳过，这里肯定大于0
      activeBuffer.position(0);
      activeBuffer.flip();
      // 从预读缓冲区跳过剩余需要跳过的字节
      readAheadBuffer.position(toSkip + readAheadBuffer.position());
      // 交换缓冲区
      swapBuffers();
      // 触发新的异步预读到空出来的预读缓冲区
      readAsync();
      return n;
    } else {
      // 内部缓冲区不够，跳过全部缓冲数据，剩余部分从底层输入流跳过
      int skippedBytes = available();
      long toSkip = n - skippedBytes;
      // 清空两个缓冲区
      activeBuffer.position(0);
      activeBuffer.flip();
      readAheadBuffer.position(0);
      readAheadBuffer.flip();
      // 从底层输入流跳过剩余字节
      long skippedFromInputStream = underlyingInputStream.skip(toSkip);
      // 触发新的异步预读
      readAsync();
      return skippedBytes + skippedFromInputStream;
    }
  }

  @Override
  public void close() throws IOException {
    boolean isSafeToCloseUnderlyingInputStream = false;
    stateChangeLock.lock();
    try {
      if (isClosed) {
        return;
      }
      isClosed = true;
      if (!isReading) {
        // 没有正在进行的预读，可以直接在当前方法关闭底层流
        isSafeToCloseUnderlyingInputStream = true;
        // 标记底层流正在关闭，避免异步任务后续重复关闭
        isUnderlyingInputStreamBeingClosed = true;
      }
    } finally {
      stateChangeLock.unlock();
    }

    try {
      // 关闭线程池，等待所有任务完成
      executorService.shutdownNow();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      InterruptedIOException iio = new InterruptedIOException(e.getMessage());
      iio.initCause(e);
      throw iio;
    } finally {
      // 如果可以安全关闭则关闭底层输入流
      if (isSafeToCloseUnderlyingInputStream) {
        underlyingInputStream.close();
      }
    }
  }
}
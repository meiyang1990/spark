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

package org.apache.spark.util.collection.unsafe.sort;

import com.google.common.io.Closeables;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.io.ReadAheadInputStream;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.unsafe.Platform;

import java.io.*;

/**
 * 读取由 {@link UnsafeSorterSpillWriter} 写入的溢写文件，用于外部排序流程中读取已排序溢写数据
 * 该类读取的文件格式说明详见 {@link UnsafeSorterSpillWriter}
 */
public final class UnsafeSorterSpillReader extends UnsafeSorterIterator implements Closeable {
  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(UnsafeSorterSpillReader.class);
  // 最大缓冲区大小，16MB
  public static final int MAX_BUFFER_SIZE_BYTES = 16777216; // 16 mb

  private InputStream in;
  private DataInputStream din;

  // 每条记录读取时会更新这些状态变量
  private int recordLength;
  private long keyPrefix;
  // 溢写文件中总记录数
  private int numRecords;
  // 剩余未读取记录数
  private int numRecordsRemaining;

  private byte[] arr = new byte[1024 * 1024];
  private Object baseObject = arr;
  private final TaskContext taskContext = TaskContext.get();

  /**
   * 构造Unsafe排序溢写文件读取器，打开并初始化溢写文件读取流
   * @param serializerManager 序列化管理器，用于包装输入流
   * @param file 溢写磁盘文件
   * @param blockId 存储块ID，用于序列化管理器识别块
   * @throws IOException 文件读取或初始化失败时抛出异常
   */
  public UnsafeSorterSpillReader(
      SerializerManager serializerManager,
      File file,
      BlockId blockId) throws IOException {
    assert (file.length() > 0);
    final ConfigEntry<Object> bufferSizeConfigEntry =
        package$.MODULE$.UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE();
    // 配置值一定小于等于MAX_BUFFER_SIZE_BYTES，强转int安全
    final int DEFAULT_BUFFER_SIZE_BYTES =
        ((Long) bufferSizeConfigEntry.defaultValue().get()).intValue();
    // 根据Spark环境配置获取缓冲区大小，无环境则使用默认值
    int bufferSizeBytes = SparkEnv.get() == null ? DEFAULT_BUFFER_SIZE_BYTES :
        ((Long) SparkEnv.get().conf().get(bufferSizeConfigEntry)).intValue();

    // 从配置获取是否启用预读优化
    final boolean readAheadEnabled = SparkEnv.get() != null && (boolean)SparkEnv.get().conf().get(
        package$.MODULE$.UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED());

    // 创建基于NIO的缓冲文件输入流
    final InputStream bs =
        new NioBufferedFileInputStream(file, bufferSizeBytes);
    try {
      // 根据配置决定是否包装预读输入流
      if (readAheadEnabled) {
        this.in = new ReadAheadInputStream(serializerManager.wrapStream(blockId, bs),
                bufferSizeBytes);
      } else {
        this.in = serializerManager.wrapStream(blockId, bs);
      }
      // 包装为数据输入流，方便读取基本类型
      this.din = new DataInputStream(this.in);
      // 读取文件头中的总记录数，初始化剩余计数
      numRecords = numRecordsRemaining = din.readInt();
    } catch (IOException e) {
      // 发生异常时关闭流，吞掉关闭异常避免覆盖原异常
      Closeables.close(bs, /* swallowIOException = */ true);
      throw e;
    }
    // 任务完成时自动关闭流，避免资源泄漏
    if (taskContext != null) {
      taskContext.addTaskCompletionListener(context -> {
        try {
          close();
        } catch (IOException e) {
          logger.info("error while closing UnsafeSorterSpillReader", e);
        }
      });
    }
  }

  @Override
  public int getNumRecords() {
    return numRecords;
  }

  @Override
  public long getCurrentPageNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    return (numRecordsRemaining > 0);
  }

  @Override
  public void loadNext() throws IOException {
    // 如果任务已被中断杀死，直接抛出异常终止读取
    // 该逻辑内联在这里避免包装迭代器带来性能开销
    // 放在loadNext而不是hasNext中，因为调用方可能使用getNumRecords判断结束
    if (taskContext != null) {
      taskContext.killTaskIfInterrupted();
    }
    // 读取当前记录长度
    recordLength = din.readInt();
    // 读取当前记录的前缀键（用于排序比较）
    keyPrefix = din.readLong();
    // 如果当前缓冲区不够容纳该记录，扩容缓冲区
    if (recordLength > arr.length) {
      arr = new byte[recordLength];
      baseObject = arr;
    }
    // 读取完整记录字节到缓冲区
    JavaUtils.readFully(in, arr, 0, recordLength);
    // 剩余记录数减一
    numRecordsRemaining--;
    // 读取完所有记录后自动关闭流释放资源
    if (numRecordsRemaining == 0) {
      close();
    }
  }

  @Override
  public Object getBaseObject() {
    return baseObject;
  }

  @Override
  public long getBaseOffset() {
    return Platform.BYTE_ARRAY_OFFSET;
  }

  @Override
  public int getRecordLength() {
    return recordLength;
  }

  @Override
  public long getKeyPrefix() {
    return keyPrefix;
  }

  @Override
  public void close() throws IOException {
   if (in != null) {
     try {
       in.close();
     } finally {
       // 置空避免重复关闭
       in = null;
       din = null;
     }
   }
  }
}
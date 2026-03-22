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

package org.apache.spark.shuffle.sort.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;

/**
 * 文件级注释：实现了将Map阶段Shuffle输出持久化到本地磁盘的Writer，兼容Spark传统的Shuffle存储机制，
 * 会同时生成数据文件和索引文件，用于sort-based shuffle的输出存储。
 *
 * Implementation of {@link ShuffleMapOutputWriter} that replicates the functionality of shuffle
 * persisting shuffle data to local disk alongside index files, identical to Spark's historic
 * canonical shuffle storage mechanism.
 */
/**
 * 本地磁盘Shuffle Map输出写入器，实现ShuffleMap输出写入接口，将Map任务的Shuffle输出写入本地磁盘
 * 兼容Spark传统的Shuffle存储格式，同时生成数据文件和分区索引文件
 */
public class LocalDiskShuffleMapOutputWriter implements ShuffleMapOutputWriter {

  private static final SparkLogger log =
    SparkLoggerFactory.getLogger(LocalDiskShuffleMapOutputWriter.class);

  private final int shuffleId;
  private final long mapId;
  private final IndexShuffleBlockResolver blockResolver;
  private final long[] partitionLengths;
  private final int bufferSize;
  private int lastPartitionId = -1;
  private long currChannelPosition;
  private long bytesWrittenToMergedFile = 0L;

  private final File outputFile;
  private File outputTempFile;
  private FileOutputStream outputFileStream;
  private FileChannel outputFileChannel;
  private BufferedOutputStream outputBufferedFileStream;

  /**
   * 构造函数，初始化本地磁盘Shuffle Map输出写入器
   * @param shuffleId Shuffle阶段ID
   * @param mapId Map任务ID
   * @param numPartitions 分区数量（对应Reduce分区数）
   * @param blockResolver Shuffle块解析器，用于处理文件和索引
   * @param sparkConf Spark配置对象
   */
  public LocalDiskShuffleMapOutputWriter(
      int shuffleId,
      long mapId,
      int numPartitions,
      IndexShuffleBlockResolver blockResolver,
      SparkConf sparkConf) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
    this.bufferSize =
      (int) (long) sparkConf.get(
        package$.MODULE$.SHUFFLE_LOCAL_DISK_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    this.partitionLengths = new long[numPartitions];
    this.outputFile = blockResolver.getDataFile(shuffleId, mapId);
    this.outputTempFile = null;
  }

  @Override
  /**
   * 获取指定Reduce分区的写入器，按递增顺序请求分区
   * @param reducePartitionId Reduce分区ID
   * @return 对应分区的写入器
   * @throws IOException 如果IO操作失败或分区顺序不正确抛出异常
   */
  public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException {
    if (reducePartitionId <= lastPartitionId) {
      throw new IllegalArgumentException("Partitions should be requested in increasing order.");
    }
    lastPartitionId = reducePartitionId;
    if (outputTempFile == null) {
      // 第一次获取分区写入器时创建临时文件
      outputTempFile = blockResolver.createTempFile(outputFile);
    }
    if (outputFileChannel != null) {
      // 记录当前文件通道位置，用于计算当前分区写入大小
      currChannelPosition = outputFileChannel.position();
    } else {
      currChannelPosition = 0L;
    }
    return new LocalDiskShufflePartitionWriter(reducePartitionId);
  }

  @Override
  /**
   * 提交所有分区的输出，完成文件写入并生成索引文件
   * @param checksums 分区校验和数组
   * @return Map输出提交消息，包含各分区长度信息
   * @throws IOException IO操作失败时抛出异常
   */
  public MapOutputCommitMessage commitAllPartitions(long[] checksums) throws IOException {
    // 检查transferTo后文件位置是否正确，兼容Linux内核2.6.32的transferTo bug
    // Check the position after transferTo loop to see if it is in the right position and raise a
    // exception if it is incorrect. The position will not be increased to the expected length
    // after calling transferTo in kernel version 2.6.32. This issue is described at
    // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
    if (outputFileChannel != null && outputFileChannel.position() != bytesWrittenToMergedFile) {
      throw new IOException(
          "Current position " + outputFileChannel.position() + " does not equal expected " +
              "position " + bytesWrittenToMergedFile + " after transferTo. Please check your " +
              " kernel version to see if it is 2.6.32, as there is a kernel bug which will lead " +
              "to unexpected behavior when using transferTo. You can set " +
              "spark.file.transferTo=false to disable this NIO feature.");
    }
    // 关闭所有打开的流和通道
    cleanUp();
    File resolvedTmp = outputTempFile != null && outputTempFile.isFile() ? outputTempFile : null;
    log.debug("Writing shuffle index file for mapId {} with length {}", mapId,
        partitionLengths.length);
    // 写入元数据索引文件并提交临时文件到最终位置
    blockResolver
      .writeMetadataFileAndCommit(shuffleId, mapId, partitionLengths, checksums, resolvedTmp);
    // 返回提交消息，包含所有分区长度
    return MapOutputCommitMessage.of(partitionLengths);
  }

  @Override
  /**
   * 中止输出，清理临时文件和资源
   * @param error 导致中止的错误
   * @throws IOException IO清理操作失败时抛出异常
   */
  public void abort(Throwable error) throws IOException {
    // 关闭所有打开的资源
    cleanUp();
    if (outputTempFile != null && outputTempFile.exists() && !outputTempFile.delete()) {
      log.warn("Failed to delete temporary shuffle file at {}",
        MDC.of(LogKeys.PATH, outputTempFile.getAbsolutePath()));
    }
  }

  /**
   * 清理所有打开的IO资源，关闭流和通道
   * @throws IOException 关闭资源失败时抛出异常
   */
  private void cleanUp() throws IOException {
    if (outputBufferedFileStream != null) {
      outputBufferedFileStream.close();
    }
    if (outputFileChannel != null) {
      outputFileChannel.close();
    }
    if (outputFileStream != null) {
      outputFileStream.close();
    }
  }

  /**
   * 懒初始化输出流，创建缓冲输出流
   * @throws IOException 创建流失败时抛出异常
   */
  private void initStream() throws IOException {
    if (outputFileStream == null) {
      // 以追加模式打开临时文件输出流
      outputFileStream = new FileOutputStream(outputTempFile, true);
    }
    if (outputBufferedFileStream == null) {
      // 使用配置的缓冲区大小创建缓冲输出流
      outputBufferedFileStream = new BufferedOutputStream(outputFileStream, bufferSize);
    }
  }

  /**
   * 懒初始化文件通道，用于零拷贝传输
   * @throws IOException 创建通道失败时抛出异常
   */
  private void initChannel() throws IOException {
    // 以追加模式打开文件以规避Linux内核transferTo bug，详见SPARK-3948
    // This file needs to opened in append mode in order to work around a Linux kernel bug that
    // affects transferTo; see SPARK-3948 for more details.
    if (outputFileChannel == null) {
      outputFileChannel = new FileOutputStream(outputTempFile, true).getChannel();
    }
  }

  /**
   * 本地磁盘Shuffle分区写入器，内部类，负责单个Reduce分区的数据写入
   */
  private class LocalDiskShufflePartitionWriter implements ShufflePartitionWriter {

    private final int partitionId;
    private PartitionWriterStream partStream = null;
    private PartitionWriterChannel partChannel = null;

    private LocalDiskShufflePartitionWriter(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    /**
     * 打开输出流写入模式，返回分区输出流
     * @return 分区输出流
     * @throws IOException 初始化流失败或已使用通道模式时抛出异常
     */
    public OutputStream openStream() throws IOException {
      if (partStream == null) {
        if (outputFileChannel != null) {
          throw new IllegalStateException("Requested an output channel for a previous write but" +
              " now an output stream has been requested. Should not be using both channels" +
              " and streams to write.");
        }
        initStream();
        partStream = new PartitionWriterStream(partitionId);
      }
      return partStream;
    }

    @Override
    /**
     * 打开通道写入模式，返回可写字节通道包装器
     * @return 通道包装器Optional实例
     * @throws IOException 初始化通道失败或已使用流模式时抛出异常
     */
    public Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
      if (partChannel == null) {
        if (partStream != null) {
          throw new IllegalStateException("Requested an output stream for a previous write but" +
              " now an output channel has been requested. Should not be using both channels" +
              " and streams to write.");
        }
        initChannel();
        partChannel = new PartitionWriterChannel(partitionId);
      }
      return Optional.of(partChannel);
    }

    @Override
    /**
     * 获取当前分区已写入的字节数
     * @return 已写入字节数
     */
    public long getNumBytesWritten() {
      if (partChannel != null) {
        try {
          return partChannel.getCount();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (partStream != null) {
        return partStream.getCount();
      } else {
        // 如果从未创建流或通道，默认是一个空分区
        // Assume an empty partition if stream and channel are never created
        return 0;
      }
    }
  }

  /**
   * 分区输出流实现，继承OutputStream，负责统计写入字节数
   */
  private class PartitionWriterStream extends OutputStream {
    private final int partitionId;
    private long count = 0;
    private boolean isClosed = false;

    PartitionWriterStream(int partitionId) {
      this.partitionId = partitionId;
    }

    public long getCount() {
      return count;
    }

    @Override
    public void write(int b) throws IOException {
      verifyNotClosed();
      outputBufferedFileStream.write(b);
      count++;
    }

    @Override
    public void write(byte[] buf, int pos, int length) throws IOException {
      verifyNotClosed();
      outputBufferedFileStream.write(buf, pos, length);
      count += length;
    }

    @Override
    public void close() {
      isClosed = true;
      // 记录分区长度到分区长度数组
      partitionLengths[partitionId] = count;
      // 累加到总写入字节数
      bytesWrittenToMergedFile += count;
    }

    /**
     * 校验流是否已关闭，防止向已关闭流写入数据
     */
    private void verifyNotClosed() {
      if (isClosed) {
        throw new IllegalStateException("Attempting to write to a closed block output stream.");
      }
    }
  }

  /**
   * 分区通道写入实现，实现WritableByteChannelWrapper，支持零拷贝写入
   */
  private class PartitionWriterChannel implements WritableByteChannelWrapper {

    private final int partitionId;

    PartitionWriterChannel(int partitionId) {
      this.partitionId = partitionId;
    }

    /**
     * 获取当前分区已写入字节数，通过文件位置差计算
     * @return 已写入字节数
     * @throws IOException 获取文件位置失败时抛出异常
     */
    public long getCount() throws IOException {
      long writtenPosition = outputFileChannel.position();
      return writtenPosition - currChannelPosition;
    }

    @Override
    public WritableByteChannel channel() {
      return outputFileChannel;
    }

    @Override
    public void close() throws IOException {
      // 计算并记录分区长度
      partitionLengths[partitionId] = getCount();
      // 累加到总写入字节数
      bytesWrittenToMergedFile += partitionLengths[partitionId];
    }
  }
}
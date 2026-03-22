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

package org.apache.spark.shuffle.sort;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.zip.Checksum;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.checksum.RowBasedChecksum;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * 绕过合并排序的Shuffle写入器，实现基于哈希分区绕过排序的Shuffle写入路径
 * 该类是排序Shuffle的哈希式降级路径，将记录直接写入每个Reduce分区对应的独立临时文件，最后合并所有临时文件为单个输出文件
 * 该路径不进行排序和Map端聚合，仅适用于Reduce分区数量较少的场景，避免了排序开销但需要同时打开多个文件句柄
 * 仅在满足两个条件时被SortShuffleManager选中：
 * <ul>
 *    <li>没有指定Map端聚合</li>
 *    <li>Reduce分区数量小于等于配置项<code>spark.shuffle.sort.bypassMergeThreshold</code></li>
 * </ul>
 * 该路径原来属于ExternalSorter，为了降低代码复杂度重构为独立类，见SPARK-7855
 * 曾有提议完全移除该路径，详见SPARK-6026
 */
final class BypassMergeSortShuffleWriter<K, V>
  extends ShuffleWriter<K, V>
  implements ShuffleChecksumSupport {

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final long mapId;
  private final Serializer serializer;
  private final ShuffleExecutorComponents shuffleExecutorComponents;

  /** 每个分区对应一个文件写入器数组 */
  private DiskBlockObjectWriter[] partitionWriters;
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;
  /** 每个分区的校验和计算器，禁用Shuffle校验和时为空数组 */
  private final Checksum[] partitionChecksums;
  /**
   * 基于行的校验和计算器，与普通校验和不同，该校验和与输入行顺序无关
   * 用于检测同一分区不同任务尝试是否输出了不同的数据
   */
  private final RowBasedChecksum[] rowBasedChecksums;

  /**
   * 标记是否正在停止写入过程，由于Map任务可能先调用stop(true)成功后又因异常调用stop(false)
   * 需要避免重复执行文件删除等清理操作
   */
  private boolean stopping = false;

  /**
   * 构造绕过合并排序的Shuffle写入器
   * @param blockManager 块管理器，用于管理磁盘块存储
   * @param handle 绕过合并排序Shuffle的句柄，包含Shuffle依赖信息
   * @param mapId 当前Map任务ID
   * @param conf Spark配置
   * @param writeMetrics Shuffle写入指标报告器，用于统计写入性能
   * @param shuffleExecutorComponents Shuffle执行组件，用于创建输出写入器
   * @throws SparkException 构造过程异常
   */
  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      BypassMergeSortShuffleHandle<K, V> handle,
      long mapId,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.transferToEnabled = (boolean) conf.get(package$.MODULE$.SHUFFLE_MERGE_PREFER_NIO());
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = writeMetrics;
    this.serializer = dep.serializer();
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
    this.rowBasedChecksums = dep.rowBasedChecksums();
  }

  /**
   * 写入所有排序后的记录，按照分区将记录写入临时文件，最后合并为最终输出
   * @param records 输入记录迭代器
   * @throws IOException 写入过程IO异常
   */
  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    ShuffleMapOutputWriter mapOutputWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, numPartitions);
    try {
      if (!records.hasNext()) {
        // 无记录，直接提交空分区输出
        partitionLengths = mapOutputWriter.commitAllPartitions(
          ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE).getPartitionLengths();
        mapStatus = MapStatus$.MODULE$.apply(
          blockManager.shuffleServerId(), partitionLengths, mapId, getAggregatedChecksumValue());
        return;
      }
      final SerializerInstance serInstance = serializer.newInstance();
      final long openStartTime = System.nanoTime();
      // 初始化每个分区的写入器数组
      partitionWriters = new DiskBlockObjectWriter[numPartitions];
      partitionWriterSegments = new FileSegment[numPartitions];
      // 为每个分区创建临时文件和磁盘写入器
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
            blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = tempShuffleBlockIdPlusFile._2();
        final BlockId blockId = tempShuffleBlockIdPlusFile._1();
        DiskBlockObjectWriter writer =
          blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
        // 设置校验和计算器
        if (partitionChecksums.length > 0) {
          writer.setChecksum(partitionChecksums[i]);
        }
        partitionWriters[i] = writer;
      }
      // 打开文件和创建写入器涉及磁盘IO，多个文件打开耗时需要计入Shuffle写入时间
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

      // 遍历所有输入记录，按分区写入对应临时文件
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        // 根据key计算分区ID
        final int partitionId = partitioner.getPartition(key);
        partitionWriters[partitionId].write(key, record._2());
        // 更新基于行的校验和
        if (rowBasedChecksums.length > 0) {
          rowBasedChecksums[partitionId].update(key, record._2());
        }
      }

      // 提交每个分区写入，获取写入文件段信息
      for (int i = 0; i < numPartitions; i++) {
        try (DiskBlockObjectWriter writer = partitionWriters[i]) {
          partitionWriterSegments[i] = writer.commitAndGet();
        }
      }

      // 合并所有分区临时文件到最终输出文件，获取各分区长度数组
      partitionLengths = writePartitionedData(mapOutputWriter);
      // 创建Map任务状态，记录分区长度等信息
      mapStatus = MapStatus$.MODULE$.apply(
        blockManager.shuffleServerId(), partitionLengths, mapId, getAggregatedChecksumValue());
    } catch (Exception e) {
      // 写入异常，中止输出并清理
      try {
        mapOutputWriter.abort(e);
      } catch (Exception e2) {
        logger.error("Failed to abort the writer after failing to write map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  @Override
  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  // 仅用于测试
  @VisibleForTesting
  RowBasedChecksum[] getRowBasedChecksums() {
    return rowBasedChecksums;
  }

  @VisibleForTesting
  long getAggregatedChecksumValue() {
    return RowBasedChecksum.getAggregatedChecksumValue(rowBasedChecksums);
  }

  /**
   * 将所有分区临时文件合并拼接为单个最终输出文件
   * @param mapOutputWriter Map输出写入器，负责写入最终输出
   * @return 每个分区在输出文件中的长度数组，供Map输出跟踪使用
   * @throws IO 合并过程IO异常
   */
  private long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter) throws IOException {
    // Track location of the partition starts in the output file
    if (partitionWriters != null) {
      final long writeStartTime = System.nanoTime();
      try {
        // 遍历所有分区，依次合并到输出文件
        for (int i = 0; i < numPartitions; i++) {
          final File file = partitionWriterSegments[i].file();
          ShufflePartitionWriter writer = mapOutputWriter.getPartitionWriter(i);
          if (file.exists()) {
            if (transferToEnabled) {
              // 使用WritableByteChannelWrapper保证资源关闭逻辑与UnsafeShuffleWriter一致
              Optional<WritableByteChannelWrapper> maybeOutputChannel = writer.openChannelWrapper();
              if (maybeOutputChannel.isPresent()) {
                // 使用NIO通道零拷贝方式传输数据
                writePartitionedDataWithChannel(file, maybeOutputChannel.get());
              } else {
                // 通道打开失败，回退到流方式传输
                writePartitionedDataWithStream(file, writer);
              }
            } else {
              // NIO传输禁用，使用流方式传输
              writePartitionedDataWithStream(file, writer);
            }
            // 合并完成删除临时文件
            if (!file.delete()) {
              logger.error("Unable to delete file for partition {}",
                MDC.of(LogKeys.PARTITION_ID, i));
            }
          }
        }
      } finally {
        writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
      }
      partitionWriters = null;
    }
    // 提交所有分区，返回分区长度数组
    return mapOutputWriter.commitAllPartitions(getChecksumValues(partitionChecksums))
      .getPartitionLengths();
  }

  /**
   * 使用NIO文件通道零拷贝方式将分区临时文件内容写入输出通道
   * @param file 分区临时文件
   * @param outputChannel 输出可写通道包装器
   * @throws IOException 传输过程IO异常
   */
  private void writePartitionedDataWithChannel(
      File file,
      WritableByteChannelWrapper outputChannel) throws IOException {
    boolean copyThrewException = true;
    try {
      FileInputStream in = new FileInputStream(file);
      try (FileChannel inputChannel = in.getChannel()) {
        Utils.copyFileStreamNIO(
            inputChannel, outputChannel.channel(), 0L, inputChannel.size());
        copyThrewException = false;
      } finally {
        Closeables.close(in, copyThrewException);
      }
    } finally {
      Closeables.close(outputChannel, copyThrewException);
    }
  }

  /**
   * 使用普通Java流方式将分区临时文件内容写入输出流
   * @param file 分区临时文件
   * @param writer 分区写入器
   * @throws IOException 传输过程IO异常
   */
  private void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
      throws IOException {
    boolean copyThrewException = true;
    FileInputStream in = new FileInputStream(file);
    OutputStream outputStream;
    try {
      outputStream = writer.openStream();
      try {
        Utils.copyStream(in, outputStream, false, false);
        copyThrewException = false;
      } finally {
        Closeables.close(outputStream, copyThrewException);
      }
    } finally {
      Closeables.close(in, copyThrewException);
    }
  }

  /**
   * 停止Shuffle写入，根据成功标志执行清理或返回结果
   * @param success 写入是否成功
   * @return 写入成功返回Map任务状态，失败返回空
   */
  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // Map任务失败，删除所有已生成的输出数据
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // 该方法不会抛出异常
              writer.closeAndDelete();
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
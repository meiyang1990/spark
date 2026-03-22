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

import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.jdk.javaapi.CollectionConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.checksum.RowBasedChecksum;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.ExposedBufferByteArrayOutputStream;
import org.apache.spark.util.Utils;

/**
 * 基于排序的不安全Shuffle写入器，使用堆外内存处理序列化后的记录，实现高效的Shuffle数据排序与输出合并
 * 仅支持序列化模式，适用于reduce分区数不超过上限的场景，通过直接操作二进制数据避免反序列化开销
 */
@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final ShuffleExecutorComponents shuffleExecutorComponents;
  private final int shuffleId;
  private final long mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int mergeBufferSizeInBytes;

  @Nullable private MapStatus mapStatus;
  @Nullable private ShuffleExternalSorter sorter;
  @Nullable private long[] partitionLengths;
  private long peakMemoryUsedBytes = 0;

  private ExposedBufferByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * 每个reduce分区的校验和计算器，校验和与输入记录顺序无关，用于检测同一分区不同任务尝试是否生成不同输出数据
   */
  private final RowBasedChecksum[] rowBasedChecksums;

  /**
   * 标记是否正在执行停止流程，避免重复删除文件等清理操作
   * 处理map任务可能先调用stop(true)后因异常调用stop(false)的场景
   */
  private boolean stopping = false;

  /**
   * 构造UnsafeShuffleWriter实例，初始化相关配置并打开写入器
   * @param blockManager 块管理器，用于存储和管理块数据
   * @param memoryManager 任务内存管理器，负责堆外内存分配与管理
   * @param handle 序列化模式的Shuffle句柄，包含Shuffle依赖信息
   * @param mapId 当前map任务ID
   * @param taskContext 任务上下文，包含任务相关信息
   * @param sparkConf Spark配置对象
   * @param writeMetrics Shuffle写入指标Reporter，用于统计写入指标
   * @param shuffleExecutorComponents Shuffle执行器组件，用于创建输出写入器
   * @throws SparkException 当reduce分区数超过上限时抛出异常
   */
  public UnsafeShuffleWriter(
      BlockManager blockManager,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      long mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) throws SparkException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    // 检查分区数是否超过序列化模式允许的最大分区数
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = writeMetrics;
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_MERGE_PREFER_NIO());
    this.initialSortBufferSize =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
    this.mergeBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_MERGE_BUFFER_SIZE()) * 1024;
    this.rowBasedChecksums = dep.rowBasedChecksums();
    open();
  }

  /**
   * 更新当前使用的峰值内存，从排序器获取最新的峰值内存使用
   */
  private void updatePeakMemoryUsed() {
    // sorter可为空当写入器已关闭
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * 获取迄今为止使用的峰值内存（字节）
   * @return 峰值内存使用量，单位字节
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  // For test only.
  @VisibleForTesting
  RowBasedChecksum[] getRowBasedChecksums() {
    return rowBasedChecksums;
  }

  @VisibleForTesting
  long getAggregatedChecksumValue() {
    return RowBasedChecksum.getAggregatedChecksumValue(rowBasedChecksums);
  }

  /**
   * 仅用于测试的便捷写入方法，将Java迭代器转换为Scala迭代器写入
   * @param records 待写入记录迭代器
   * @throws IOException 写入过程发生IO异常时抛出
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(CollectionConverters.asScala(records));
  }

  /**
   * 写入所有记录到排序器，完成后关闭并输出最终结果
   * @param records 待写入记录的Scala迭代器
   * @throws IOException 写入或合并过程发生IO异常时抛出
   */
  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // 标记是否写入成功，用于处理异常清理场景
    boolean success = false;
    try {
      // 遍历所有记录，插入排序器
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      // 关闭排序器并合并溢出文件输出最终结果
      closeAndWriteOutput();
      success = true;
    } finally {
      // 无论成功失败，都清理排序器占用的资源
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // 仅当原本没有错误时才抛出清理异常，避免掩盖原有错误
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  /**
   * 初始化排序器和序列化缓冲区，打开写入器准备接收记录
   * @throws SparkException 初始化过程发生异常时抛出
   */
  private void open() throws SparkException {
    assert (sorter == null);
    // 创建外部排序器，负责排序和溢出
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      initialSortBufferSize,
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    // 创建序列化缓冲区和序列化输出流
    serBuffer = new ExposedBufferByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    // 更新峰值内存使用
    updatePeakMemoryUsed();
    // 释放序列化缓冲区引用，帮助GC
    serBuffer = null;
    serOutputStream = null;
    // 关闭排序器，获取所有溢出文件信息
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    try {
      // 合并所有溢出文件，得到各分区长度
      partitionLengths = mergeSpills(spills);
    } finally {
      // 清理排序器和所有溢出文件
      sorter = null;
      for (SpillInfo spill : spills) {
        if (spill.file.exists() && !spill.file.delete()) {
          logger.error("Error while deleting spill file {}",
            MDC.of(LogKeys.PATH, spill.file.getPath()));
        }
      }
    }
    // 创建MapStatus，记录该map任务输出分区信息
    mapStatus = MapStatus$.MODULE$.apply(
      blockManager.shuffleServerId(), partitionLengths, mapId, getAggregatedChecksumValue());
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    // 获取记录的键值对
    final K key = record._1();
    // 根据键计算分区ID
    final int partitionId = partitioner.getPartition(key);
    // 重置序列化缓冲区
    serBuffer.reset();
    // 序列化键
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    // 序列化值
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    // 获取序列化后记录大小
    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    // 将序列化后的记录插入排序器
    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
    // 如果启用了校验和，更新对应分区的校验和
    if (rowBasedChecksums.length > 0) {
      rowBasedChecksums[partitionId].update(key, record._2());
    }
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    // 强制排序器将内存数据溢出到磁盘
    sorter.spill();
  }

  /**
   * 根据溢出数量和压缩配置选择最快的合并策略，合并零个或多个溢出文件
   * @param spills 待合并的溢出文件数组
   * @return 合并后每个分区的长度数组
   * @throws IOException 合并过程发生IO异常时抛出
   */
  private long[] mergeSpills(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    if (spills.length == 0) {
      // 没有溢出，直接创建空输出
      final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
          .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
      return mapWriter.commitAllPartitions(
        ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE).getPartitionLengths();
    } else if (spills.length == 1) {
      // 只有一个溢出文件，尝试直接转存不需要重新合并
      Optional<SingleSpillShuffleMapOutputWriter> maybeSingleFileWriter =
          shuffleExecutorComponents.createSingleFileMapOutputWriter(shuffleId, mapId);
      if (maybeSingleFileWriter.isPresent()) {
        // 此处不需要更新指标，因为写入字节已经在溢出时统计过
        partitionLengths = spills[0].partitionLengths;
        logger.debug("Merge shuffle spills for mapId {} with length {}", mapId,
            partitionLengths.length);
        // 直接将溢出文件传输给输出写入器
        maybeSingleFileWriter.get()
          .transferMapSpillFile(spills[0].file, partitionLengths, sorter.getChecksums());
      } else {
        // 不支持单文件直接输出，使用标准合并流程
        partitionLengths = mergeSpillsUsingStandardWriter(spills);
      }
    } else {
      // 多个溢出文件，使用标准合并流程
      partitionLengths = mergeSpillsUsingStandardWriter(spills);
    }
    return partitionLengths;
  }

  /**
   * 使用标准Map输出写入器合并多个溢出文件，根据配置选择不同合并策略
   * @param spills 待合并的溢出文件数组
   * @return 合并后每个分区的长度数组
   * @throws IOException 合并过程发生IO异常时抛出
   */
  private long[] mergeSpillsUsingStandardWriter(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    // 读取Shuffle压缩配置
    final boolean compressionEnabled = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS());
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    // 读取快速合并配置
    final boolean fastMergeEnabled =
        (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FAST_MERGE_ENABLE());
    // 检查压缩编解码器是否支持连接序列化流拼接
    final boolean fastMergeIsSupported = !compressionEnabled ||
        CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    // 检查是否启用加密
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    // 创建map输出写入器
    final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
    try {
      // 满足快速合并条件，使用快速合并
      if (fastMergeEnabled && fastMergeIsSupported) {
        // 压缩未启用或编解码器支持拼接，可直接拼接二进制数据不需要解码重编码
        if (transferToEnabled && !encryptionEnabled) {
          logger.debug("Using transferTo-based fast merge");
          // 使用NIO transferTo零拷贝方式快速合并
          mergeSpillsWithTransferTo(spills, mapWriter);
        } else {
          logger.debug("Using fileStream-based fast merge");
          // 使用文件流方式快速合并
          mergeSpillsWithFileStream(spills, mapWriter, null);
        }
      } else {
        logger.debug("Using slow merge");
        // 不满足快速合并条件，需要解码重编码，使用慢速合并
        mergeSpillsWithFileStream(spills, mapWriter, compressionCodec);
      }
      // 提交所有分区，获取分区长度
      partitionLengths
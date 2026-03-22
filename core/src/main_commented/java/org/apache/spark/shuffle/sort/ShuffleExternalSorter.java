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

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.zip.Checksum;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * 文件: core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java
 * 所属模块: Spark core核心计算模块
 * 核心职责: 排序Shuffle专用的外部排序器，专门为基于排序的Shuffle流程设计，负责将Map端输出的记录按照分区ID排序并溢出到磁盘，不合并溢出文件，合并操作由上层UnsafeShuffleWriter完成。
 * <p>
 * 排序式Shuffle工作流程：
 * 输入记录会追加到内存数据页中，当所有记录插入完成或达到线程Shuffle内存限制时，
 * 会调用内存排序器{@link ShuffleInMemorySorter}对内存中的记录按分区ID排序，
 * 排序后的记录写入单个输出文件（如果多次溢出则生成多个溢出文件）。
 * 输出文件格式与{@link org.apache.spark.shuffle.sort.SortShuffleWriter}写入的最终输出文件格式一致，
 * 每个输出分区的记录以单个序列化压缩流形式存储，可直接通过解压反序列化流读取。
 * <p>
 * 和通用{@link org.apache.spark.util.collection.ExternalSorter}不同，该排序器不对溢出文件进行合并，
 * 合并操作在{@link UnsafeShuffleWriter}中通过专门的合并流程完成，避免了额外的序列化/反序列化开销。
 */
final class ShuffleExternalSorter extends MemoryConsumer implements ShuffleChecksumSupport {

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(ShuffleExternalSorter.class);

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  private final int numPartitions;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final ShuffleWriteMetricsReporter writeMetrics;

  /**
   * 触发强制溢出的内存元素数量阈值，当内存中元素数量达到该值时强制溢出到磁盘
   */
  private final int numElementsForSpillThreshold;

  /**
   * 触发强制溢出的内存大小阈值，当内存使用字节数超过该阈值时强制溢出到磁盘
   */
  private final long sizeInBytesForSpillThreshold;

  /** 使用DiskBlockObjectWriter写入溢出文件时使用的缓冲区大小 */
  private final int fileBufferSizeBytes;

  /** 将排序记录写入磁盘文件时使用的磁盘写缓冲区大小 */
  private final int diskWriteBufferSize;

  /**
   * 存储待排序记录的内存页列表，溢出时这些页会被释放，理论上可以跨溢出复用页，
   * 如果TaskMemoryManager本身维护可复用页池则不需要主动回收。
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /** 该排序器到目前为止使用的峰值内存大小，单位字节 **/
  private long peakMemoryUsedBytes;

  // 这些变量在溢出后会被重置复用
  @Nullable private ShuffleInMemorySorter inMemSorter;
  @Nullable private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long totalPageMemoryUsageBytes = 0;

  // 每个分区对应的校验和计算器，Shuffle校验和禁用时为空数组
  private final Checksum[] partitionChecksums;

  /**
   * 构造Shuffle外部排序器，初始化内存分配、排序器和各类配置参数
   * @param memoryManager 任务内存管理器，负责分配管理该任务的内存
   * @param blockManager 块管理器，负责管理磁盘和内存块存储
   * @param taskContext 当前任务上下文
   * @param initialSize 内存排序器指针数组初始大小
   * @param numPartitions Shuffle总分区数
   * @param conf Spark配置
   * @param writeMetrics Shuffle写metrics记录器
   * @throws SparkException 初始化失败抛出异常
   */
  ShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics) throws SparkException {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.numElementsForSpillThreshold =
        (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
    this.sizeInBytesForSpillThreshold =
        (long) conf.get(package$.MODULE$.SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD());
    this.writeMetrics = writeMetrics;
    this.inMemSorter = new ShuffleInMemorySorter(
      this, initialSize, (boolean) conf.get(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT()));
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.diskWriteBufferSize =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
  }

  /**
   * 获取所有分区的校验和结果数组，用于Shuffle数据校验
   * @return 每个分区校验和组成的数组
   */
  public long[] getChecksums() {
    return getChecksumValues(partitionChecksums);
  }

  /**
   * 对内存中的记录排序，并将排序后的记录写入磁盘文件
   * 该方法不会释放排序数据结构的内存，由调用者负责后续释放
   *
   * @param isFinalFile 如果为true表示正在写入最终输出文件，写入字节计入Shuffle写metrics；
   *                    如果为false表示写入溢出文件，写入字节计入溢出metrics不计入最终Shuffle写metrics
   */
  private void writeSortedFile(boolean isFinalFile) {
    // 仅在实际溢出时打日志，最终输出不打日志
    if (!isFinalFile) {
      logger.info(
        "Task {} on Thread {} spilling sort data of {} to disk ({} {} so far)",
        MDC.of(LogKeys.TASK_ATTEMPT_ID, taskContext.taskAttemptId()),
        MDC.of(LogKeys.THREAD_ID, Thread.currentThread().getId()),
        MDC.of(LogKeys.MEMORY_SIZE, Utils.bytesToString(getMemoryUsage())),
        MDC.of(LogKeys.NUM_SPILLS, spills.size()),
        MDC.of(LogKeys.SPILL_TIMES, spills.size() != 1 ? "times" : "time"));
    }

    // 调用内存排序器执行实际排序，获取排序后的迭代器
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    // 如果没有排序记录，不需要创建空溢出文件直接返回
    if (!sortedRecords.hasNext()) {
      return;
    }

    final ShuffleWriteMetricsReporter writeMetricsToUse;

    if (isFinalFile) {
      // 写入最终非溢出文件，需要将写入计入Shuffle字节指标
      writeMetricsToUse = writeMetrics;
    } else {
      // 写入溢出文件，字节写入应计入溢出指标而非最终Shuffle写指标
      // 使用空指标对象捕获这些指标，不会计入最终Shuffle写，实际合并时才会统计
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    // 直接从托管内存向磁盘写效率较低，通过字节数组缓冲区中转
    // 缓冲区不需要大到能容纳单条记录，分段写入即可
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    // 因为输出文件会在Shuffle阶段被读取，压缩编解码器必须由spark.shuffle.compress控制
    // 而非spark.shuffle.spill.compress，因此需要使用createTempShuffleBlock创建临时块
    // 详见SPARK-3426
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = spilledFileInfo._2();
    final TempShuffleBlockId blockId = spilledFileInfo._1();
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // 构造DiskBlockObjectWriter需要序列化器实例，但我们的写路径直接调用OutputStream的write方法
    // 实际上不会使用这个序列化器，但DiskBlockObjectWriter仍会调用它的方法，因此传入一个空操作序列化器绕开
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;

    int currentPartition = -1;
    final FileSegment committedSegment;
    try (DiskBlockObjectWriter writer =
        blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse)) {
      // 获取Unsafe对齐偏移量的大小，区分32位/64位模式
      final int uaoSize = UnsafeAlignedOffset.getUaoSize();
      while (sortedRecords.hasNext()) {
        sortedRecords.loadNext();
        final int partition = sortedRecords.packedRecordPointer.getPartitionId();
        assert (partition >= currentPartition);
        if (partition != currentPartition) {
          // 切换到新分区，先提交当前分区的写入
          if (currentPartition != -1) {
            final FileSegment fileSegment = writer.commitAndGet();
            spillInfo.partitionLengths[currentPartition] = fileSegment.length();
          }
          currentPartition = partition;
          // 如果启用了校验和，设置当前分区的校验和计算器
          if (partitionChecksums.length > 0) {
            writer.setChecksum(partitionChecksums[currentPartition]);
          }
        }

        // 从打包指针中获取记录指针，解析出记录所在页和页内偏移
        final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
        final Object recordPage = taskMemoryManager.getPage(recordPointer);
        final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
        // 获取记录总长度，跳过长度字段偏移得到实际数据起始位置
        int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
        long recordReadPosition = recordOffsetInPage + uaoSize;
        // 分段将记录从内存页拷贝到写缓冲区，再写入磁盘
        while (dataRemaining > 0) {
          final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);
          Platform.copyMemory(
            recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
          writer.write(writeBuffer, 0, toTransfer);
          recordReadPosition += toTransfer;
          dataRemaining -= toTransfer;
        }
        writer.recordWritten();
      }

      committedSegment = writer.commitAndGet();
    }
    // 如果closeAndGetSpills调用时没有插入过任何记录，文件可能为空，不需要添加到溢出列表
    if (currentPartition != -1) {
      spillInfo.partitionLengths[currentPartition] = committedSegment.length();
      spills.add(spillInfo);
    }

    if (!isFinalFile) {  // 这是一个溢出文件
      // spill写入的记录数和字节数需要同步到全局metrics，但不将溢出字节计入Shuffle写字节
      // 仅将磁盘溢出字节计入任务的diskBytesSpilled指标，和ExternalSorter保持一致，不将溢出IO计入Shuffle写时间
      // 溢出时间单独在SPARK-3577中跟踪
      writeMetrics.incRecordsWritten(
        ((ShuffleWriteMetrics)writeMetricsToUse).recordsWritten());
      taskContext.taskMetrics().incDiskBytesSpilled(
        ((ShuffleWriteMetrics)writeMetricsToUse).bytesWritten());
    }
  }

  /**
   * 响应内存压力，对当前内存中的记录排序并溢出到磁盘，释放占用内存
   * @param size 需要释放的内存大小，单位字节
   * @param trigger 触发本次溢出的内存消费者
   * @return 实际释放的内存大小，单位字节
   * @throws IOException 溢出写入磁盘失败抛出异常
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    writeSortedFile(false);
    final long spillSize = freeMemory();
    inMemSorter.reset();
    // 先释放记录占用的内存页，再重置内存排序器的指针数组，否则无法为指针数组分配新内存
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    return spillSize;
  }

  /**
   * 获取当前排序器总内存使用量，包含排序指针数组和数据页两部分
   * @return 总内存使用量，单位字节
   */
  private long getMemoryUsage() {
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageMemoryUsageBytes;
  }

  /**
   * 更新峰值内存使用记录，如果当前内存使用超过已有峰值则更新
   */
  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * 获取该排序器至今使用的峰值内存大小
   * @return 峰值内存大小，单位字节
   */
  long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * 释放所有已分配的数据页内存，清空分配列表，重置当前页分配指针
   * @return 释放的总内存大小，单位字节
   */
  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
      totalPageMemoryUsageBytes -= block.size();
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * 强制释放所有内存并删除所有溢出文件，由Shuffle错误处理代码在任务失败时调用
   */
  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}",
          MDC.of(LogKeys.PATH
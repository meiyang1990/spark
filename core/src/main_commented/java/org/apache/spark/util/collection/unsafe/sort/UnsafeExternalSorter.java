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

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.MDC;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * 文件：UnsafeExternalSorter.java
 * 功能：基于Unsafe内存操作的外部排序器，在内存不足时将数据溢写磁盘，最终多路归并得到有序结果，
 *      主要用于Shuffle阶段的数据排序，支持基于堆外内存的高效排序，减少GC压力。
 * 核心职责：实现内外存结合的排序，管理内存分配与溢写，提供最终的有序迭代器。
 */
public final class UnsafeExternalSorter extends MemoryConsumer {

  private static final SparkLogger logger =
    SparkLoggerFactory.getLogger(UnsafeExternalSorter.class);

  @Nullable
  private final PrefixComparator prefixComparator;

  /**
   * 记录比较器由工厂方法提供，避免UnsafeExternalSorter持有比较器实例导致内存无法回收，
   * 因为比较器可能持有上次比较记录的引用，而UnsafeExternalSorter被TaskContext引用直到任务结束。
   */
  @Nullable
  private final Supplier<RecordComparator> recordComparatorSupplier;

  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final SerializerManager serializerManager;
  private final TaskContext taskContext;

  /** 溢写磁盘时缓冲区大小（字节） */
  private final int fileBufferSizeBytes;

  /**
   * 强制溢写阈值：内存中元素数量达到该值时触发溢写。
   */
  private final int numElementsForSpillThreshold;

  /**
   * 强制溢写阈值：内存使用字节数超过该值时触发溢写。
   */
  private final long sizeInBytesForSpillThreshold;

  /**
   * 存储待排序记录的内存页，溢写时会释放这些页。
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  /** 保存所有溢写文件的写入器，最终归并时会读取这些文件 */
  private final LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();

  // 溢写后会重置这些变量
  @Nullable private volatile UnsafeInMemorySorter inMemSorter;
  private long totalPageMemoryUsageBytes = 0;

  private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long peakMemoryUsedBytes = 0;
  private long totalSpillBytes = 0L;
  private long totalSortTimeNanos = 0L;
  private volatile SpillableIterator readingIterator = null;

  /**
   * 使用已有的内存内排序器创建外部排序器，用于已有排序结果场景。
   *
   * @param taskMemoryManager 任务内存管理器
   * @param blockManager 块管理器，用于管理磁盘块
   * @param serializerManager 序列化管理器
   * @param taskContext 当前任务上下文
   * @param recordComparatorSupplier 记录比较器工厂
   * @param prefixComparator 前缀比较器，用于前缀排序优化
   * @param initialSize 初始指针数组大小
   * @param pageSizeBytes 内存页大小
   * @param numElementsForSpillThreshold 元素数量溢写阈值
   * @param sizeInBytesForSpillThreshold 内存大小溢写阈值
   * @param inMemorySorter 已有的内存内排序器
   * @param existingMemoryConsumption 已有内存占用
   * @return 创建好的外部排序器实例
   * @throws IOException 溢写失败时抛出IO异常
   */
  public static UnsafeExternalSorter createWithExistingInMemorySorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      long sizeInBytesForSpillThreshold,
      UnsafeInMemorySorter inMemorySorter,
      long existingMemoryConsumption) throws IOException {
    UnsafeExternalSorter sorter = new UnsafeExternalSorter(taskMemoryManager, blockManager,
      serializerManager, taskContext, recordComparatorSupplier, prefixComparator, initialSize,
        pageSizeBytes, numElementsForSpillThreshold, sizeInBytesForSpillThreshold,
        inMemorySorter, false /* ignored */);
    sorter.spill(Long.MAX_VALUE, sorter);
    taskContext.taskMetrics().incMemoryBytesSpilled(existingMemoryConsumption);
    sorter.totalSpillBytes += existingMemoryConsumption;
    // 外部排序器将用于插入新记录，不需要已有的内存排序器
    sorter.inMemSorter = null;
    return sorter;
  }

  /**
   * 创建新的空外部排序器。
   *
   * @param taskMemoryManager 任务内存管理器
   * @param blockManager 块管理器，用于管理磁盘块
   * @param serializerManager 序列化管理器
   * @param taskContext 当前任务上下文
   * @param recordComparatorSupplier 记录比较器工厂
   * @param prefixComparator 前缀比较器，用于前缀排序优化
   * @param initialSize 初始指针数组大小
   * @param pageSizeBytes 内存页大小
   * @param numElementsForSpillThreshold 元素数量溢写阈值
   * @param sizeInBytesForSpillThreshold 内存大小溢写阈值
   * @param canUseRadixSort 是否可以使用基数排序优化
   * @return 创建好的外部排序器实例
   */
  public static UnsafeExternalSorter create(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      long sizeInBytesForSpillThreshold,
      boolean canUseRadixSort) {
    return new UnsafeExternalSorter(taskMemoryManager, blockManager, serializerManager,
      taskContext, recordComparatorSupplier, prefixComparator, initialSize, pageSizeBytes,
      numElementsForSpillThreshold, sizeInBytesForSpillThreshold, null, canUseRadixSort);
  }

  /**
   * 私有构造方法，由静态工厂方法调用创建实例。
   */
  private UnsafeExternalSorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      long sizeInBytesForSpillThreshold,
      @Nullable UnsafeInMemorySorter existingInMemorySorter,
      boolean canUseRadixSort) {
    super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.taskContext = taskContext;
    this.recordComparatorSupplier = recordComparatorSupplier;
    this.prefixComparator = prefixComparator;
    // 保持向后兼容性，默认32k缓冲区
    this.fileBufferSizeBytes = 32 * 1024;

    if (existingInMemorySorter == null) {
      RecordComparator comparator = null;
      if (recordComparatorSupplier != null) {
        comparator = recordComparatorSupplier.get();
      }
      this.inMemSorter = new UnsafeInMemorySorter(
        this,
        taskMemoryManager,
        comparator,
        prefixComparator,
        initialSize,
        canUseRadixSort);
    } else {
      this.inMemSorter = existingInMemorySorter;
    }
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.sizeInBytesForSpillThreshold = sizeInBytesForSpillThreshold;
    this.numElementsForSpillThreshold = numElementsForSpillThreshold;

    // 注册任务完成清理钩子，确保任务结束时释放所有内存和删除溢写文件
    // 避免下游算子没有完全消费输出（例如排序后接limit）导致内存泄漏
    taskContext.addTaskCompletionListener(context -> {
      cleanupResources();
    });
  }

  /**
   * 将当前页标记为已满，下次插入记录时会分配新页或触发溢写。
   */
  @VisibleForTesting
  public void closeCurrentPage() {
    if (currentPage != null) {
      pageCursor = currentPage.getBaseOffset() + currentPage.size();
    }
  }

  /**
   * 响应内存压力，对当前内存中的记录排序并溢写到磁盘，释放内存。
   * 实现MemoryConsumer的spill方法，由内存管理器调用。
   *
   * @param size 需要释放的内存大小（字节）
   * @param trigger 触发本次溢写的内存消费者
   * @return 实际释放的内存大小（字节）
   * @throws IOException 溢写IO失败时抛出异常
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this) {
      if (readingIterator != null) {
        return readingIterator.spill();
      }
      return 0L;
    }

    if (inMemSorter == null || inMemSorter.numRecords() <= 0) {
      // 内存排序器中没有记录，但仍可能有已分配的内存，不过我们不溢写这部分内存
      // 确保至少可以处理一条记录再溢写，详见allocateMemoryForRecordIfNecessary注释
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      MDC.of(LogKeys.THREAD_ID, Thread.currentThread().getId()),
      MDC.of(LogKeys.MEMORY_SIZE, Utils.bytesToString(getMemoryUsage())),
      MDC.of(LogKeys.NUM_SPILL_WRITERS, spillWriters.size()),
      MDC.of(LogKeys.SPILL_TIMES, spillWriters.size() > 1 ? "times" : "time"));

    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();

    final UnsafeSorterSpillWriter spillWriter =
      new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
        inMemSorter.numRecords());
    spillWriters.add(spillWriter);
    // 将排序后的记录写入溢写文件
    spillIterator(inMemSorter.getSortedIterator(), spillWriter);

    final long spillSize = freeMemory();
    // 注意：这里会将页面中的浪费空间也算作溢写内存，同时也包含排序器指针数组的空间
    inMemSorter.freeMemory();
    // 先释放记录所在内存页，再重置内存排序器的指针数组，否则如果任务内存超配，可能无法为指针数组分配内存

    // 更新任务指标
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    taskContext.taskMetrics().incDiskBytesSpilled(writeMetrics.bytesWritten());
    totalSpillBytes += spillSize;
    return spillSize;
  }

  /**
   * 获取排序器总内存使用量，包含数据页和排序器指针数组。
   *
   * @return 总内存使用量（字节）
   */
  private long getMemoryUsage() {
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageMemoryUsageBytes;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * 获取目前为止的峰值内存使用量。
   *
   * @return 峰值内存使用量（字节）
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * 获取内存排序花费的总时间（仅内存内排序）。
   *
   * @return 排序时间（纳秒）
   */
  public long getSortTimeNanos() {
    UnsafeInMemorySorter sorter = inMemSorter;
    if (sorter != null) {
      return sorter.getSortTimeNanos();
    }
    return totalSortTimeNanos;
  }

  /**
   * 获取到目前为止总共溢写到磁盘的字节数。
   *
   * @return 总溢写字节数
   */
  public long getSpillSize() {
    return totalSpillBytes;
  }

  @VisibleForTesting
  public int getNumberOfAllocatedPages() {
    return allocatedPages.size();
  }

  /**
   * 释放所有数据页内存。
   *
   * @return 释放的总字节数
   */
  private long freeMemory() {
    List<MemoryBlock> pagesToFree = clearAndGetAllocatedPagesToFree();
    long memoryFreed = 0;
    for (MemoryBlock block : pagesToFree) {
      memoryFreed += block.size();
      freePage(block);
      totalPageMemoryUsageBytes -= block.size();
    }
    return memoryFreed;
  }

  /**
   * 清空已分配页面列表，返回需要释放的页面列表。
   * 这样做是为了避免嵌套锁死锁：调用方持有UnsafeExternalSorter锁时，调用freePage会持有TaskMemoryManager锁，导致嵌套锁。
   *
   * @return 需要释放的页面列表
   */
  private List<MemoryBlock> clearAndGetAllocatedPagesToFree() {
    updatePeakMemoryUsed();
    List<MemoryBlock> pagesToFree = new LinkedList<>(allocatedPages);
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return pagesToFree;
  }

  /**
   * 删除所有溢写文件。
   */
  private void deleteSpillFiles() {
    for (UnsafeSorterSpillWriter spill : spillWriters) {
      File file = spill.getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}",
            MDC.of(LogKeys.PATH, file.getAbsolutePath()));
        }
      }
    }
  }

  /**
   * 释放排序器所有内存数据结构并清理溢写文件，任务结束时调用。
   */
  public void cleanupResources() {
    // 避免死锁：不在持有UnsafeExternalSorter锁的情况下调用可能获取TaskMemoryManager锁的方法
    // 先在同步块内修改状态，实际释放操作放在同步块外执行
    UnsafeInMemorySorter inMemSorterToFree = null;
    List<MemoryBlock> pagesToFree = null;
    try {
      synchronized (this) {
        deleteSpillFiles();
        pagesToFree = clearAndGetAllocatedPagesToFree();
        if (inMemSorter != null) {
          inMemSorterToFree = inMemSorter;
          inMemSorter = null;
        }
      }
    } finally {
      if (pagesToFree != null) {
        for (MemoryBlock pageToFree : pagesToFree) {
          freePage(pageToFree);
          totalPageMemoryUsageBytes -= pageToFree.size();
        }
      }
      if (inMemSorterToFree != null) {
        inMemSorterToFree.freeMemory();
      }
    }
  }

  /**
   * 检查排序指针数组是否有足够空间插入新记录，如果空间不足则扩容
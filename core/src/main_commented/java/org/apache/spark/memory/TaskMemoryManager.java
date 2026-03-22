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

package org.apache.spark.memory;

import javax.annotation.concurrent.GuardedBy;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * 文件: TaskMemoryManager.java
 * 所属模块: Spark核心计算模块(core)
 * 核心职责: 单个任务的内存管理器，负责管理该任务的执行内存分配、释放、溢出和页表编码，为Tungsten执行引擎提供内存抽象
 * 
 * 地址编码设计说明:
 * 为了将堆内地址压缩编码到64位长整型中，采用页表机制：
 * 1. 高13位存储页号，最多支持8192个页
 * 2. 低51位存储页内偏移
 * 3. 通过页表将页号映射到实际的堆内存基对象，解决GC移动对象后地址不稳定的问题
 * 4. 堆外模式直接存储原始绝对地址，不需要页表查询
 */
/**
 * Manages the memory allocated by an individual task.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * (2^31 - 1) * 8 bytes, which is
 * approximately 140 terabytes of memory.
 */
public class TaskMemoryManager {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(TaskMemoryManager.class);

  /** 用于页表寻址的比特位数 */
  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** 用于编码页内偏移的比特位数 */
  /** The number of bits used to encode offsets in data pages. */
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /** 页表的总条目数量 */
  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's
   * maximum page size is limited by the maximum amount of data that can be stored in a long[]
   * array, which is (2^31 - 1) * 8 bytes (or about 17 gigabytes). Therefore, we cap this at 17
   * gigabytes.
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /** 提取低51位偏移量的位掩码 */
  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both on- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an on-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  private final MemoryManager memoryManager;

  private final long taskAttemptId;

  /**
   * Tracks whether we're on-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   */
  final MemoryMode tungstenMemoryMode;

  /**
   * Tracks spillable memory consumers.
   */
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  /**
   * The amount of memory that is acquired but not used.
   */
  private volatile long acquiredButNotUsed = 0L;

  /**
   * Current off heap memory usage by this task.
   */
  private long currentOffHeapMemory = 0L;

  private final Object offHeapMemoryLock = new Object();

  /*
   * Current on heap memory usage by this task.
   */
  private long currentOnHeapMemory = 0L;

  private final Object onHeapMemoryLock = new Object();

  /**
   * Peak off heap memory usage by this task.
   */
  private volatile long peakOffHeapMemory = 0L;

  /**
   * Peak on heap memory usage by this task.
   */
  private volatile long peakOnHeapMemory = 0L;

  /**
   * 构造单个任务的内存管理器
   * @param memoryManager 全局内存管理器
   * @param taskAttemptId 当前任务尝试ID
   */
  /**
   * Construct a new TaskMemoryManager.
   */
  public TaskMemoryManager(MemoryManager memoryManager, long taskAttemptId) {
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.taskAttemptId = taskAttemptId;
    this.consumers = new HashSet<>();
  }

  /**
   * 为内存消费者申请指定大小的执行内存，内存不足时会触发其他消费者 spill 释放内存
   * @param required 需要申请的内存字节数
   * @param requestingConsumer 发起申请的内存消费者
   * @return 实际成功分配的内存字节数，小于等于请求大小
   */
  /**
   * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
   * spill() of consumers to release more memory.
   *
   * @return number of bytes successfully granted (<= N).
   */
  public long acquireExecutionMemory(long required, MemoryConsumer requestingConsumer) {
    assert(required >= 0);
    assert(requestingConsumer != null);
    MemoryMode mode = requestingConsumer.getMode();
    // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
    // memory here, then it may not make sense to spill since that would only end up freeing
    // off-heap memory. This is subject to change, though, so it may be risky to make this
    // optimization now in case we forget to undo it late when making changes.
    synchronized (this) {
      long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

      // Try to release memory from other consumers first, then we can reduce the frequency of
      // spilling, avoid to have too many spilled files.
      if (got < required) {
        if (logger.isDebugEnabled()) {
          logger.debug("Task {} need to spill {} for {}", taskAttemptId,
            Utils.bytesToString(required - got), requestingConsumer);
        }
        // We need to call spill() on consumers to free up more memory. We want to optimize for two
        // things:
        // * Minimize the number of spill calls, to reduce the number of spill files and avoid small
        //   spill files.
        // * Avoid spilling more data than necessary - if we only need a little more memory, we may
        //   not want to spill as much data as possible. Many consumers spill more than the
        //   requested amount, so we can take that into account in our decisions.
        // We use a heuristic that selects the smallest memory consumer with at least `required`
        // bytes of memory in an attempt to balance these factors. It may work well if there are
        // fewer larger requests, but can result in many small spills if there are many smaller
        // requests.

        // 按内存使用量对消费者排序，优先溢出使用内存最少且满足需求的消费者，平衡溢出次数和溢出量
        // Build a map of consumer in order of memory usage to prioritize spilling. Assign current
        // consumer (if present) a nominal memory usage of 0 so that it is always last in priority
        // order. The map will include all consumers that have previously acquired memory.
        TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();
        for (MemoryConsumer c: consumers) {
          if (c.getUsed() > 0 && c.getMode() == mode) {
            long key = c == requestingConsumer ? 0 : c.getUsed();
            List<MemoryConsumer> list =
                sortedConsumers.computeIfAbsent(key, k -> new ArrayList<>(1));
            list.add(c);
          }
        }
        // 迭代溢出消费者，直到获得足够内存或没有可溢出的消费者
        // Iteratively spill consumers until we've freed enough memory or run out of consumers.
        while (got < required && !sortedConsumers.isEmpty()) {
          // 获取内存使用量大于等于剩余需求的最小消费者
          // Get the consumer using the least memory more than the remaining required memory.
          Map.Entry<Long, List<MemoryConsumer>> currentEntry =
            sortedConsumers.ceilingEntry(required - got);
          // No consumer has enough memory on its own, start with spilling the biggest consumer.
          if (currentEntry == null) {
            currentEntry = sortedConsumers.lastEntry();
          }
          List<MemoryConsumer> cList = currentEntry.getValue();
          got += trySpillAndAcquire(requestingConsumer, required - got, cList, cList.size() - 1);
          if (cList.isEmpty()) {
            sortedConsumers.remove(currentEntry.getKey());
          }
        }
      }

      consumers.add(requestingConsumer);
      if (logger.isDebugEnabled()) {
        logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got),
          requestingConsumer);
      }

      // 更新当前任务对应内存模式的内存使用量和峰值
      if (mode == MemoryMode.OFF_HEAP) {
        synchronized (offHeapMemoryLock) {
          currentOffHeapMemory += got;
          peakOffHeapMemory = Math.max(peakOffHeapMemory, currentOffHeapMemory);
        }
      } else {
        synchronized (onHeapMemoryLock) {
          currentOnHeapMemory += got;
          peakOnHeapMemory = Math.max(peakOnHeapMemory, currentOnHeapMemory);
        }
      }

      return got;
    }
  }

  /**
   * 尝试溢出指定消费者释放内存，并重新申请释放出的内存
   * @param requestingConsumer 发起申请的消费者
   * @param requested 需要申请的内存大小
   * @param cList 待溢出消费者列表
   * @param idx 待溢出消费者在列表中的索引
   * @return 实际成功分配的内存字节数
   * @throws RuntimeException 任务被中断时抛出
   * @throws SparkOutOfMemoryError 溢出过程IO异常时抛出
   */
  /**
   * Try to acquire as much memory as possible from `cList[idx]`, up to `requested` bytes by
   * spilling and then acquiring the freed memory. If no more memory can be spilled from
   * `cList[idx]`, remove it from the list.
   *
   * @return number of bytes acquired (<= requested)
   * @throws RuntimeException if task is interrupted
   * @throws SparkOutOfMemoryError if an IOException occurs during spilling
   */
  private long trySpillAndAcquire(
      MemoryConsumer requestingConsumer,
      long requested,
      List<MemoryConsumer> cList,
      int idx) {
    MemoryMode mode = requestingConsumer.getMode();
    MemoryConsumer consumerToSpill = cList.get(idx);
    if (logger.isDebugEnabled()) {
      logger.debug("Task {} try to spill {} from {} for {}", taskAttemptId,
        Utils.bytesToString(requested), consumerToSpill, requestingConsumer);
    }
    try {
      long released = consumerToSpill.spill(requested, requestingConsumer);
      if (released > 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("Task {} spilled {} of requested {} from {} for {}", taskAttemptId,
            Utils.bytesToString(released), Utils.bytesToString(requested), consumerToSpill,
            requestingConsumer);
        }

        // When our spill handler releases memory, `ExecutionMemoryPool#releaseMemory()` will
        // immediately notify other tasks that memory has been freed, and they may acquire the
        // newly-freed memory before we have a chance to do so (SPARK-35486). Therefore we may
        // not be able to acquire all the memory that was just spilled. In that case, we will
        // try again in the next loop iteration.
        return memoryManager.acquireExecutionMemory(requested, taskAttemptId, mode);
      } else {
        cList.remove(idx);
        return 0;
      }
    } catch (ClosedByInterruptException | InterruptedIOException e) {
      // This called by user to kill a task (e.g: speculative task).
      logger.error("Error while calling spill() on {}", e,
        MDC.of(LogKeys.MEMORY_CONSUMER, consumerToSpill));
      throw new RuntimeException(e.getMessage());
    } catch (IOException e) {
      logger.error("Error while calling spill() on {}", e,
        MDC.of(LogKeys.MEMORY_CONSUMER, consumerToSpill));
      // checkstyle.off: RegexpSinglelineJava
      throw new SparkOutOfMemoryError(
        "SPILL_OUT_OF_MEMORY",
        new HashMap<String, String>() {{
          put("consumerToSpill", consumerToSpill.toString());
          put("message", e.getMessage());
        }});
      // checkstyle.on: RegexpSinglelineJava
    }
  }

  /**
   * 释放指定内存消费者占用的执行内存
   * @param size 要释放的内存字节数
   * @param consumer 占用内存的消费者
   */
  /**
   * Release N bytes of execution memory for a MemoryConsumer.
   */
  public void releaseExecutionMemory(long size, MemoryConsumer consumer) {
    if (logger.isDebugEnabled()) {
      logger.debug("Task {} release {} from {}", taskAttemptId, Utils.bytesToString(size),
        consumer);
    }
    memoryManager.releaseExecutionMemory(size, taskAttemptId, consumer.getMode());
    // 更新当前任务内存使用量
    if (consumer.getMode() == MemoryMode.OFF_HEAP) {
      synchronized (offHeapMemoryLock) {
        currentOffHeapMemory -= size;
      }
    } else {
      synchronized (onHeapMemoryLock) {
        currentOnHeapMemory -= size;
      }
    }
  }

  /**
   * 打印当前任务所有消费者的内存使用情况到日志，用于调试排查内存问题
   */
  /**
   * Dump the memory usage of all consumers.
   */
  public void showMemoryUsage() {
    logger.info("Memory used in task {}",
      MDC.of(LogKeys.TASK_ATTEMPT_ID,
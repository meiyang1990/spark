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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;

import javax.annotation.Nullable;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;

/**
 * 基于AlphaSort风格的键前缀排序实现，用于在内存中对Unsafe格式记录排序。
 * 将记录指针和排序键前缀存储在一起，比较时先比较前缀，仅当前缀相等时才访问完整记录，
 * 减少随机内存访问提升缓存命中率，从而提高排序性能。
 */
public final class UnsafeInMemorySorter {

  /**
   * 排序比较器，实现带键前缀优化的比较逻辑
   */
  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    private final RecordComparator recordComparator;
    private final PrefixComparator prefixComparator;
    private final TaskMemoryManager memoryManager;

    SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.memoryManager = memoryManager;
    }

    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
      // 先比较键前缀
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      if (prefixComparisonResult == 0) {
        // 前缀相等时，才比较完整记录
        final Object baseObject1 = memoryManager.getPage(r1.recordPointer);
        final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + uaoSize;
        final int baseLength1 = UnsafeAlignedOffset.getSize(baseObject1, baseOffset1 - uaoSize);
        final Object baseObject2 = memoryManager.getPage(r2.recordPointer);
        final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + uaoSize;
        final int baseLength2 = UnsafeAlignedOffset.getSize(baseObject2, baseOffset2 - uaoSize);
        return recordComparator.compare(baseObject1, baseOffset1, baseLength1, baseObject2,
          baseOffset2, baseLength2);
      } else {
        // 前缀不相等直接返回结果
        return prefixComparisonResult;
      }
    }
  }

  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;
  @Nullable
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * 如果非null，指定基数排序参数，表示将使用基数排序
   */
  @Nullable
  private final PrefixComparators.RadixSortSupport radixSortSupport;

  /**
   * 排序缓冲区：位置 2*i 存储记录指针，位置 2*i+1 存储8字节键前缀。
   * 仅部分数组用于存储指针，剩余部分预留作为排序的临时缓冲区。
   */
  private LongArray array;

  /**
   * 排序缓冲区中可插入新记录的当前位置
   */
  private int pos = 0;

  /**
   * 基数排序场景下，存储空前缀记录和非空前缀记录的边界位置。
   * 区间 [0..nullBoundaryPos) 存储空前缀记录，区间 [nullBoundaryPos..pos) 存储非空前缀记录，
   * 避免对空值进行基数排序。
   */
  private int nullBoundaryPos = 0;

  /*
   * 可插入记录的最大容量：数组一部分空间需要预留用于排序，因此实际可用容量小于数组长度。
   */
  private int usableCapacity = 0;

  private long initialSize;

  private long totalSortTimeNanos = 0L;

  /**
   * 构造一个新的Unsafe内存排序器
   * @param consumer 内存消费者，用于分配/释放内存
   * @param memoryManager 任务内存管理器，用于解析记录指针
   * @param recordComparator 完整记录比较器
   * @param prefixComparator 键前缀比较器
   * @param initialSize 初始记录容量
   * @param canUseRadixSort 是否可以使用基数排序优化
   */
  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator,
    int initialSize,
    boolean canUseRadixSort) {
    this(consumer, memoryManager, recordComparator, prefixComparator,
      consumer.allocateArray(initialSize * 2L), canUseRadixSort);
  }

  /**
   * 构造一个新的Unsafe内存排序器，使用预先分配好的数组
   * @param consumer 内存消费者，用于分配/释放内存
   * @param memoryManager 任务内存管理器，用于解析记录指针
   * @param recordComparator 完整记录比较器
   * @param prefixComparator 键前缀比较器
   * @param array 预先分配好的存储指针和前缀的数组
   * @param canUseRadixSort 是否可以使用基数排序优化
   */
  public UnsafeInMemorySorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array,
      boolean canUseRadixSort) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.initialSize = array.size();
    if (recordComparator != null) {
      this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
      // 检查是否满足基数排序条件
      if (canUseRadixSort &&
        prefixComparator instanceof PrefixComparators.RadixSortSupport radixSortSupport) {
        this.radixSortSupport = radixSortSupport;
      } else {
        this.radixSortSupport = null;
      }
    } else {
      this.sortComparator = null;
      this.radixSortSupport = null;
    }
    this.array = array;
    this.usableCapacity = getUsableCapacity();
  }

  /**
   * 计算可用于存储记录的实际可用容量
   * 基数排序需要额外一倍空间作为缓冲区，TimSort需要额外一半空间作为缓冲区
   */
  private int getUsableCapacity() {
    return (int) (array.size() / (radixSortSupport != null ? 2 : 1.5));
  }

  public long getInitialSize() {
    return initialSize;
  }

  /**
   * 释放指针数组占用的内存
   */
  public void freeMemory() {
    if (consumer != null) {
      if (array != null) {
        consumer.freeArray(array);
      }

      // 将数组置为null，不在此重新分配。重新分配可能触发新的溢写，而此方法已经在溢写流程中被调用，
      // 分配时可能持有未完成的大内存分配，会导致其他内存分配失败。需要时再重新分配即可。
      array = null;
      usableCapacity = 0;
    }
    pos = 0;
    nullBoundaryPos = 0;
  }

  /**
   * @return 已插入到排序器中的记录数量
   */
  public int numRecords() {
    return pos / 2;
  }

  /**
   * @return 内存排序累计耗时（纳秒）
   */
  public long getSortTimeNanos() {
    return totalSortTimeNanos;
  }

  /**
   * @return 当前排序器占用内存大小（字节）
   */
  public long getMemoryUsage() {
    if (array == null) {
      return 0L;
    }

    return array.size() * 8;
  }

  /**
   * @return 是否还有空间插入一条新记录
   */
  public boolean hasSpaceForAnotherRecord() {
    return pos + 1 < usableCapacity;
  }

  /**
   * 扩展存储指针和前缀的数组，将原有数据复制到新数组
   * @param newArray 扩展后的新数组
   */
  public void expandPointerArray(LongArray newArray) {
    if (array != null) {
      if (newArray.size() < array.size()) {
        // checkstyle.off: RegexpSinglelineJava
        throw new SparkOutOfMemoryError("POINTER_ARRAY_OUT_OF_MEMORY", new HashMap<>());
        // checkstyle.on: RegexpSinglelineJava
      }
      // 复制原有数据到新数组
      Platform.copyMemory(
        array.getBaseObject(),
        array.getBaseOffset(),
        newArray.getBaseObject(),
        newArray.getBaseOffset(),
        pos * 8L);
      // 释放原数组内存
      consumer.freeArray(array);
    }
    array = newArray;
    usableCapacity = getUsableCapacity();
  }

  /**
   * 插入一条待排序记录。要求记录指针指向记录长度（存储为uaoSize字节整数），长度之后是记录字节。
   * @param recordPointer TaskMemoryManager编码的记录指针，指向数据页中的记录
   * @param keyPrefix 用户定义的排序键前缀
   * @param prefixIsNull 前缀是否为空
   */
  public void insertRecord(long recordPointer, long keyPrefix, boolean prefixIsNull) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    // 基数排序场景下，空前缀记录放在数组前部，非空前缀放在后部，分离存储避免对空排序
    if (prefixIsNull && radixSortSupport != null) {
      // 将非空记录交换前移，在null边界位置腾出空间给当前空前缀记录
      array.set(pos, array.get(nullBoundaryPos));
      pos++;
      array.set(pos, array.get(nullBoundaryPos + 1));
      pos++;
      // 将当前空前缀记录放入null边界位置
      array.set(nullBoundaryPos, recordPointer);
      nullBoundaryPos++;
      array.set(nullBoundaryPos, keyPrefix);
      nullBoundaryPos++;
    } else {
      // 非空前缀直接插入到当前位置
      array.set(pos, recordPointer);
      pos++;
      array.set(pos, keyPrefix);
      pos++;
    }
  }

  /**
   * 排序结果迭代器，遍历已排序的记录
   */
  public final class SortedIterator extends UnsafeSorterIterator implements Cloneable {

    private final int numRecords;
    private int position;
    private int offset;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;
    private long currentPageNumber;
    private final TaskContext taskContext = TaskContext.get();

    private SortedIterator(int numRecords, int offset) {
      this.numRecords = numRecords;
      this.position = 0;
      this.offset = offset;
    }

    @Override
    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords, offset);
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      iter.currentPageNumber = currentPageNumber;
      return iter;
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      return position / 2 < numRecords;
    }

    @Override
    public void loadNext() {
      // 任务被中断时及时终止，内联此检查避免包装迭代器带来性能开销
      // 放在loadNext而不是hasNext中，因为调用者可能使用getNumRecords控制遍历终止
      if (taskContext != null) {
        taskContext.killTaskIfInterrupted();
      }
      // 解析记录指针，获取记录位置和长度
      final long recordPointer = array.get(offset + position);
      currentPageNumber = TaskMemoryManager.decodePageNumber(recordPointer);
      int uaoSize = UnsafeAlignedOffset.getUaoSize();
      baseObject = memoryManager.getPage(recordPointer);
      // 跳过长度字段，偏移到实际记录数据
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + uaoSize;
      recordLength = UnsafeAlignedOffset.getSize(baseObject, baseOffset - uaoSize);
      keyPrefix = array.get(offset + position + 1);
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    @Override
    public long getCurrentPageNumber() {
      return currentPageNumber;
    }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * 获取排序后的记录迭代器。为了性能，所有next()调用返回同一个可变对象。
   * @return 排序后的记录迭代器
   */
  public UnsafeSorterIterator getSortedIterator() {
    if (numRecords() == 0) {
      // 数组可能为null，提前返回避免空指针访问
      return new SortedIterator(0, 0);
    }

    int offset = 0;
    long start = System.nanoTime();
    if (sortComparator != null) {
      if (this.radixSortSupport != null) {
        // 使用基数排序，仅对非空前缀部分排序
        offset = RadixSort.sortKeyPrefixArray(
          array, nullBoundaryPos, (pos - nullBoundaryPos) / 2L, 0, 7,
          radixSortSupport.sortDescending(), radixSortSupport.sortSigned());
      } else {
        // 使用TimSort，将数组未使用部分作为排序临时缓冲区
        MemoryBlock unused = new MemoryBlock(
          array.getBaseObject(),
          array.getBaseOffset() + pos * 8L,
          (array.size() - pos) * 8L);
        LongArray buffer = new LongArray(unused);
        Sorter<RecordPointerAndKeyPrefix, LongArray> sorter =
          new Sorter<>(new UnsafeSortDataFormat(buffer));
        sorter.sort(array, 0, pos / 2, sortComparator);
      }
    }
    // 累加排序耗时
    totalSortTimeNanos += System.nanoTime() - start;
    if (nullBoundaryPos > 0) {
      // 存在空前缀记录，需要合并两个迭代器
      assert radixSortSupport != null : "Nulls are only stored separately with radix sort";
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();

      // 无论排序方向是升序还是降序，空值的顺序固定放在开头或结尾
      if (radixSortSupport.nullsFirst()) {
        // 空值在前，先迭代空值部分再迭代非空部分
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
      } else {
        // 空值在后，先迭代非空部分再迭代空值部分
        queue.add(new SortedIterator((pos - nullBoundaryPos) / 2, offset));
        queue.add(new SortedIterator(nullBoundaryPos / 2, 0));
      }
      return new UnsafeExternalSorter.ChainedIterator(queue);
    } else {
      // 没有空前缀，直接返回非空部分迭代器
      return new SortedIterator(pos / 2, offset);
    }
  }
}
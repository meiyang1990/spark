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

import java.util.Comparator;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.Sorter;
import org.apache.spark.util.collection.unsafe.sort.RadixSort;

/**
 * Shuffle排序过程中的内存内排序器，负责在Map端对不同分区的记录指针按分区ID排序，
 * 是SortShuffleManager核心组件，用于减少磁盘IO和提升shuffle排序效率。
 * 仅对记录指针进行排序而非直接操作记录数据，排序后可高效按分区写出数据。
 */
final class ShuffleInMemorySorter {

  /**
   * 排序比较器，按分区ID对打包后的记录指针进行比较排序
   */
  private static final class SortComparator implements Comparator<PackedRecordPointer> {
    @Override
    public int compare(PackedRecordPointer left, PackedRecordPointer right) {
      return Integer.compare(left.getPartitionId(), right.getPartitionId());
    }
  }
  private static final SortComparator SORT_COMPARATOR = new SortComparator();

  private final MemoryConsumer consumer;

  /**
   * 存储打包后的记录指针数组，每个元素编码了记录地址和分区ID，排序直接操作该数组
   * 数组预留一部分空间作为排序临时缓冲区，仅前半部分（基数排序）或三分之二（Tim排序）用于存储记录
   */
  private LongArray array;

  /**
   * 是否使用基数排序，基数排序速度更快但需要额外预留一半空间作为缓冲区
   */
  private final boolean useRadixSort;

  /**
   * 下一个新记录插入的数组位置指针
   */
  private int pos = 0;

  /**
   * 数组可存储记录的可用容量，需要预留排序缓冲区因此小于数组实际容量
   */
  private int usableCapacity = 0;

  private final int initialSize;

  /**
   * 构造内存内排序器，申请初始内存空间存储记录指针
   * @param consumer 内存消费者，用于从Spark内存管理器申请/释放内存
   * @param initialSize 初始数组大小（单位：long元素）
   * @param useRadixSort 是否启用基数排序优化
   */
  ShuffleInMemorySorter(MemoryConsumer consumer, int initialSize, boolean useRadixSort) {
    this.consumer = consumer;
    assert (initialSize > 0);
    this.initialSize = initialSize;
    this.useRadixSort = useRadixSort;
    this.array = consumer.allocateArray(initialSize);
    this.usableCapacity = getUsableCapacity();
  }

  /**
   * 根据排序算法计算可用存储容量，不同排序算法需要预留不同大小的临时缓冲区
   * @return 可用于存储记录指针的容量
   */
  private int getUsableCapacity() {
    // 基数排序需要预留一半空间作为缓冲区，Tim排序需要预留三分之一空间作为缓冲区
    return (int) (array.size() / (useRadixSort ? 2 : 1.5));
  }

  /**
   * 释放排序器占用的内存资源
   */
  public void free() {
    if (array != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  /**
   * 获取当前已插入的记录数量
   * @return 已插入记录数
   */
  public int numRecords() {
    return pos;
  }

  /**
   * 重置排序器状态，重新申请初始内存，用于排序器复用场景
   */
  public void reset() {
    // 先重置pos，确保后续allocateArray触发spill时不会处理无效数据
    pos = 0;
    if (consumer != null) {
      consumer.freeArray(array);
      // 释放后先置空数组，避免allocateArray抛出OOM时被误访问
      array = null;
      usableCapacity = 0;
      array = consumer.allocateArray(initialSize);
      usableCapacity = getUsableCapacity();
    }
  }

  /**
   * 扩展记录指针数组，将原有数据拷贝到新数组并替换旧数组
   * @param newArray 扩容后的新数组
   */
  public void expandPointerArray(LongArray newArray) {
    assert(newArray.size() > array.size());
    // 拷贝已插入的记录指针到新数组
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      pos * 8L
    );
    // 释放旧数组内存
    consumer.freeArray(array);
    array = newArray;
    usableCapacity = getUsableCapacity();
  }

  /**
   * 检查是否还有剩余空间插入新记录
   * @return true表示有空间插入，false表示空间不足
   */
  public boolean hasSpaceForAnotherRecord() {
    return pos < usableCapacity;
  }

  /**
   * 获取排序器当前占用的内存大小（单位：字节）
   * @return 占用内存字节数
   */
  public long getMemoryUsage() {
    return array.size() * 8;
  }

  /**
   * 插入一条待排序的记录，将记录指针和分区ID打包存入数组
   *
   * @param recordPointer 记录在内存页中的指针，由任务内存管理器编码
   * @param partitionId 记录所属的分区ID
   */
  public void insertRecord(long recordPointer, int partitionId) {
    if (!hasSpaceForAnotherRecord()) {
      throw new IllegalStateException("There is no space for new record");
    }
    array.set(pos, PackedRecordPointer.packPointer(recordPointer, partitionId));
    pos++;
  }

  /**
   * 排序后的记录迭代器，为了优化内联性能不使用Java原生Iterator
   */
  public static final class ShuffleSorterIterator {

    private final LongArray pointerArray;
    private final int limit;
    final PackedRecordPointer packedRecordPointer = new PackedRecordPointer();
    private int position = 0;

    /**
     * 构造排序后迭代器
     * @param numRecords 总记录数
     * @param pointerArray 排序后的指针数组
     * @param startingPosition 迭代起始位置
     */
    ShuffleSorterIterator(int numRecords, LongArray pointerArray, int startingPosition) {
      this.limit = numRecords + startingPosition;
      this.pointerArray = pointerArray;
      this.position = startingPosition;
    }

    /**
     * 检查是否还有未遍历的记录
     * @return true表示还有下一条记录
     */
    public boolean hasNext() {
      return position < limit;
    }

    /**
     * 加载下一条记录到packedRecordPointer中
     */
    public void loadNext() {
      packedRecordPointer.set(pointerArray.get(position));
      position++;
    }
  }

  /**
   * 执行排序，返回排序后的记录迭代器
   * @return 按分区ID排序后的记录迭代器
   */
  public ShuffleSorterIterator getSortedIterator() {
    int offset = 0;
    if (useRadixSort) {
      // 使用基数排序按分区ID排序
      offset = RadixSort.sort(
        array, pos,
        PackedRecordPointer.PARTITION_ID_START_BYTE_INDEX,
        PackedRecordPointer.PARTITION_ID_END_BYTE_INDEX, false, false);
    } else {
      // 使用Tim排序，利用数组剩余未使用空间作为排序缓冲区
      MemoryBlock unused = new MemoryBlock(
        array.getBaseObject(),
        array.getBaseOffset() + pos * 8L,
        (array.size() - pos) * 8L);
      LongArray buffer = new LongArray(unused);
      Sorter<PackedRecordPointer, LongArray> sorter =
        new Sorter<>(new ShuffleSortDataFormat(buffer));

      sorter.sort(array, 0, pos, SORT_COMPARATOR);
    }
    return new ShuffleSorterIterator(pos, array, offset);
  }
}
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

package org.apache.spark.unsafe.map;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;

import org.apache.spark.SparkEnv;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.internal.MDC;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillReader;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillWriter;

/**
 * 仅追加的字节键值对哈希表，用于Spark底层Shuffle等场景存储聚合数据
 * <p>
 * 采用2的幂次方大小的哈希表结构，使用三角数二次探测解决哈希冲突，可保证探测穷尽空间
 * 最多支持2^29个key，键值对数量超过该值时更推荐使用排序获得更好的缓存局部性
 * <p>
 * 键值存储格式（与UnsafeExternalSorter兼容，可直接传入排序）：
 * <ul>
 *   <li>前 uaoSize 字节：整个记录长度（键长 + 值长 + uaoSize）</li>
 *   <li>接下来 uaoSize 字节：键的长度（字节）</li>
 *   <li>接下来 len(k) 字节：键数据</li>
 *   <li>接下来 len(v) 字节：值数据</li>
 *   <li>最后8字节：指向下一个同key值对的指针</li>
 * </ul>
 */
public final class BytesToBytesMap extends MemoryConsumer {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(BytesToBytesMap.class);

  private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

  private final TaskMemoryManager taskMemoryManager;

  /**
   * 链表跟踪所有已分配的数据页，用于在释放时清理所有内存
   */
  private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();

  /**
   * 用于存储新哈希表条目的当前数据页，页满时会分配新页并更新指针
   */
  private MemoryBlock currentPage = null;

  /**
   * 当前页中可插入新数据的偏移量，不包含页的基偏移
   */
  private long pageCursor = 0;

  /**
   * 哈希表支持的最大key数量。由于哈希表大小必须为2的幂次方，Java数组最多容纳(1 << 30)个元素，
   * 每个key占用两个long数组项，因此最大容量为(1 << 29)
   */
  public static final int MAX_CAPACITY = (1 << 29);

  // This choice of page table size and page size means that we can address up to 500 gigabytes
  // of memory.

  /**
   * 存储key指针和哈希码的长数组，格式：
   * 位置 2*i 存储指向key的指针，位置 2*i + 1存储key的完整32位哈希码
   */
  @Nullable private LongArray longArray;
  // TODO: we're wasting 32 bits of space here; we can probably store fewer bits of the hashcode
  // and exploit word-alignment to use fewer bits to hold the address.  This might let us store
  // only one long per map entry, increasing the chance that this array will fit in cache at the
  // expense of maybe performing more lookups if we have hash collisions.  Say that we stored only
  // 27 bits of the hashcode and 37 bits of the address.  37 bits is enough to address 1 terabyte
  // of RAM given word-alignment.  If we use 13 bits of this for our page table, that gives us a
  // maximum page size of 2^24 * 8 = ~134 megabytes per page. This change will require us to store
  // full base addresses in the page table for off-heap mode so that we can reconstruct the full
  // absolute memory addresses.

  /**
   * 标记哈希表数组是否还可以扩容，为false时不再插入新元素
   */
  private boolean canGrowArray = true;

  private final double loadFactor;

  /**
   * 存储键值数据的数据页大小，单个键值对不能跨页，因此该值限制了单个条目的最大大小
   */
  private final long pageSizeBytes;

  /**
   * 哈希表中已定义的key数量
   */
  private int numKeys;

  /**
   * 哈希表中已定义的值数量，一个key可以对应多个值
   */
  private int numValues;

  /**
   * 扩容阈值，当key数量超过该值时触发哈希表扩容
   */
  private int growthThreshold;

  /**
   * 哈希码掩码，用于将哈希码截断到数组大小范围内，利用位运算替代取模，是幂次方大小哈希表的优化
   */
  private int mask;

  /**
   * lookup方法的返回值对象，复用同一实例避免对象分配
   */
  private final Location loc;

  private long numProbes = 0L;

  private long numKeyLookups = 0L;

  private long peakMemoryUsedBytes = 0L;

  private final int initialCapacity;

  private final BlockManager blockManager;
  private final SerializerManager serializerManager;
  private volatile MapIterator destructiveIterator = null;
  private LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();

  /**
   * 构造BytesToBytesMap实例
   * @param taskMemoryManager 任务内存管理器，用于分配释放内存
   * @param blockManager 块管理器，用于溢写磁盘时存储溢出文件
   * @param serializerManager 序列化管理器，用于溢写时序列化数据
   * @param initialCapacity 初始容量
   * @param loadFactor 负载因子，达到该负载触发扩容
   * @param pageSizeBytes 数据页大小
   */
  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      int initialCapacity,
      double loadFactor,
      long pageSizeBytes) {
    super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.loadFactor = loadFactor;
    this.loc = new Location();
    this.pageSizeBytes = pageSizeBytes;
    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("Initial capacity must be greater than 0");
    }
    if (initialCapacity > MAX_CAPACITY) {
      throw new IllegalArgumentException(
        "Initial capacity " + initialCapacity + " exceeds maximum capacity of " + MAX_CAPACITY);
    }
    if (pageSizeBytes > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) {
      throw new IllegalArgumentException("Page size " + pageSizeBytes + " cannot exceed " +
        TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES);
    }
    this.initialCapacity = initialCapacity;
    allocate(initialCapacity);
  }

  /**
   * 简化构造方法，使用默认负载因子和SparkEnv获取块管理器和序列化管理器
   * @param taskMemoryManager 任务内存管理器
   * @param initialCapacity 初始容量
   * @param pageSizeBytes 数据页大小
   */
  public BytesToBytesMap(
      TaskMemoryManager taskMemoryManager,
      int initialCapacity,
      long pageSizeBytes) {
    this(
      taskMemoryManager,
      SparkEnv.get() != null ? SparkEnv.get().blockManager() :  null,
      SparkEnv.get() != null ? SparkEnv.get().serializerManager() :  null,
      initialCapacity,
      // In order to re-use the longArray for sorting, the load factor cannot be larger than 0.5.
      0.5,
      pageSizeBytes);
  }

  /**
   * 获取哈希表中的key数量
   * @return key数量
   */
  public int numKeys() { return numKeys; }

  /**
   * 获取哈希表中的值数量，一个key可对应多个值
   * @return 值数量
   */
  public int numValues() { return numValues; }

  /**
   * 遍历哈希表所有条目迭代器，遍历过程不释放内存，可重复遍历
   * <p>
   * 为提高效率，所有next()调用返回同一个Location对象
   * <p>
   * 迭代器线程安全，但遍历过程中修改哈希表会导致行为未定义
   */
  public final class MapIterator implements Iterator<Location> {

    private int numRecords;
    private final Location loc;

    private MemoryBlock currentPage = null;
    private int recordsInPage = 0;
    private Object pageBaseObject;
    private long offsetInPage;

    // If this iterator destructive or not. When it is true, it frees each page as it moves onto
    // next one.
    private boolean destructive = false;
    private UnsafeSorterSpillReader reader = null;

    private MapIterator(int numRecords, Location loc, boolean destructive) {
      this.numRecords = numRecords;
      this.loc = loc;
      this.destructive = destructive;
      if (destructive) {
        destructiveIterator = this;
        // longArray will not be used anymore if destructive is true, release it now.
        if (longArray != null) {
          freeArray(longArray);
          longArray = null;
        }
      }
    }

    /**
     * 前进到下一个数据页，处理内存页切换和溢读文件读取
     */
    private void advanceToNextPage() {
      // SPARK-26265: We will first lock this `MapIterator` and then `TaskMemoryManager` when going
      // to free a memory page by calling `freePage`. At the same time, it is possibly that another
      // memory consumer first locks `TaskMemoryManager` and then this `MapIterator` when it
      // acquires memory and causes spilling on this `MapIterator`. To avoid deadlock here, we keep
      // reference to the page to free and free it after releasing the lock of `MapIterator`.
      MemoryBlock pageToFree = null;

      try {
        synchronized (this) {
          int nextIdx = dataPages.indexOf(currentPage) + 1;
          if (destructive && currentPage != null) {
            dataPages.remove(currentPage);
            pageToFree = currentPage;
            nextIdx--;
          }
          if (dataPages.size() > nextIdx) {
            // 还有未处理的内存页，加载下一个内存页
            currentPage = dataPages.get(nextIdx);
            pageBaseObject = currentPage.getBaseObject();
            offsetInPage = currentPage.getBaseOffset();
            recordsInPage = UnsafeAlignedOffset.getSize(pageBaseObject, offsetInPage);
            offsetInPage += UnsafeAlignedOffset.getUaoSize();
          } else {
            // 内存页处理完，切换到溢写文件读取
            currentPage = null;
            if (reader != null) {
              handleFailedDelete();
            }
            try {
              Closeables.close(reader, /* swallowIOException = */ false);
              reader = spillWriters.getFirst().getReader(serializerManager);
              recordsInPage = -1;
            } catch (IOException e) {
              // Scala iterator does not handle exception
              Platform.throwException(e);
            }
          }
        }
      } finally {
        if (pageToFree != null) {
          freePage(pageToFree);
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (numRecords == 0) {
        if (reader != null) {
          handleFailedDelete();
        }
      }
      return numRecords > 0;
    }

    @Override
    public Location next() {
      if (recordsInPage == 0) {
        advanceToNextPage();
      }
      numRecords--;
      if (currentPage != null) {
        // 从当前内存页读取下一条记录
        int totalLength = UnsafeAlignedOffset.getSize(pageBaseObject, offsetInPage);
        loc.with(currentPage, offsetInPage);
        // [total size] [key size] [key] [value] [pointer to next]
        offsetInPage += UnsafeAlignedOffset.getUaoSize() + totalLength + 8;
        recordsInPage --;
        return loc;
      } else {
        // 从溢写文件读取下一条记录
        assert(reader != null);
        if (!reader.hasNext()) {
          advanceToNextPage();
        }
        try {
          reader.loadNext();
        } catch (IOException e) {
          try {
            reader.close();
          } catch(IOException e2) {
            logger.error("Error while closing spill reader", e2);
          }
          // Scala iterator does not handle exception
          Platform.throwException(e);
        }
        loc.with(reader.getBaseObject(), reader.getBaseOffset(), reader.getRecordLength());
        return loc;
      }
    }

    /**
     * 将部分已处理的数据页溢写到磁盘，释放内存
     * @param numBytes 需要释放的内存字节数
     * @return 实际释放的字节数
     * @throws IOException 溢写磁盘时IO异常
     */
    public synchronized long spill(long numBytes) throws IOException {
      if (!destructive || dataPages.size() == 1) {
        return 0L;
      }

      updatePeakMemoryUsed();

      // TODO: use existing ShuffleWriteMetrics
      ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();

      long released = 0L;
      while (dataPages.size() > 0) {
        MemoryBlock block = dataPages.getLast();
        // The currentPage is used, cannot be released
        if (block == currentPage) {
          break;
        }

        // 遍历数据页中所有记录，写入溢写文件
        Object base = block.getBaseObject();
        long offset = block.getBaseOffset();
        int numRecords = UnsafeAlignedOffset.getSize(base, offset);
        int uaoSize = UnsafeAlignedOffset.getUaoSize();
        offset += uaoSize;
        final UnsafeSorterSpillWriter writer =
                new UnsafeSorterSpillWriter(blockManager, 32 * 1024, writeMetrics, numRecords);
        while (numRecords > 0) {
          int length = UnsafeAlignedOffset.getSize(base, offset);
          writer.write(base, offset + uaoSize, length, 0);
          offset += uaoSize + length + 8;
          numRecords--;
        }
        writer.close();
        spillWriters.add(writer);

        // 释放已溢写的数据页内存
        dataPages.removeLast();
        released += block.size();
        freePage(block);

        if (released >= numBytes) {
          break;
        }
      }

      return released;
    }

    /**
     * 处理已读取完的溢写文件删除
     */
    private void handleFailedDelete() {
      if (spillWriters.size() > 0) {
        // remove the spill file from disk
        File file = spillWriters.removeFirst().getFile();
        if (file != null && file.exists() && !file.delete()) {
          logger.error("Was unable to delete spill file {}",
            MDC.of(LogKeys.PATH, file.getAbsolutePath()));
        }
      }
    }
  }

  /**
   * 返回非破坏性遍历哈希表条目的迭代器
   * @return 迭代器实例
   */
  public MapIterator iterator() {
    return new MapIterator(numValues, new Location(), false);
  }

  /**
   * 返回破坏性遍历哈希表条目的迭代器，遍历过程会逐页释放
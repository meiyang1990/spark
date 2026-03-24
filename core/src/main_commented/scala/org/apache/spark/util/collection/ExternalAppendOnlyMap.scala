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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.serializer.{DeserializationStream, Serializer, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator

/**
 * 文件级别注释：
 * 外部追加仅写Map实现，当内存不足时自动将排序内容溢写磁盘，用于Shuffle阶段聚合数据，支持大规模数据的分组聚合操作。
 * 处理流程分为两步：
 * 1. 将值合并到局部聚合器，内存不足时排序后溢写到磁盘
 * 2. 从磁盘读取所有溢写分区，多路归并得到最终聚合结果
 */

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 */
/**
 * 外部追加仅写Map，支持内存不足时自动溢写磁盘，用于Shuffle聚合场景。
 * 核心职责：支持增量插入键值对，自动管理内存，内存不足时溢写排序后的分区到磁盘，最终通过归并得到全量聚合结果。
 * @param createCombiner 为新键创建初始聚合值的函数
 * @param mergeValue 将新值合并到已有聚合值的函数
 * @param mergeCombiners 合并两个来自不同分区的聚合值的函数
 * @param serializer 序列化器，用于溢写磁盘时序列化数据
 * @param blockManager 块管理器，用于管理磁盘块存储
 * @param context 当前任务上下文
 * @param serializerManager 序列化管理器
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    context: TaskContext = TaskContext.get(),
    serializerManager: SerializerManager = SparkEnv.get.serializerManager)
  extends Spillable[SizeTracker](context.taskMemoryManager())
  with Serializable
  with Logging
  with Iterable[(K, C)] {

  if (context == null) {
    throw new IllegalStateException(
      "Spillable collections should not be instantiated outside of tasks")
  }

  // Backwards-compatibility constructor for binary compatibility
  def this(
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      serializer: Serializer,
      blockManager: BlockManager) = {
    this(createCombiner, mergeValue, mergeCombiners, serializer, blockManager, TaskContext.get())
  }

  /**
   * Exposed for testing
   */
  @volatile private[collection] var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  // 存储所有已溢写到磁盘的分区迭代器
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   */
  // 序列化批次大小，每次批量处理对象，减少反序列化时引用跟踪的内存开销
  private val serializerBatchSize = sparkConf.get(config.SHUFFLE_SPILL_BATCH_SIZE)

  // 累计溢写到磁盘的总字节数
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  // 磁盘文件缓冲区大小（字节）
  private val fileBufferSize = sparkConf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // Shuffle写性能指标统计对象
  private val writeMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  // 内存Map使用的峰值内存大小（字节）
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  // 基于哈希码排序的键比较器
  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  @volatile private var readingIterator: SpillableIterator = null

  /**
   * Number of files this map has spilled so far.
   * Exposed for testing.
   */
  private[collection] def numSpills: Int = spilledMaps.size

  /**
   * 插入单个键值对到Map
   */
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  /**
   * 批量插入迭代器中的所有键值对，插入过程中会自动检查内存，不足时触发溢写。
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }
    // 可复用的更新函数，避免为每个元素分配新闭包
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    while (entries.hasNext) {
      curEntry = entries.next()
      val estimatedSize = currentMap.estimateSize()
      // 更新峰值内存使用量
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      // 检查是否需要溢写，若溢写完成则创建新的内存Map
      if (maybeSpill(currentMap, estimatedSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      // 更新指定键的聚合值
      currentMap.changeValue(curEntry._1, update)
      // 增加已读取元素计数，用于内存跟踪
      addElementsRead()
    }
  }

  /**
   * 批量插入可迭代集合中的所有键值对
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * 将当前内存Map排序后溢写到磁盘，实现Spillable接口的溢写方法
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator)
    val diskMapIterator = spillMemoryIteratorToDisk(inMemoryIterator)
    spilledMaps += diskMapIterator
  }

  /**
   * 强制溢写当前内存集合到磁盘，由TaskMemoryManager在内存不足时调用，用于主动释放内存
   * @return 是否成功触发溢写并释放内存
   */
  override protected[this] def forceSpill(): Boolean = {
    if (readingIterator != null) {
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        currentMap = null
      }
      isSpilled
    } else if (currentMap.size > 0) {
      spill(currentMap)
      currentMap = new SizeTrackingAppendOnlyMap[K, C]
      true
    } else {
      false
    }
  }

  /**
   * 将内存中已排序的迭代器内容溢写到磁盘临时文件
   * @param inMemoryIterator 内存中已排序的键值对迭代器
   * @return 磁盘文件的读取迭代器
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: Iterator[(K, C)])
      : DiskMapIterator = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    val writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, writeMetrics)
    var objectsWritten = 0

    // 记录每个批次的大小（字节），用于后续反序列化
    val batchSizes = new ArrayBuffer[Long]

    // 刷新当前批次到磁盘，更新统计信息
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val kv = inMemoryIterator.next()
        writer.write(kv._1, kv._2)
        objectsWritten += 1

        // 达到批次大小上限，刷新到磁盘
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      // 写入剩余未满批次的数据
      if (objectsWritten > 0) {
        flush()
        writer.close()
      } else {
        // 没有数据，回滚部分写入并关闭流
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      // 写入失败时清理临时文件
      if (!success) {
        writer.closeAndDelete()
      }
    }

    new DiskMapIterator(file, blockId, batchSizes)
  }

  /**
   * 返回可被强制溢写的破坏性迭代器，当内存不足时可将内存数据溢写到磁盘
   * @param inMemoryIterator 内存迭代器
   * @return 包装后的可溢写迭代器
   */
  def destructiveIterator(inMemoryIterator: Iterator[(K, C)]): Iterator[(K, C)] = {
    readingIterator = new SpillableIterator(inMemoryIterator)
    readingIterator.toCompletionIterator
  }

  /**
   * 返回合并所有内存和磁盘分区的破坏性迭代器，仅可调用一次
   * @return 全量聚合结果的迭代器
   */
  override def iterator: Iterator[(K, C)] = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
    }
    if (spilledMaps.isEmpty) {
      destructiveIterator(currentMap.iterator)
    } else {
      new ExternalIterator()
    }
  }

  private def freeCurrentMap(): Unit = {
    if (currentMap != null) {
      currentMap = null // 释放引用帮助GC回收内存
      releaseMemory()
    }
  }

  /**
   * 多路归并迭代器，合并来自内存和所有溢写磁盘分区的已排序数据，得到最终聚合结果
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // 优先队列，维护每个流当前最小哈希码的缓冲区，用于归并排序，队列中仅包含非空缓冲区
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // 输入流列表，包含当前内存Map和所有溢写磁盘分区，均已按键哈希排序
    private val sortedMap = destructiveIterator(
      currentMap.destructiveSortedIterator(keyComparator))
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    // 初始化每个流，读取第一个相同哈希码的所有键到缓冲区，加入优先队列
    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * 从迭代器中读取下一批相同哈希码的键值对，放入缓冲区，保证按哈希码顺序处理
     * @param it 已排序输入迭代器
     * @param buf 结果缓冲区
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)
        // 读取所有相同哈希码的键，处理哈希碰撞
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * 如果缓冲区中存在指定键，则将其聚合值合并到baseCombiner，并从缓冲区移除该键
     * @param key 待查找的键
     * @param baseCombiner 当前已聚合的值
     * @param buffer 待查找的缓冲区
     * @return 合并后的聚合值
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          // 溢写前每个键已经聚合完成，每个缓冲区最多一个匹配键，找到后直接返回
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    /**
     * 从ArrayBuffer中常量时间移除指定索引的元素，通过交换最后一个元素实现，不需要移动数组元素，比默认remove更高效
     * @param buffer 目标数组缓冲区
     * @param index 待移除元素索引
     * @return 被移除的元素
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)  // 索引为最后一个元素时也能正常工作
      buffer.dropRightInPlace(1)
      elem
    }

    /**
     * 检查是否还有未处理的元素
     * @return true如果还有元素，false反之
     */
    override def hasNext: Boolean = mergeHeap.nonEmpty

    /**
     * 获取下一个聚合后的键值对，基于最小堆选择最小哈希码，合并所有流中的相同键
     * @return 聚合后的键值对
     */
    override def next(): (K, C) = {
      if (mergeHeap.isEmpty) {
        throw new NoSuchElementException
      }
      // 从堆中取出当前哈希码最小的缓冲区
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      // 取出缓冲区中最小哈希对应的键值对
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2
      assert(hashKey(minPair) == minHash)

      // 处理所有其他缓冲区中相同哈希码的键，合并相同键的聚合值
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.nonEmpty && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // 重新填充每个处理过的缓冲区，若还有剩余元素则放回优先队列
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer
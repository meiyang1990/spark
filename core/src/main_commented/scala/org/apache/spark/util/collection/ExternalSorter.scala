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

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{NUM_BYTES, TASK_ATTEMPT_ID}
import org.apache.spark.serializer._
import org.apache.spark.shuffle.{ShufflePartitionPairsWriter, ShuffleWriteMetricsReporter}
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.shuffle.checksum.{RowBasedChecksum, ShuffleChecksumSupport}
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, Utils => TryUtils}

/**
 * 外部排序器，用于对大量键值对进行排序、聚合和磁盘溢出，支持基于排序的Shuffle操作
 * 
 * 首先使用分区器将key分组到不同分区，然后可选地使用自定义比较器对分区内的key排序。
 * 可以输出按分区排列的排序文件，每个分区对应独立字节范围，适合Shuffle拉取使用。
 * 如果禁用聚合，则C类型必须等于V类型，最终会进行类型转换。
 * 
 * 注意：虽然ExternalSorter是通用排序器，但部分配置绑定到基于排序的Shuffle场景，
 * 例如块压缩由`spark.shuffle.compress`控制。如果在非Shuffle场景使用可能需要调整配置。
 * 
 * @param aggregator 可选聚合器，包含合并数据的函数
 * @param partitioner 可选分区器，如果提供，则先按分区ID排序，再按key排序
 * @param ordering 可选排序器，用于对分区内key排序，要求是全序
 * @param serializer 溢出到磁盘时使用的序列化器
 * @param rowBasedChecksums 基于行的校验和计算器数组，用于Shuffle数据校验
 * 
 * 使用方式：
 * 1. 实例化ExternalSorter
 * 2. 调用insertAll()插入一批记录
 * 3. 请求iterator()获取排序聚合后的迭代器，或者调用writePartitionedMapOutput()写入分区输出文件
 * 
 * 内部工作原理：
 *  - 重复填充内存缓冲区，如果需要按key聚合则使用PartitionedAppendOnlyMap，否则使用PartitionedPairBuffer
 *  - 缓冲区达到内存限制时，将其排序后溢出到磁盘文件。文件按分区ID排序，每个文件记录每个分区的元素数量，不需要每个元素都存储分区ID
 *  - 用户请求结果时，将所有溢出文件和剩余内存数据合并，使用定义好的排序顺序
 *  - 使用结束后用户需要调用stop()删除所有中间临时文件
 */
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Serializer = SparkEnv.get.serializer,
    rowBasedChecksums: Array[RowBasedChecksum] = Array.empty)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
  with Logging with ShuffleChecksumSupport {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val actualPartitioner =
    if (numPartitions > 1) partitioner.get else new ConstantPartitioner

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  // Total spilling statistics
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SpillableIterator = null

  /** Checksum calculator for each partition. Empty when shuffle checksum disabled. */
  private val partitionChecksums = createPartitionChecksums(numPartitions, conf)

  def getChecksums: Array[Long] = getChecksumValues(partitionChecksums)

  def getRowBasedChecksums: Array[RowBasedChecksum] = rowBasedChecksums

  def getAggregatedChecksumValue: Long =
    RowBasedChecksum.getAggregatedChecksumValue(rowBasedChecksums)

  // 分区内key的比较器，用于聚合或排序。如果用户未提供全序，则按hash码进行偏序比较
  // 偏序意味着不同key也可能比较结果相等，后续需要进一步做相等性检查
  // 如果既没有聚合也没有排序，则忽略该比较器
  private val keyComparator: Comparator[K] = ordering.getOrElse((a: K, b: K) => {
    val h1 = if (a == null) 0 else a.hashCode()
    val h2 = if (b == null) 0 else b.hashCode()
    if (h1 < h2) -1 else if (h1 == h2) 0 else 1
  })

  /**
   * 获取key比较器：当需要排序或聚合时返回比较器，否则返回None
   * @return Some(比较器) 或 None
   */
  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  /**
   * 溢出文件信息结构体，存储临时溢出文件的元数据
   * @param file 磁盘上的临时文件
   * @param blockId 块ID
   * @param serializerBatchSizes 每个序列化批次的字节大小
   * @param elementsPerPartition 每个分区包含的元素数量
   */
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * 获取已溢出的文件数量，暴露给测试使用
   * @return 溢出文件数
   */
  private[spark] def numSpills: Int = spills.size

  /**
   * 插入一批键值对到排序器，按配置进行分区和聚合
   * @param records 待插入的键值对迭代器
   */
  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // 启用聚合，使用AppendOnlyMap在内存中合并值
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()
        kv = records.next()
        // 计算key的分区ID
        val partitionId = actualPartitioner.getPartition(kv._1)
        // 更新聚合值
        map.changeValue((partitionId, kv._1), update)
        // 判断是否需要溢出到磁盘
        maybeSpillCollection(usingMap = true)
        // 更新行校验和（如果启用）
        if (rowBasedChecksums.nonEmpty) {
          rowBasedChecksums(partitionId).update(kv._1, kv._2)
        }
      }
    } else {
      // 不启用聚合，直接插入到数组缓冲区
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        val partitionId = actualPartitioner.getPartition(kv._1)
        buffer.insert(partitionId, kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
        if (rowBasedChecksums.nonEmpty) {
          rowBasedChecksums(partitionId).update(kv._1, kv._2)
        }
      }
    }
  }

  /**
   * 在内存集合达到大小限制时，将其溢出到磁盘
   * @param usingMap 当前是否使用聚合map（true为map，false为pair缓冲区）
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    // 更新峰值内存使用量
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * 将内存集合溢出到排序文件，后续合并使用。添加溢出文件到 spills 列表
   * @param collection 待溢出的内存集合（map或buffer）
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // 获取按分区排序后的破坏性迭代器
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills += spillFile
  }

  /**
   * 强制将当前内存集合溢出到磁盘释放内存，由TaskMemoryManager在内存不足时调用
   * @return 是否成功溢出
   */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
   * 将内存迭代器的内容写入磁盘临时文件
   * @param inMemoryIterator 待写入的内存分区迭代器
   * @return 溢出文件信息结构体
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator[K, C])
      : SpilledFile = {
    // 因为这些文件后续会被Shuffle读取，压缩必须由spark.shuffle.compress控制，所以使用createTempShuffleBlock创建
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // 每次flush后重置这些变量
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // 按写入顺序存储每个批次的字节大小
    val batchSizes = new ArrayBuffer[Long]

    // 每个分区包含的元素数量
    val elementsPerPartition = new Array[Long](numPartitions)

    /**
     * 将磁盘写入器的内容刷新到磁盘，更新相关变量，提交写入
     */
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        // 达到批次大小后刷新
        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      // 处理剩余未写入的对象
      if (objectsWritten > 0) {
        flush()
        writer.close()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // 异常路径，关闭并删除临时文件
        writer.closeAndDelete()
      }
    }

    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /**
   * 合并多个已排序的溢出文件和内存数据，返回按分区分组的迭代器
   * 返回的迭代器按分区ID顺序排列，每个分区对应一个元素迭代器，必须按顺序访问分区
   * @param spills 溢出文件列表
   * @param inMemory 内存中剩余数据的迭代器
   * @return 按分区分组的迭代器 (分区ID, 分区元素迭代器)
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    // 为每个溢出文件创建读取器
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    // 遍历每个分区，构建分区迭代器
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (aggregator.isDefined) {
        // 需要聚合，跨多个迭代器合并相同key
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // 不需要聚合，但需要排序，直接归并排序
        (p, mergeSort(iterators, ordering.get))
      } else {
        // 既不需要聚合也不需要排序，直接拼接
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * 使用归并排序合并多个已排序的key-value迭代器
   * @param iterators 已按key排序的迭代器序列
   * @param comparator key比较器
   * @return 合并后的全局排序迭代器
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] = {
    // 过滤空迭代器，转换为缓冲迭代器
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    // PriorityQueue出队最大元素，所以使用逆序比较
    val heap = new mutable.PriorityQueue[Iter]()(
      (x: Iter, y: Iter) => comparator.compare(y.head._1, x.head._1))
    heap.enqueue(bufferedIters: _*)
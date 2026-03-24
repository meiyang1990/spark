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

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{STORAGE_UNROLL_MEMORY_THRESHOLD, UNROLL_MEMORY_CHECK_PERIOD, UNROLL_MEMORY_GROWTH_FACTOR}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * 内存存储块的抽象根 trait，定义存储块必须提供的基本属性
 * @tparam T 存储块中元素的类型
 */
private sealed trait MemoryEntry[T] {
  def size: Long
  def memoryMode: MemoryMode
  def classTag: ClassTag[T]
}

/**
 * 反序列化后的内存存储块，存储反序列化后的Java对象数组
 * @param value 存储的反序列化对象数组
 * @param size 占用内存大小（字节）
 * @param memoryMode 内存模式（堆内/堆外）
 * @param classTag 元素类型标记
 * @tparam T 存储块中元素的类型
 */
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
}

/**
 * 序列化后的内存存储块，存储序列化后的字节缓冲
 * @param buffer 序列化后的分块字节缓冲
 * @param memoryMode 内存模式（堆内/堆外）
 * @param classTag 元素类型标记
 * @tparam T 存储块中元素的类型
 */
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

/**
 * 块驱逐处理器接口，定义内存不足时驱逐块到磁盘的处理逻辑
 */
private[storage] trait BlockEvictionHandler {
  /**
   * 从内存中移除块，如有必要则写入磁盘。内存不足需要释放空间时调用。
   * 调用者必须在调用前持有块的写锁，该方法不会释放锁。
   * @param blockId 要移除的块ID
   * @param data 提供块数据的函数，返回反序列化数组或序列化缓冲
   * @tparam T 块中元素类型
   * @return 块驱逐后的有效存储级别
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
 * Spark执行器内存存储模块，负责将块存储在内存中，支持反序列化对象和序列化字节两种存储格式
 * 是Spark存储体系的核心内存层组件
 */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!

  // 使用带访问顺序的LinkedHashMap存储块，支持LRU顺序访问，用于驱逐策略
  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

  // 任务尝试ID -> 任务展开块占用的堆内内存大小（字节），所有访问必须在memoryManager上同步
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // 任务尝试ID -> 任务展开块占用的堆外内存大小（字节），仅在putIteratorAsBytes中使用
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // 展开块前初始申请的内存大小
  private val unrollMemoryThreshold: Long =
    conf.get(STORAGE_UNROLL_MEMORY_THRESHOLD)

  /** 获取可用于存储的总内存大小（字节），包含堆内和堆外 */
  private def maxMemory: Long = {
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(log"Max memory ${MDC(NUM_BYTES, Utils.bytesToString(maxMemory))} " +
      log"is less than the initial memory " +
      log"threshold ${MDC(MAX_SIZE, Utils.bytesToString(unrollMemoryThreshold))} " +
      log"needed to store a block in memory. Please configure Spark with more memory.")
  }

  logInfo(log"MemoryStore started with capacity " +
    log"${MDC(MEMORY_SIZE, Utils.bytesToString(maxMemory))}")

  /** 获取当前已使用的总存储内存大小（字节），包含展开内存 */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * 获取缓存块使用的存储内存大小（字节），不包含展开过程使用的临时内存
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  /**
   * 获取指定块在内存中的大小
   * @param blockId 块ID
   * @return 块大小（字节）
   */
  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * 将已序列化的字节块存入内存，先判断空间是否足够，足够才创建缓冲存储
   * 调用者必须保证传入的size是准确的
   * @param blockId 块ID
   * @param size 块大小（字节）
   * @param memoryMode 内存模式（堆内/堆外）
   * @param _bytes 生成字节缓冲的函数，仅在空间足够时调用
   * @tparam T 块中元素类型
   * @return 存储成功返回true，失败返回false
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      // 成功申请到内存，执行存储
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo(log"Block ${MDC(BLOCK_ID, blockId)} stored as bytes in memory " +
        log"(estimated size ${MDC(SIZE, Utils.bytesToString(size))}, " +
        log"free ${MDC(MEMORY_SIZE, Utils.bytesToString(maxMemory - blocksMemoryUsed))})")
      true
    } else {
      false
    }
  }

  /**
   * 尝试将迭代器展开并存入内存，支持渐进式展开检查内存，避免OOM
   * 展开过程使用的临时展开内存最终会转换为块的存储内存
   * @param blockId 块ID
   * @param values 要存储的元素迭代器
   * @param classTag 元素类型标记
   * @param memoryMode 内存模式（堆内/堆外）
   * @param valuesHolder 存储载体，支持反序列化和序列化两种存储方式
   * @tparam T 元素类型
   * @return Left(展开失败已使用的内存大小)，Right(存储成功后块的大小)
   */
  private def putIterator[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode,
      valuesHolder: ValuesHolder[T]): Either[Long, Long] = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // 已展开的元素数量
    var elementsUnrolled = 0
    // 是否还有足够内存继续展开
    var keepUnrolling = true
    // 初始每个任务展开块申请的内存阈值（字节）
    val initialMemoryThreshold = unrollMemoryThreshold
    // 检查是否需要申请更多内存的周期，每处理N个元素检查一次
    val memoryCheckPeriod = conf.get(UNROLL_MEMORY_CHECK_PERIOD)
    // 当前为展开操作保留的内存阈值
    var memoryThreshold = initialMemoryThreshold
    // 内存增长因子，申请额外内存时按当前大小的倍数增长
    val memoryGrowthFactor = conf.get(UNROLL_MEMORY_GROWTH_FACTOR)
    // 当前块本次展开操作已使用的内存大小
    var unrollMemoryUsedByThisBlock = 0L

    // 申请初始内存开始展开
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

    if (!keepUnrolling) {
      logWarning(log"Failed to reserve initial memory threshold of " +
        log"${MDC(NUM_BYTES, Utils.bytesToString(initialMemoryThreshold))} " +
        log"for computing block ${MDC(BLOCK_ID, blockId)} in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // 仅在执行器上需要检查线程中断
    val shouldCheckThreadInterruption = Option(TaskContext.get()).isDefined

    // 安全展开块，定期检查内存阈值和线程中断状态
    while (values.hasNext && keepUnrolling &&
      (!shouldCheckThreadInterruption || !Thread.currentThread().isInterrupted)) {
      valuesHolder.storeValue(values.next())
      // 达到检查周期，检查是否需要申请更多内存
      if (elementsUnrolled % memoryCheckPeriod == 0) {
        val currentSize = valuesHolder.estimatedSize()
        // 当前估算大小超过阈值，申请更多内存
        if (currentSize >= memoryThreshold) {
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // 更新阈值为新的大小
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    // SPARK-45025: 如果线程被中断，释放已申请的内存返回失败，避免任务被reaper杀死
    if (shouldCheckThreadInterruption && Thread.currentThread().isInterrupted) {
      logInfo(
        log"Failed to unroll block=${MDC(BLOCK_ID, blockId)} since thread interrupt was received")
      Left(unrollMemoryUsedByThisBlock)
    } else if (keepUnrolling) {
      // 展开完成，最终检查实际大小是否超过已申请内存，必要时再申请额外内存
      val entryBuilder = valuesHolder.getBuilder()
      val size = entryBuilder.preciseSize
      if (size > unrollMemoryUsedByThisBlock) {
        val amountToRequest = size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisBlock(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }

      if (keepUnrolling) {
        val entry = entryBuilder.build()
        // 原子转换：释放展开内存，申请存储内存
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
          val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
          assert(success, "transferring unroll memory to storage memory failed")
        }

        entries.synchronized {
          entries.put(blockId, entry)
        }

        logInfo(log"Block ${MDC(BLOCK_ID, blockId)} stored as values in memory " +
          log"(estimated size ${MDC(MEMORY_SIZE, Utils.bytesToString(entry.size))}, free " +
          log"${MDC(FREE_MEMORY_SIZE, Utils.bytesToString(maxMemory - blocksMemoryUsed))})")
        Right(entry.size)
      } else {
        // 申请最终内存失败，返回已使用内存
        logUnrollFailureMessage(blockId, entryBuilder.preciseSize)
        Left(unrollMemoryUsedByThisBlock)
      }
    } else {
      // 展开过程中内存不足失败，返回已使用内存
      logUnrollFailureMessage(blockId, valuesHolder.estimatedSize())
      Left(unrollMemoryUsedByThisBlock)
    }
  }

  /**
   * 尝试将迭代器以反序列化对象形式存入内存
   * @param blockId 块ID
   * @param values 元素迭代器
   * @param memoryMode 内存模式（堆内/堆外）
   * @param classTag 元素类型标记
   * @tparam T 元素类型
   * @return Right(存储成功后块大小)，Left(存储失败的部分展开迭代器，包含已展开元素和剩余元素)
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      memoryMode: MemoryMode,
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    val valuesHolder = new DeserializedValuesHolder[T](classTag, memoryMode)

    putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        val unrolledIterator = if (valuesHolder.vector != null) {
          valuesHolder.vector.iterator
        } else {
          valuesHolder.arrayValues.iterator
        }

        Left(new PartiallyUnrolledIterator(
          this,
          memoryMode,
          unrollMemoryUsedByThisBlock,
          unrolled = unrolledIterator,
          rest = values))
    }
  }

  /**
   * 尝试将迭代器以序列化字节形式存入内存
   * @param blockId 块ID
   * @param values 元素迭代器
   * @param classTag 元素类型标记
   * @param memoryMode 内存模式（堆内/堆外）
   * @tparam T 元素类型
   * @return Right(存储成功后块大小)，Left(存储失败的部分序列化块，支持继续写入磁盘或反序列化回迭代器)
   */
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // 初始每个任务展开块申请的内存阈值（字节）
    val initialMemoryThreshold = unrollMemoryThreshold
    // 分块大小，不超过JVM数组最大长度限制
    val chunkSize = if (initialMemoryThreshold > ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH) {
      logWarning(log"Initial memory threshold of " +
        log"${MDC(NUM_BYTES, Utils.bytesToString(initialMemoryThreshold))} " +
        log"is too large to be set as chunk size. Chunk size has been capped to " +
        log"${MDC(MAX_SIZE, Utils.bytesToString(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH))}")
      ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    } else {
      initialMemoryThreshold.toInt
    }

    val valuesHolder = new SerializedValuesHolder[T](blockId, chunkSize, classTag,
      memoryMode, serializerManager)

    val res = putIterator(blockId, values, classTag, memoryMode, valuesHolder) match {
      case Right(storedSize) => Right(storedSize)
      case Left(unrollMemoryUsedByThisBlock) =>
        Left(new PartiallySerializedBlock(
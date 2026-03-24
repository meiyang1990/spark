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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * 可溢写抽象基类，当内存集合大小超过阈值时，将内容溢写到磁盘释放内存，
 * 为Spark shuffle过程中处理大型集合提供内存溢出保护。
 * 
 * @param taskMemoryManager 任务内存管理器，用于管理当前任务的内存分配
 * @tparam C 要进行溢写操作的内存集合类型
 */
private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager, MemoryMode.ON_HEAP) with Logging {

  /**
   * 将当前内存集合溢写到磁盘，并释放占用的内存
   *
   * @param collection 需要溢写到磁盘的集合
   */
  protected def spill(collection: C): Unit

  /**
   * 强制将当前内存集合溢写到磁盘释放内存，任务内存不足时由TaskMemoryManager调用
   *
   * @return 是否成功完成强制溢写
   */
  protected def forceSpill(): Boolean

  // 获取上次溢写后读取的元素数量
  protected def elementsRead: Int = _elementsRead

  // 子类每读取一条记录调用一次，用于检查溢写频率
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // 开始跟踪内存使用前的集合大小初始阈值，仅用于测试
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD)

  // 达到此元素数量时强制溢写集合，仅用于测试
  private[this] val numElementsForceSpillThreshold: Int =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD)

  // 集合大小超过此阈值时强制溢写集合
  private[this] val maxSizeForceSpillThreshold: Long =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_MAX_SIZE_FORCE_SPILL_THRESHOLD)

  // 开始跟踪内存使用前的集合大小阈值，初始值远大于0避免频繁溢写小集合
  @volatile private[this] var myMemoryThreshold = initialMemoryThreshold

  // 上次溢写后从输入读取的元素数量
  private[this] var _elementsRead = 0

  // 累计溢写到磁盘的总字节数
  @volatile private[this] var _memoryBytesSpilled = 0L

  // 溢写次数
  private[this] var _spillCount = 0

  /**
   * 在需要时将当前内存集合溢写到磁盘，溢写前尝试申请更多内存
   *
   * @param collection 需要溢写的集合
   * @param currentMemory 当前集合估算大小（字节）
   * @return 是否执行了溢写
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    val shouldSpill = if (_elementsRead > numElementsForceSpillThreshold
      || currentMemory > maxSizeForceSpillThreshold) {
      // 达到元素数量或内存大小任一限制，触发溢写
      true
    } else if (_elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // 每读取32个元素才检查一次，降低检查开销
      // 尝试从shuffle内存池申请最多不超过当前内存两倍的空间
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireMemory(amountToRequest)
      myMemoryThreshold += granted
      // 如果申请到的内存仍不够继续增长，触发溢写
      currentMemory >= myMemoryThreshold
    } else {
      false
    }
    // 实际执行溢写
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory, _elementsRead)
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }

  /**
   * 溢写部分数据到磁盘释放内存，任务内存不足时由TaskMemoryManager调用
   * @param size 需要释放的内存大小
   * @param trigger 触发本次溢写的内存消费者
   * @return 实际释放的内存字节数
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val isSpilled = forceSpill()
      if (!isSpilled) {
        0L
      } else {
          // 计算释放的内存大小，更新统计信息
          val freeMemory = myMemoryThreshold - initialMemoryThreshold
          _memoryBytesSpilled += freeMemory
          releaseMemory()
          freeMemory
        }
    } else {
      0L
    }
  }

  /**
   * @return 累计溢写到磁盘的总字节数
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * 将占用的内存释放回执行内存池，供其他任务使用
   */
  def releaseMemory(): Unit = {
    freeMemory(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * 打印标准化的溢写日志信息
   *
   * @param size 本次溢写字节数
   * @param elements 上次溢写后读取的元素数
   */
  @inline private def logSpillage(size: Long, elements: Int): Unit = {
    val threadId = Thread.currentThread().getId
    logInfo(log"Thread ${MDC(LogKeys.THREAD_ID, threadId)} " +
      log"spilling in-memory map of ${MDC(LogKeys.BYTE_SIZE,
        org.apache.spark.util.Utils.bytesToString(size))} " +
      log"(elements: ${MDC(LogKeys.NUM_ELEMENTS_SPILL_RECORDS, elements)}) to disk " +
      log"(${MDC(LogKeys.NUM_SPILLS, _spillCount)} times so far)")
  }
}
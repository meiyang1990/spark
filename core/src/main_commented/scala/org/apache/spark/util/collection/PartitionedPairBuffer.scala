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

import java.util.Comparator

import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * 文件: core/src/main/scala/org/apache/spark/util/collection/PartitionedPairBuffer.scala
 * 描述: Spark核心模块工具类，实现带分区ID的键值对追加缓冲区，用于Shuffle阶段数据缓存，跟踪内存估算大小
 *
 * 带分区ID的仅追加键值对缓冲区，跟踪自身字节大小估算值
 *
 * 该缓冲区最多支持 1073741819 个元素
 */
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  // 检查初始容量不超过最大限制
  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  // 检查初始容量合法（不小于1）
  require(initialCapacity >= 1, "Invalid initial capacity")

  // 使用单个数组存储分区+键与值，方便KVArraySortDataFormat高效排序
  // 当前缓冲区容量
  private var capacity = initialCapacity
  // 当前已存储元素数量
  private var curSize = 0
  // 存储数据数组，每个元素占2个位置：第一个存(分区ID, 键)，第二个存值
  private var data = new Array[AnyRef](2 * initialCapacity)

  /**
   * 向缓冲区插入一个带分区ID的键值对
   * @param partition 分区ID
   * @param key 键
   * @param value 值
   */
  def insert(partition: Int, key: K, value: V): Unit = {
    if (curSize == capacity) {
      // 缓冲区已满，扩容数组
      growArray()
    }
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1
    // 更新大小跟踪采样
    afterUpdate()
  }

  /**
   * 数组容量不足时扩容，扩容为原容量的2倍，不超过最大限制
   */
  private def growArray(): Unit = {
    if (capacity >= MAXIMUM_CAPACITY) {
      // 已达到最大容量限制，抛出异常
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    val newCapacity =
      if (capacity * 2 > MAXIMUM_CAPACITY) { // 防止溢出，直接扩容到最大容量
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    // 创建新容量数组
    val newArray = new Array[AnyRef](2 * newCapacity)
    // 复制原数组数据到新数组
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)
    // 更新缓冲区引用
    data = newArray
    capacity = newCapacity
    // 重置大小采样，重新统计内存变化
    resetSamples()
  }

  /**
   * 对缓冲区数据按分区（和可选键）排序，返回排序后的迭代器，该操作非破坏性
   * @param keyComparator 可选的键比较器，用于同分区内键排序
   * @return 排序后的带分区键值对迭代器
   */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    // 根据是否提供键比较器生成对应比较器
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    // 使用KV数组排序器对数据原地排序
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    // 返回迭代器
    iterator()
  }

  /**
   * 创建按顺序遍历缓冲区数据的迭代器
   * @return 带分区键值对迭代器
   */
  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        // 无更多元素，抛出异常
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

/**
 * PartitionedPairBuffer伴生对象，定义缓冲区最大容量常量
 */
private object PartitionedPairBuffer {
  // 最大容量，基于JVM数组最大长度减半（每个元素占2个数组位置）
  val MAXIMUM_CAPACITY: Int = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 2
}
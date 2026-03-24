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

/**
 * 文件说明: 可写分区键值对集合公共接口，用于Spark Shuffle过程中内存高效排序与写入
 * 
 * 核心特性:
 *  - 每个键值对关联分区编号
 *  - 支持内存高效的排序迭代器
 *  - 支持直接将内容以字节形式写出的可写分区迭代器
 */
private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * 向集合中插入带分区编号的键值对
   * 
   * @param partition 键值对所属分区编号
   * @param key 键
   * @param value 值
   */
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * 按照分区ID升序，再根据指定键比较器排序，返回排序后的迭代器
   * 注意：此方法可能会破坏底层集合结构，调用后集合不可再用
   * 
   * @param keyComparator 键的可选比较器，None表示仅按分区排序
   * @return 排序后的((分区ID, 键), 值)迭代器
   */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)]

  /**
   * 返回按分区和键排序后的可写分区迭代器，支持直接写出元素
   * 注意：此方法可能会破坏底层集合结构，调用后集合不可再用
   * 
   * @param keyComparator 键的可选比较器，None表示仅按分区排序
   * @return 可写分区迭代器实例
   */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator[K, V] = {
    val it = partitionedDestructiveSortedIterator(keyComparator)
    new WritablePartitionedIterator[K, V](it)
  }
}

/**
 * WritablePartitionedPairCollection的伴生对象，提供通用比较器实现
 */
private[spark] object WritablePartitionedPairCollection {
  /**
   * 仅按分区ID对(分区ID, 键)二元组排序的比较器
   * 
   * @return 分区比较器实例
   */
  def partitionComparator[K]: Comparator[(Int, K)] = (a: (Int, K), b: (Int, K)) => a._1 - b._1

  /**
   * 先按分区ID排序，分区相同时再按指定键比较器排序的比较器
   * 
   * @param keyComparator 键的比较器
   * @return 分区+键组合比较器实例
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] =
    (a: (Int, K), b: (Int, K)) => {
      val partitionDiff = a._1 - b._1
      if (partitionDiff != 0) {
        partitionDiff
      } else {
        keyComparator.compare(a._2, b._2)
      }
    }
}

/**
 * 可写分区迭代器，不直接返回元素，而是支持将元素直接写入磁盘块写出器
 * 用于Shuffle过程中排序后直接溢写磁盘，减少内存拷贝
 * 
 * @param it 已排序的((分区ID, 键), 值)迭代器
 */
private[spark] class WritablePartitionedIterator[K, V](it: Iterator[((Int, K), V)]) {
  // 缓存当前待处理元素
  private[this] var cur = if (it.hasNext) it.next() else null

  /**
   * 将当前元素写入指定写出器，并移动到下一个元素
   * 
   * @param writer 键值对写出器
   */
  def writeNext(writer: PairsWriter): Unit = {
    writer.write(cur._1._2, cur._2)
    cur = if (it.hasNext) it.next() else null
  }

  /**
   * 检查是否还有未处理的元素
   * 
   * @return true表示还有元素，false表示迭代完成
   */
  def hasNext: Boolean = cur != null

  /**
   * 获取当前元素所属的分区编号
   * 
   * @return 当前元素的分区ID
   */
  def nextPartition(): Int = cur._1._1
}
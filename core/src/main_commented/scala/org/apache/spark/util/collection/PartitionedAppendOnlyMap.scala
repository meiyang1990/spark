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
 * Unless required by applicable law or agreed to in agreement, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * 文件说明：基于分区的仅追加哈希表，实现按分区聚合键值对集合
 * 核心功能：维护键为(分区ID, K)元组的仅追加映射表，用于Shuffle阶段按键分区聚合数据
 */
private[spark] class PartitionedAppendOnlyMap[K, V]
  extends SizeTrackingAppendOnlyMap[(Int, K), V] with WritablePartitionedPairCollection[K, V] {

  /**
   * 按分区生成排序后的破坏性迭代器，排序过程会修改原集合数据
   * @param keyComparator 用户自定义键比较器，用于分区内对键排序
   * @return 排序后的(分区键值对迭代器
   */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    // 根据是否提供键比较器，生成对应分区键比较器
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    destructiveSortedIterator(comparator)
  }

  /**
   * 向集合中插入一条分区键值对
   * @param partition 目标分区ID
   * @param key 数据键
   * @param value 数据值
   */
  def insert(partition: Int, key: K, value: V): Unit = {
    update((partition, key), value)
  }
}
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

import java.util.Collections

import scala.collection.immutable
import scala.jdk.CollectionConverters._

import com.google.common.collect.{Iterators => GuavaIterators, Ordering => GuavaOrdering}

import org.apache.spark.util.SparkCollectionUtils

/**
 * 文件级：集合工具类，提供Spark核心模块中常用的集合操作辅助函数
 * 集合操作工具类，提供集合排序、合并、转换等通用工具能力
 */
private[spark] object Utils extends SparkCollectionUtils {

  /**
   * 从输入迭代器中获取排序后的前K个最小元素，保持排序结果有序
   * 基于Guava的Ordering实现高效的Top-K选取，避免全量排序
   *
   * @param input 输入元素迭代器
   * @param num 需要选取的元素数量K
   * @param ord 元素排序规则
   * @return 排序后的前K个元素迭代器
   */
  def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    ordering.leastOf(input.asJava, num).iterator.asScala
  }

  /**
   * 合并多个已经有序的迭代器，输出保持整体有序的合并结果
   * 不会对重复元素去重，要求所有输入迭代器已经按照相同排序规则提前排序
   *
   * @param inputs 多个已经有序的输入迭代器集合
   * @param ord 元素排序规则
   * @return 合并后整体有序的元素迭代器
   */
  def mergeOrdered[T](inputs: Iterable[IterableOnce[T]])(
    implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    GuavaIterators.mergeSorted(
      inputs.map(_.iterator.asJava).asJava, ordering).asScala
  }

  /**
   * 将Option序列转换为Option包裹的序列，仅当所有元素都为Some时返回结果
   * 只要存在一个None元素，直接返回None
   *
   * @param input 包含Option元素的输入序列
   * @return 全部元素都存在时返回Some(展开后的元素序列)，否则返回None
   */
  def sequenceToOption[T](input: Seq[Option[T]]): Option[Seq[T]] =
    if (input.forall(_.isDefined)) Some(input.flatten) else None

  /**
   * 将两个分别包含键和值的可迭代集合转换为不可变Map，性能优于zip后转Map
   *
   * @param keys 键集合
   * @param values 值集合
   * @return 键值对组成的不可变Scala Map
   */
  def toMap[K, V](keys: Iterable[K], values: Iterable[V]): Map[K, V] = {
    val builder = immutable.Map.newBuilder[K, V]
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      builder += (keyIter.next(), valueIter.next()).asInstanceOf[(K, V)]
    }
    builder.result()
  }

  /**
   * 将两个分别包含键和值的可迭代集合转换为不可变Java Map，性能优于zip后转Java Map
   *
   * @param keys 键集合
   * @param values 值集合
   * @return 键值对组成的不可变Java Map
   */
  def toJavaMap[K, V](keys: Iterable[K], values: Iterable[V]): java.util.Map[K, V] = {
    val map = new java.util.HashMap[K, V]()
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      map.put(keyIter.next(), valueIter.next())
    }
    Collections.unmodifiableMap(map)
  }
}
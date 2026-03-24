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

package org.apache.spark.util

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

import scala.collection.mutable.Growable
import scala.jdk.CollectionConverters._

/**
 * 文件：BoundedPriorityQueue.scala
 * 所属模块：Spark核心工具模块
 * 核心功能：实现有容量上限的优先队列，仅保留优先级最高的N个元素
 * 应用场景：常用于分布式计算中获取Top K元素的场景，在每个分区维护局部Top K结果，减少数据传输量
 */

/**
 * 有界优先队列，封装Java原生优先队列，保证队列大小不超过设定上限，仅保留优先级最高的元素
 * @param maxSize 队列最大容量，超过该容量时会移除优先级最低的元素
 * @param ord 元素排序规则，隐式传入，用于定义元素优先级
 * @tparam A 队列中存储的元素类型
 * @设计目的：用于高效获取Top K元素，在分布式计算中每个分区先计算得到局部Top K，再合并得到全局Top K，大幅降低数据传输和计算开销
 */
private[spark] class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  // 底层基于Java PriorityQueue实现存储
  private val underlying = new JPriorityQueue[A](maxSize, ord)

  override def iterator: Iterator[A] = underlying.iterator.asScala

  override def size: Int = underlying.size

  override def knownSize: Int = size

  override def addAll(xs: IterableOnce[A]): this.type = {
    xs.iterator.foreach { this += _ }
    this
  }

  /**
   * 向队列添加单个元素，如果队列未满直接加入；如果已满则尝试替换队列中优先级最低的元素
   * @param elem 待添加的元素
   * @return 当前队列对象，支持链式调用
   */
  override def addOne(elem: A): this.type = {
    if (size < maxSize) {
      // 队列未满，直接入队
      underlying.offer(elem)
    } else {
      // 队列已满，尝试替换优先级最低元素
      maybeReplaceLowest(elem)
    }
    this
  }

  /**
   * 弹出并返回队列中优先级最低的元素
   * @return 优先级最低的元素
   */
  def poll(): A = {
    underlying.poll()
  }

  override def clear(): Unit = { underlying.clear() }

  /**
   * 当队列已满时，判断新元素是否需要替换队列中优先级最低的元素
   * 如果新元素优先级高于当前最低元素，则移除最低元素，加入新元素；否则不做修改
   * @param a 待加入的新元素
   * @return 是否发生了替换操作
   */
  private def maybeReplaceLowest(a: A): Boolean = {
    // 获取当前队列中优先级最低的元素（堆顶元素）
    val head = underlying.peek()
    if (head != null && ord.gt(a, head)) {
      // 新元素优先级更高，移除原最低元素，加入新元素
      underlying.poll()
      underlying.offer(a)
      true
    } else {
      // 新元素优先级更低，不替换
      false
    }
  }
}
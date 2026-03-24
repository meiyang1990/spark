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

import java.util.PriorityQueue

/**
 * 文件: PercentileHeap.scala
 * 所属模块: Spark core 公共工具模块
 * 核心功能: 实现基于双堆结构的高效百分位数计算器，支持O(log n)插入、O(1)查询
 */

/**
 * 双堆结构百分位数计算器，维护数值集合的目标百分位数
 * 
 * 设计原理：使用两个堆分别存储目标百分位数左右两侧的数值：
 *  - 小堆(smallHeap)存储小于目标百分位数的所有数值
 *  - 大堆(largeHeap)存储大于等于目标百分位数的所有数值
 * 插入时动态调整两个堆的大小比例，使其匹配目标百分位数，保证大堆堆顶即为所求百分位数
 * 
 * 时间复杂度：插入O(log n)，查询O(1)
 * 
 * @param percentage 目标百分位数，取值范围(0, 1)，默认0.5即中位数
 */
private[spark] class PercentileHeap(percentage: Double = 0.5) {
  assert(percentage > 0 && percentage < 1)

  // 大堆，存储大于等于百分位数的数值，使用最小堆，堆顶即为所求百分位数
  private[this] val largeHeap = new PriorityQueue[Double]
  // 小堆，存储小于百分位数的数值，通过存储负值实现最大堆效果，避免自定义比较器带来的性能开销
  private[this] val smallHeap = new PriorityQueue[Double]

  // SPARK-52418: 缓存空集合状态，避免重复计算isEmpty()提升性能
  private[this] var noElements = true

  def isEmpty(): Boolean = noElements

  def size(): Int = smallHeap.size + largeHeap.size

  /**
   * 获取当前插入元素集合的目标百分位数
   * 
   * 计算规则：将所有元素排序后，取索引为 `(元素总数 * 目标百分比).toInt` 位置的值
   * 
   * @return 目标百分位数值
   * @throws NoSuchElementException 当集合为空时抛出
   */
  def percentile(): Double = {
    if (isEmpty()) throw new NoSuchElementException("empty")
    largeHeap.peek
  }

  /**
   * 向集合插入一个新数值，自动调整双堆结构保持百分位数正确
   * 
   * @param x 待插入的数值
   */
  def insert(x: Double): Unit = {
    if (isEmpty()) {
      // 空集合插入第一个元素，直接放入大堆
      largeHeap.offer(x)
      noElements = false
    } else {
      // 获取当前百分位数作为分界参考
      val p = largeHeap.peek
      // 判断插入后是否需要增大小堆容量以保持目标比例
      val growBot = ((size() + 1) * percentage).toInt > smallHeap.size
      if (growBot) {
        // 需要增大小堆容量
        if (x < p) {
          // 新值小于当前百分位数，直接放入小堆
          smallHeap.offer(-x)
        } else {
          // 新值大于等于当前百分位数，放入大堆后将大堆最小值移到小堆
          largeHeap.offer(x)
          smallHeap.offer(-largeHeap.poll)
        }
      } else {
        // 不需要增大小堆容量
        if (x < p) {
          // 新值小于当前百分位数，放入小堆后将小堆最大值移到大堆
          smallHeap.offer(-x)
          largeHeap.offer(-smallHeap.poll)
        } else {
          // 新值大于等于当前百分位数，直接放入大堆
          largeHeap.offer(x)
        }
      }
    }
  }
}
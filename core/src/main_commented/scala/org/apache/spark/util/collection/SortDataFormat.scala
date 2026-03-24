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

import scala.reflect.ClassTag

/**
 * 文件概述：Spark内部排序数据格式抽象基类，为任意数据缓冲区定义排序所需的统一操作接口，
 * 用于Shuffle排序等场景中支持不同数据存储格式的统一排序逻辑。
 * 
 * Abstraction for sorting an arbitrary input buffer of data. This interface requires determining
 * the sort key for a given element index, as well as swapping elements and moving data from one
 * buffer to another.
 *
 * Example format: an array of numbers, where each element is also the key.
 * See [[KVArraySortDataFormat]] for a more exciting format.
 *
 * Note: Declaring and instantiating multiple subclasses of this class would prevent JIT inlining
 * overridden methods and hence decrease the shuffle performance.
 *
 * @tparam K 每个元素排序键的类型
 * @tparam Buffer 特定格式使用的内部数据结构（例如Array[Int]）
 */
// TODO: Making Buffer a real trait would be a better abstraction, but adds some complexity.
private[spark]
abstract class SortDataFormat[K, Buffer] {

  /**
   * 创建一个可复用的新键对象，用于对象复用减少GC。如果需要覆写
   * [[getKey(Buffer, Int, K)]]方法，则必须实现此方法。
   */
  def newKey(): K = null.asInstanceOf[K]

  /** 
   * 获取指定索引位置元素的排序键 
   * @param data 数据缓冲区
   * @param pos 元素索引位置
   * @return 元素对应的排序键
   */
  protected def getKey(data: Buffer, pos: Int): K

  /**
   * 获取指定索引位置元素的排序键，尽可能复用传入的已有键对象减少内存分配。
   * 默认实现忽略复用参数，直接调用[[getKey(Buffer, Int)]]。如果要覆写此方法，必须同时实现[[newKey()]]。
   * 
   * @param data 数据缓冲区
   * @param pos 元素索引位置
   * @param reuse 可复用的已有键对象
   * @return 元素对应的排序键
   */
  def getKey(data: Buffer, pos: Int, reuse: K): K = {
    getKey(data, pos)
  }

  /** 
   * 交换缓冲区中两个位置的元素 
   * @param data 数据缓冲区
   * @param pos0 第一个元素索引
   * @param pos1 第二个元素索引
   */
  def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  /** 
   * 将源缓冲区指定位置的单个元素复制到目标缓冲区指定位置 
   * @param src 源数据缓冲区
   * @param srcPos 源元素索引
   * @param dst 目标数据缓冲区
   * @param dstPos 目标位置索引
   */
  def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  /**
   * 将源缓冲区从srcPos开始的一段连续元素复制到目标缓冲区从dstPos开始的位置，支持重叠区域复制。
   * 
   * @param src 源数据缓冲区
   * @param srcPos 源起始索引
   * @param dst 目标数据缓冲区
   * @param dstPos 目标起始索引
   * @param length 要复制的元素数量
   */
  def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  /**
   * 分配一个可容纳指定数量元素的新缓冲区，分配后缓冲区元素在显式复制数据前均为无效状态。
   * 
   * @param length 可容纳的最大元素数量
   * @return 新分配的空缓冲区
   */
  def allocate(length: Int): Buffer
}

/**
 * 键值对数组排序数据格式实现，支持数组中键值交替存储的键值对排序，用于AppendOnlyMap的排序场景。
 * 
 * @tparam K 每个元素排序键的类型
 * @tparam T 待排序数组的元素类型，通常需要扩展AnyRef以支持键和值类型不同的场景
 */
private[spark]
class KVArraySortDataFormat[K, T <: AnyRef : ClassTag] extends SortDataFormat[K, Array[T]] {

  override def getKey(data: Array[T], pos: Int): K = data(2 * pos).asInstanceOf[K]

  override def swap(data: Array[T], pos0: Int, pos1: Int): Unit = {
    val tmpKey = data(2 * pos0)
    val tmpVal = data(2 * pos0 + 1)
    data(2 * pos0) = data(2 * pos1)
    data(2 * pos0 + 1) = data(2 * pos1 + 1)
    data(2 * pos1) = tmpKey
    data(2 * pos1 + 1) = tmpVal
  }

  override def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int): Unit = {
    dst(2 * dstPos) = src(2 * srcPos)
    dst(2 * dstPos + 1) = src(2 * srcPos + 1)
  }

  override def copyRange(src: Array[T], srcPos: Int,
      dst: Array[T], dstPos: Int, length: Int): Unit = {
    System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
  }

  override def allocate(length: Int): Array[T] = {
    new Array[T](2 * length)
  }
}
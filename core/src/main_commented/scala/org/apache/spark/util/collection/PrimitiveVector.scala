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
 * 只支持追加操作、非线程安全、基于数组实现的原生类型优化向量
 * 针对Long、Int、Double类型做了特殊优化，避免自动装箱开销
 */
private[spark]
class PrimitiveVector[@specialized(Long, Int, Double) V: ClassTag](initialSize: Int = 64) {
  private var _numElements = 0
  private var _array: Array[V] = _

  // 注意：必须和变量声明分开，否则特化父类会创建同名同大小的额外数组
  _array = new Array[V](initialSize)

  /**
   * 获取指定索引位置的元素
   * @param index 元素索引
   * @return 索引位置对应的元素
   */
  def apply(index: Int): V = {
    require(index < _numElements)
    _array(index)
  }

  /**
   * 向向量尾部追加一个元素
   * @param value 待追加的元素值
   */
  def +=(value: V): Unit = {
    // 数组已满，扩容为原大小的2倍
    if (_numElements == _array.length) {
      resize(_array.length * 2)
    }
    _array(_numElements) = value
    _numElements += 1
  }

  /**
   * 获取向量当前的容量（底层数组长度）
   * @return 当前容量
   */
  def capacity: Int = _array.length

  /**
   * 获取向量已存储的元素数量
   * @return 元素数量
   */
  def length: Int = _numElements

  /**
   * 获取向量已存储的元素数量
   * @return 元素数量
   */
  def size: Int = _numElements

  /**
   * 获取遍历向量元素的迭代器
   * @return 元素迭代器
   */
  def iterator: Iterator[V] = new Iterator[V] {
    var index = 0
    override def hasNext: Boolean = index < _numElements
    override def next(): V = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val value = _array(index)
      index += 1
      value
    }
  }

  /** Gets the underlying array backing this vector. */
  def array: Array[V] = _array

  /** Trims this vector so that the capacity is equal to the size. */
  def trim(): PrimitiveVector[V] = resize(size)

  /** Resizes the array, dropping elements if the total length decreases. */
  def resize(newLength: Int): PrimitiveVector[V] = {
    _array = copyArrayWithLength(newLength)
    if (newLength < _numElements) {
      _numElements = newLength
    }
    this
  }

  /** Return a trimmed version of the underlying array. */
  def toArray: Array[V] = {
    copyArrayWithLength(size)
  }

  /**
   * 复制当前数组内容到指定长度的新数组
   * @param length 新数组长度
   * @return 复制完成的新数组
   */
  private def copyArrayWithLength(length: Int): Array[V] = {
    val copy = new Array[V](length)
    _array.copyToArray(copy)
    copy
  }
}
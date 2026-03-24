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

import org.apache.spark.unsafe.array.ByteArrayMethods

/**
 * 一种仅追加的缓冲区，类似ArrayBuffer但对小缓冲区有更高的内存效率。
 * ArrayBuffer总是分配一个对象数组存储数据，默认初始大小为16，因此有大约80-100字节的 overhead。
 * 相比之下，CompactBuffer最多可以将前两个元素存储在主对象的字段中，只有元素超过两个时才会分配额外数组。
 * 这种设计让它在groupBy这类场景中更高效，因为这类场景中很多key只会对应很少几个元素。
 *
 * @tparam T 缓冲区存储的元素类型
 */
private[spark] class CompactBuffer[T: ClassTag] extends Seq[T] with Serializable {
  // 第一个元素
  private var element0: T = _
  // 第二个元素
  private var element1: T = _

  // 元素总数量，包含存储在主对象字段中的前两个元素
  private var curSize = 0

  // 存储第三个及之后元素的额外数组
  private var otherElements: Array[T] = null

  /**
   * 根据索引获取缓冲区中的元素
   * @param position 元素索引
   * @return 对应索引位置的元素
   */
  def apply(position: Int): T = {
    if (position < 0 || position >= curSize) {
      throw new IndexOutOfBoundsException
    }
    if (position == 0) {
      element0
    } else if (position == 1) {
      element1
    } else {
      otherElements(position - 2)
    }
  }

  /**
   * 更新指定索引位置的元素值
   * @param position 元素索引
   * @param value 新元素值
   */
  private def update(position: Int, value: T): Unit = {
    if (position < 0 || position >= curSize) {
      throw new IndexOutOfBoundsException
    }
    if (position == 0) {
      element0 = value
    } else if (position == 1) {
      element1 = value
    } else {
      otherElements(position - 2) = value
    }
  }

  /**
   * 向缓冲区末尾追加一个元素
   * @param value 待追加的元素
   * @return 当前缓冲区本身
   */
  def += (value: T): CompactBuffer[T] = {
    val newIndex = curSize
    if (newIndex == 0) {
      element0 = value
      curSize = 1
    } else if (newIndex == 1) {
      element1 = value
      curSize = 2
    } else {
      growToSize(curSize + 1)
      otherElements(newIndex - 2) = value
    }
    this
  }

  /**
   * 将另一个集合的所有元素追加到当前缓冲区末尾
   * @param values 待追加的元素集合
   * @return 当前缓冲区本身
   */
  def ++= (values: IterableOnce[T]): CompactBuffer[T] = {
    values match {
      // 对CompactBuffer合并做优化，该优化用于cogroup和groupByKey操作
      case compactBuf: CompactBuffer[T] =>
        val oldSize = curSize
        // 将另一个缓冲区的大小和元素拷贝到局部变量，避免处理self-add场景的问题
        val itsSize = compactBuf.curSize
        val itsElements = compactBuf.otherElements
        growToSize(curSize + itsSize)
        if (itsSize == 1) {
          this(oldSize) = compactBuf.element0
        } else if (itsSize == 2) {
          this(oldSize) = compactBuf.element0
          this(oldSize + 1) = compactBuf.element1
        } else if (itsSize > 2) {
          this(oldSize) = compactBuf.element0
          this(oldSize + 1) = compactBuf.element1
          // 此时当前缓冲区大小也超过2，直接将对方数组拷贝到当前数组
          // 注意：因为我们已经添加了上面两个元素，所以当前数组中开始拷贝的位置就是oldSize
          System.arraycopy(itsElements, 0, otherElements, oldSize, itsSize - 2)
        }

      case _ =>
        values.iterator.foreach(e => this += e)
    }
    this
  }

  override def length: Int = curSize

  override def iterator: Iterator[T] = new Iterator[T] {
    private var pos = 0
    override def hasNext: Boolean = pos < curSize
    override def next(): T = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      pos += 1
      apply(pos - 1)
    }
  }

  /**
   * 将缓冲区扩容到指定大小，必要时扩展底层存储数组
   * @param newSize 目标新大小
   */
  private def growToSize(newSize: Int): Unit = {
    // 因为前两个元素存储在字段中，数组只需要存储newSize - 2个元素
    val newArraySize = newSize - 2
    val arrayMax = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    if (newSize < 0 || newArraySize > arrayMax) {
      throw new UnsupportedOperationException(s"Can't grow buffer past $arrayMax elements")
    }
    // 获取当前数组容量，如果数组未创建则容量为0
    val capacity = if (otherElements != null) otherElements.length else 0
    // 仅当所需容量超过当前容量时才需要扩容
    if (newArraySize > capacity) {
      // 从8开始，按2倍指数增长计算新数组长度
      var newArrayLen = 8L
      while (newArraySize > newArrayLen) {
        newArrayLen *= 2
      }
      // 不超过最大允许数组长度
      if (newArrayLen > arrayMax) {
        newArrayLen = arrayMax
      }
      // 创建新数组并拷贝原有数据
      val newArray = new Array[T](newArrayLen.toInt)
      if (otherElements != null) {
        System.arraycopy(otherElements, 0, newArray, 0, otherElements.length)
      }
      otherElements = newArray
    }
    curSize = newSize
  }
}

/**
 * CompactBuffer的工厂对象，提供便捷构造方法
 */
private[spark] object CompactBuffer {
  /**
   * 构造空的CompactBuffer
   * @tparam T 元素类型
   * @return 新建的空缓冲区
   */
  def apply[T: ClassTag](): CompactBuffer[T] = new CompactBuffer[T]

  /**
   * 构造包含单个元素的CompactBuffer
   * @param value 初始元素
   * @tparam T 元素类型
   * @return 新建的包含初始元素的缓冲区
   */
  def apply[T: ClassTag](value: T): CompactBuffer[T] = {
    val buf = new CompactBuffer[T]
    buf += value
  }
}
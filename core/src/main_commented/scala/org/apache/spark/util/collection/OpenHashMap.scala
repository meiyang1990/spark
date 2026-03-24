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
 * 文件说明：Spark核心工具模块中的开放寻址哈希表实现，用于支持快速插入查询且不支持删除，
 * 相比JDK HashMap性能更高、空间开销更低，内部基于OpenHashSet实现键存储。
 * 
 * 支持空键存储，针对数值类型键值做了特殊化优化，是Spark内部高性能数据结构之一。
 *
 * A fast hash map implementation for nullable keys. This hash map supports insertions and updates,
 * but not deletions. This map is about 5X faster than java.util.HashMap, while using much less
 * space overhead.
 *
 * Under the hood, it uses our OpenHashSet implementation.
 *
 * NOTE: when using numeric type as the value type, the user of this class should be careful to
 * distinguish between the 0/0.0/0L and non-exist value
 */
private[spark]
class OpenHashMap[K : ClassTag, @specialized(Long, Int, Double) V: ClassTag](
    initialCapacity: Int)
  extends Iterable[(K, V)]
  with Serializable {

  /** 使用默认初始容量64构造哈希表 */
  def this() = this(64)

  // 存储所有键的开放哈希集合
  protected var _keySet = new OpenHashSet[K](initialCapacity)

  // Init in constructor (instead of in declaration) to work around a Scala compiler specialization
  // bug that would generate two arrays (one for Object and one for specialized T).
  // 存储对应键的值，数组下标与keySet中的位置一一对应
  private var _values: Array[V] = _
  _values = new Array[V](_keySet.capacity)

  @transient private var _oldValues: Array[V] = null

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  // 是否存在空键对应的value
  private var haveNullValue = false
  // 空键对应的值
  private var nullValue: V = null.asInstanceOf[V]

  /** 获取哈希表中键值对总数 */
  override def size: Int = if (haveNullValue) _keySet.size + 1 else _keySet.size

  /** Tests whether this map contains a binding for a key. */
  /** 检查哈希表中是否包含指定键 */
  def contains(k: K): Boolean = {
    if (k == null) {
      haveNullValue
    } else {
      _keySet.getPos(k) != OpenHashSet.INVALID_POS
    }
  }

  /** Get the value for a given key */
  /** 根据键获取对应值，键不存在则返回null */
  def apply(k: K): V = {
    if (k == null) {
      nullValue
    } else {
      val pos = _keySet.getPos(k)
      if (pos < 0) {
        null.asInstanceOf[V]
      } else {
        _values(pos)
      }
    }
  }

  /** Get the value for a given key, return None if the key doesn't exist */
  /** 根据键获取对应值，键不存在则返回None */
  def get(k: K): Option[V] = {
    if (k == null) {
      if (haveNullValue) {
        Some(nullValue)
      } else {
        None
      }
    } else {
      val pos = _keySet.getPos(k)
      if (pos < 0) {
        None
      } else {
        Some(_values(pos))
      }
    }
  }

  /** Set the value for a key */
  /** 更新或插入指定键对应的值 */
  def update(k: K, v: V): Unit = {
    if (k == null) {
      haveNullValue = true
      nullValue = v
    } else {
      val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
      _values(pos) = v
      _keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    }
  }

  /**
   * 如果键不存在则使用defaultValue初始化，否则使用mergeValue合并旧值，返回更新后的值。
   * 常用于聚合场景，在一次查找操作中完成值的更新，减少哈希查找次数。
   *
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
    if (k == null) {
      if (haveNullValue) {
        nullValue = mergeValue(nullValue)
      } else {
        haveNullValue = true
        nullValue = defaultValue
      }
      nullValue
    } else {
      val pos = _keySet.addWithoutResize(k)
      if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = defaultValue
        _values(pos & OpenHashSet.POSITION_MASK) = newValue
        _keySet.rehashIfNeeded(k, grow, move)
        newValue
      } else {
        _values(pos) = mergeValue(_values(pos))
        _values(pos)
      }
    }
  }

  /** 迭代器实现，按顺序遍历所有键值对，优先返回空键 */
  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    // 当前遍历位置，-1代表准备遍历空键
    var pos = -1
    // 缓存下一个待返回的键值对
    var nextPair: (K, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating */
    /** 计算下一个待返回的键值对，遍历完成返回null */
    def computeNextPair(): (K, V) = {
      if (pos == -1) {    // Treat position -1 as looking at the null value
        if (haveNullValue) {
          pos += 1
          return (null.asInstanceOf[K], nullValue)
        }
        pos += 1
      }
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = (_keySet.getValue(pos), _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    /** 检查是否还有未遍历的元素 */
    def hasNext: Boolean = nextPair != null

    /** 返回下一个键值对 */
    def next(): (K, V) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  /** 重新哈希扩容时，分配新的值数组，保存旧数组引用 */
  private def grow(newCapacity: Int): Unit = {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  /** 重新哈希扩容时，将旧位置的值移动到新位置 */
  private def move(oldPos: Int, newPos: Int): Unit = {
    _values(newPos) = _oldValues(oldPos)
  }
}
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

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * 文件说明: 仅追加的开放哈希表实现，针对只追加不删除key的场景优化，支持修改已有key对应的value
 *
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
@DeveloperApi
/**
 * 仅追加哈希表类，核心用于只追加不删除key的聚合场景，优化了内存局部性和插入性能
 * @param initialCapacity 初始容量
 * @tparam K key类型
 * @tparam V value类型
 */
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // 负载因子，达到负载后触发扩容
  private val LOAD_FACTOR = 0.7

  // 当前哈希表容量，总是2的幂次
  private var capacity = nextPowerOf2(initialCapacity)
  // 掩码，用于哈希取模，等价于capacity-1，因为capacity是2的幂次
  private var mask = capacity - 1
  // 当前已存储元素数量
  private var curSize = 0
  // 触发扩容的阈值，等于容量乘以负载因子
  private var growThreshold = (LOAD_FACTOR * capacity).toInt

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  /** 存储键值对的数组，key和value连续存储提升内存局部性，顺序: key0, value0, key1, value1... */
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  /** 是否存储了null key */
  private var haveNullValue = false
  /** null key对应的value */
  private var nullValue: V = null.asInstanceOf[V]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  /** 标记哈希表是否已被破坏性排序迭代器销毁，销毁后无法再进行读写 */
  private var destroyed = false
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /** 
   * 根据key获取对应value
   * @param key 要查询的key
   * @return key对应的value，不存在则返回null
   */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /** 
   * 更新指定key的value值，key不存在则插入新key
   * @param key 要更新的key
   * @param value 新的value
   */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        incrementSize()  // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * 使用用户提供的更新函数修改指定key的value，key不存在则插入新key
   * @param key 要更新的key
   * @param updateFunc 更新函数，输入为是否存在旧值和旧值，输出为新value
   * @return 更新后的新value
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else if (k.eq(curKey) || k.equals(curKey)) {
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** 
   * 获取无序迭代器遍历所有键值对
   * @return 无序迭代器
   * Iterator method from Iterable 
   */
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1

      /** Get the next value we should return from next(), or null if we're finished iterating */
      def nextValue(): (K, V) = {
        if (pos == -1) {    // Treat position -1 as looking at the null value
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /** Increase table size by 1, rehashing if necessary */
  /** 增加元素计数，超过阈值则触发哈希表扩容 */
  private def incrementSize(): Unit = {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   * 使用Murmur3哈希二次散列，减少原始哈希低位冲突概率
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32_fixed().hashInt(h).asInt()

  /** Double the table's size and re-hash everything */
  /** 将哈希表容量加倍，重新散列所有元素到新表 */
  protected def growTable(): Unit = {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  /** 计算大于等于输入n的最小2次幂 */
  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   * 获取排序后的迭代器，通过原地排序实现不需要额外内存，但会销毁原始哈希表结构
   * @param keyComparator key排序比较器
   * @return 排序后的键值对迭代器
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    // 将所有非空键值对紧凑排列到数组前端，腾出后续空间
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))

    // 对紧凑排列的键值对数组原地排序
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    // 返回排序后的迭代器，优先返回null key
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   * 判断下一次插入是否会触发扩容
   * @return true如果当前大小等于扩容阈值
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

/**
 * AppendOnlyMap伴生对象，定义最大容量常量
 */
private object AppendOnlyMap {
  /** 最大容量，为2^29 */
  val MAXIMUM_CAPACITY = (1 << 29)
}
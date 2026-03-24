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

import scala.reflect._

import com.google.common.hash.Hashing

import org.apache.spark.annotation.Private

/**
 * 文件概述：开放寻址哈希集合实现，专为只插入不删除场景优化的高性能哈希集合
 * 
 * 核心设计特点：
 * 1. 针对四种基本类型(Long/Int/Double/Float)通过Scala specialization生成优化存储，大幅降低装箱开销，性能优于标准Java HashSet
 * 2. 只支持插入操作，不支持删除，是更高级数据结构（如优化版HashMap）的基础构建块
 * 3. 使用二次探测法解决哈希冲突，哈希表大小固定为2的幂，保证可以探测到所有可用空间
 * 4. 提供回调接口和位置访问能力，方便上层数据结构扩展定制
 */
@Private
/**
 * 开放寻址哈希集合，针对只插入不删除的非空元素场景优化
 * 
 * @tparam T 集合中存储的元素类型，支持对四种基本类型的特化优化
 * @param initialCapacity 哈希表初始容量
 * @param loadFactor 负载因子，超过该比例会触发扩容重哈希
 */
class OpenHashSet[@specialized(Long, Int, Double, Float) T: ClassTag](
    initialCapacity: Int,
    loadFactor: Double)
  extends Serializable {

  require(initialCapacity <= OpenHashSet.MAX_CAPACITY,
    s"Can't make capacity bigger than ${OpenHashSet.MAX_CAPACITY} elements")
  require(initialCapacity >= 0, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import OpenHashSet._

  /**
   * 构造方法：指定初始容量，使用默认负载因子0.7
   * @param initialCapacity 哈希表初始容量
   */
  def this(initialCapacity: Int) = this(initialCapacity, 0.7)

  /**
   * 无参构造方法：使用默认初始容量64和默认负载因子0.7
   */
  def this() = this(64)

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).

  // 根据元素类型选择对应的特化哈希函数
  protected val hasher: Hasher[T] = classTag[T] match {
    case ClassTag.Long => new LongHasher().asInstanceOf[Hasher[T]]
    case ClassTag.Int => new IntHasher().asInstanceOf[Hasher[T]]
    case ClassTag.Double => new DoubleHasher().asInstanceOf[Hasher[T]]
    case ClassTag.Float => new FloatHasher().asInstanceOf[Hasher[T]]
    case _ => new Hasher[T]
  }

  protected var _capacity = nextPowerOf2(initialCapacity)
  protected var _mask = _capacity - 1
  protected var _size = 0
  protected var _growThreshold = (loadFactor * _capacity).toInt

  // 比特集，标记对应位置是否存储了元素
  protected var _bitset = new BitSet(_capacity)

  def getBitSet: BitSet = _bitset

  // Init of the array in constructor (instead of in declaration) to work around a Scala compiler
  // specialization bug that would generate two arrays (one for Object and one for specialized T).
  protected var _data: Array[T] = _
  // 分配存储元素的数组
  _data = new Array[T](_capacity)

  /** 获取集合中元素的数量 */
  def size: Int = _size

  /** 获取哈希表容量（底层数组大小） */
  def capacity: Int = _capacity

  /**
   * 判断集合是否包含指定元素
   * @param k 待查询元素
   * @return 包含返回true，否则返回false
   */
  def contains(k: T): Boolean = getPos(k) != INVALID_POS

  /**
   * 向集合添加元素，如果超出负载阈值则触发扩容重哈希
   * @param k 待添加元素
   */
  def add(k: T): Unit = {
    addWithoutResize(k)
    rehashIfNeeded(k, grow, move)
  }

  /**
   * 合并另一个OpenHashSet的所有元素到当前集合
   * @param other 待合并的哈希集合
   * @return 当前集合本身
   */
  def union(other: OpenHashSet[T]): OpenHashSet[T] = {
    val iterator = other.iterator
    while (iterator.hasNext) {
      add(iterator.next())
    }
    this
  }

  /**
   * 使用对象等价性判断指定位置是否存储目标key，避免==和equals结果不一致的问题
   * 例如处理0.0/-0.0和NaN的特殊情况，解决SPARK-45599问题
   * 
   * @param k 目标key
   * @param pos 待检查位置
   * @return 指定位置存储的元素等于k返回true，否则返回false
   */
  @annotation.nowarn("cat=other-non-cooperative-equals")
  private def keyExistsAtPos(k: T, pos: Int) =
    _data(pos) equals k

  /**
   * 添加元素不触发扩容，由调用方负责调用rehashIfNeeded检查扩容
   * 
   * 返回值说明：使用(retval & POSITION_MASK)获取实际位置，
   * 如果(retval & NONEXISTENCE_MASK) == 0说明元素之前已存在，否则是新增元素
   *
   * @param k 待添加元素
   * @return 元素存储位置，如果元素之前不存在则设置最高位为1
   */
  def addWithoutResize(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        // 新增元素，写入数据并标记比特位
        _data(pos) = k
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (keyExistsAtPos(k, pos)) {
        // 元素已存在，直接返回位置
        return pos
      } else {
        // 二次探测，步长逐次加1
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /**
   * 如果超出负载阈值则触发重哈希扩容
   * @param k 未实际使用，仅用于强制Scala编译器对方法特化
   * @param allocateFunc 分配新数组时的回调函数
   * @param moveFunc 元素从旧位置移动到新位置时的回调函数
   */
  def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit): Unit = {
    if (_size > _growThreshold) {
      rehash(k, allocateFunc, moveFunc)
    }
  }

  /**
   * 获取元素在底层数组中的位置，不存在则返回INVALID_POS
   * @param k 待查询元素
   * @return 元素位置，不存在返回-1
   */
  def getPos(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        return INVALID_POS
      } else if (keyExistsAtPos(k, pos)) {
        return pos
      } else {
        // 二次探测，步长逐次加1
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /**
   * 获取指定位置存储的元素，不检查位置是否合法
   * @param pos 数组下标
   * @return 对应位置存储的元素
   */
  def getValue(pos: Int): T = _data(pos)

  /**
   * 获取集合迭代器，遍历所有存储的元素
   * @return 元素迭代器
   */
  def iterator: Iterator[T] = new Iterator[T] {
    var pos = nextPos(0)
    override def hasNext: Boolean = pos != INVALID_POS
    override def next(): T = {
      val tmp = getValue(pos)
      pos = nextPos(pos + 1)
      tmp
    }
  }

  /**
   * 安全获取指定位置存储的元素，会先断言位置已被占用
   * @param pos 数组下标
   * @return 对应位置存储的元素
   */
  def getValueSafe(pos: Int): T = {
    assert(_bitset.get(pos))
    _data(pos)
  }

  /**
   * 从指定位置（包含）开始查找下一个存储了元素的位置
   * @param fromPos 起始查找位置
   * @return 下一个存储元素的位置，找不到返回INVALID_POS
   */
  def nextPos(fromPos: Int): Int = _bitset.nextSetBit(fromPos)

  /**
   * 将哈希表容量翻倍，重新哈希所有元素
   * 参数k未实际使用，仅用于强制Scala编译器对方法特化
   * 
   * @param k 未实际使用，仅用于触发特化
   * @param allocateFunc 分配新数组时的回调函数
   * @param moveFunc 元素从旧位置移动到新位置时的回调函数
   */
  private def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit): Unit = {
    val newCapacity = _capacity * 2
    require(newCapacity > 0 && newCapacity <= OpenHashSet.MAX_CAPACITY,
      s"Can't contain more than ${(loadFactor * OpenHashSet.MAX_CAPACITY).toInt} elements")
    allocateFunc(newCapacity)
    val newBitset = new BitSet(newCapacity)
    val newData = new Array[T](newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      if (_bitset.get(oldPos)) {
        val key = _data(oldPos)
        var newPos = hashcode(hasher.hash(key)) & newMask
        var i = 1
        var keepGoing = true
        // 插入过程不需要检查相等性，比addWithoutResize少一个分支判断
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // 插入到新位置
            newData(newPos) = key
            newBitset.set(newPos)
            moveFunc(oldPos, newPos)
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

    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }

  /**
   * 二次哈希，优化原始哈希低位分散不足的问题，使用Murmur3_32哈希
   * @param h 原始哈希值
   * @return 重新计算后的哈希值
   */
  private def hashcode(h: Int): Int = Hashing.murmur3_32_fixed().hashInt(h).asInt()

  /**
   * 计算大于等于输入n的最小2的幂
   * @param n 输入数值
   * @return 大于等于n的最小2的幂，输入为0返回1
   */
  private def nextPowerOf2(n: Int): Int = {
    if (n == 0) {
      1
    } else {
      val highBit = Integer.highestOneBit(n)
      if (highBit == n) n else highBit << 1
    }
  }
}


/**
 * OpenHashSet的伴生对象，定义常量、特化哈希器实现和默认回调函数
 */
private[spark]
object OpenHashSet {

  val MAX_CAPACITY = 1 << 30
  val INVALID_POS = -1
  val NONEXISTENCE_MASK = 1 << 31
  val POSITION_MASK = (1 << 31) - 1

  /**
   * 特化哈希函数抽象，避免特化实现中哈希计算的装箱开销
   * 针对四种基本类型分别提供实现
   */
  sealed class Hasher[@specialized(Long, Int, Double, Float) T] extends Serializable {
    def hash(o: T): Int = o.hashCode()
  }

  /** Long类型特化哈希函数实现 */
  class LongHasher extends Hasher[Long] {
    override def hash(o: Long): Int = (o ^ (o >>> 32)).toInt
  }

  /** Int类型特化哈希函数实现 */
  class IntHasher extends Hasher[Int] {
    override def hash(o: Int): Int = o
  }

  /** Double类型特化哈希函数实现 */
  class DoubleHasher extends Hasher[Double] {
    override def hash(o: Double): Int = {
      val bits = java.lang.Double.doubleToLongBits(o)
      (bits ^ (bits >>> 32)).toInt
    }
  }

  /** Float类型特化哈希函数实现 */
  class FloatHasher extends Hasher[Float] {
    override def hash(o: Float): Int = java.lang.Float.floatToIntBits(o)
  }

  private def grow1(newSize: Int): Unit = {}
  private def move1(oldPos: Int, newPos: Int): Unit = { }

  private val grow = grow1 _
  private val move = move1 _
}
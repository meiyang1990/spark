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

import java.util.Arrays

/**
 * 简单的固定大小BitSet实现，通过跳过安全边界检查提升性能，Spark内部用于高效位操作存储。
 * 
 * 核心职责：提供高效的位存储与位运算操作，常用于Spark中分区标记、存在性判断、布隆过滤等场景。
 * @param numBits 需要存储的总位数
 */
class BitSet(numBits: Int) extends Serializable {

  private val words = new Array[Long](bit2words(numBits))
  private val numWords = words.length

  /**
   * 获取当前BitSet可存储的最大位数（容量）。
   * @return 可容纳的总位数
   */
  def capacity: Int = numWords * 64

  /**
   * 清空BitSet，将所有位设置为0。
   */
  def clear(): Unit = Arrays.fill(words, 0)

  /**
   * 将指定索引之前（不包含该索引）的所有位都设置为1。
   * @param bitIndex 截止位索引
   */
  def setUntil(bitIndex: Int): Unit = {
    // 计算目标位所在的Long数组索引（每个Long占64位）
    val wordIndex = bitIndex >> 6 // divide by 64
    // 将前面完整的Long块全部填充为-1（二进制全1）
    Arrays.fill(words, 0, wordIndex, -1)
    if (wordIndex < words.length) {
      // 生成当前Long块中需要置1的位掩码
      val mask = ~(-1L << (bitIndex & 0x3f))
      words(wordIndex) |= mask
    }
  }

  /**
   * 将指定索引之前（不包含该索引）的所有位都清除为0。
   * @param bitIndex 截止位索引
   */
  def clearUntil(bitIndex: Int): Unit = {
    val wordIndex = bitIndex >> 6 // divide by 64
    // 将前面完整的Long块全部填充为0
    Arrays.fill(words, 0, wordIndex, 0)
    if (wordIndex < words.length) {
      // 生成当前Long块中需要保持为1的位掩码
      val mask = -1L << (bitIndex & 0x3f)
      words(wordIndex) &= mask
    }
  }

  /**
   * 按位与操作，返回两个BitSet按位与后的新BitSet。
   * @param other 另一个参与运算的BitSet
   * @return 按位与结果，容量取两个BitSet中的较大值
   */
  def &(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
   * 按位或操作，返回两个BitSet按位或后的新BitSet。
   * @param other 另一个参与运算的BitSet
   * @return 按位或结果，容量取两个BitSet中的较大值
   */
  def |(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    assert(newBS.numWords >= numWords)
    assert(newBS.numWords >= other.numWords)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) | other.words(ind)
      ind += 1
    }
    // 复制剩余位（当前BitSet多出来的部分）
    while (ind < numWords) {
      newBS.words(ind) = words(ind)
      ind += 1
    }
    // 复制剩余位（另一个BitSet多出来的部分）
    while (ind < other.numWords) {
      newBS.words(ind) = other.words(ind)
      ind += 1
    }
    newBS
  }

  /**
   * 按位异或操作，返回两个BitSet对称差后的新BitSet。
   * 对称差即仅在其中一个BitSet中为1的位置结果为1。
   * @param other 另一个参与运算的BitSet
   * @return 按位异或结果，容量取两个BitSet中的较大值
   */
  def ^(other: BitSet): BitSet = {
    val newBS = new BitSet(math.max(capacity, other.capacity))
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) ^ other.words(ind)
      ind += 1
    }
    // 复制剩余位（当前BitSet多出来的部分）
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    // 复制剩余位（另一个BitSet多出来的部分）
    if (ind < other.numWords) {
      Array.copy( other.words, ind, newBS.words, ind, other.numWords - ind )
    }
    newBS
  }

  /**
   * 按位与非操作，返回当前BitSet减去另一个BitSet后的差集。
   * 结果中保留仅在当前BitSet中为1的位。
   * @param other 要减去的BitSet
   * @return 差集结果，容量与当前BitSet相同
   */
  def andNot(other: BitSet): BitSet = {
    val newBS = new BitSet(capacity)
    val smaller = math.min(numWords, other.numWords)
    var ind = 0
    while (ind < smaller) {
      newBS.words(ind) = words(ind) & ~other.words(ind)
      ind += 1
    }
    // 复制剩余位（当前BitSet多出来的部分）
    if (ind < numWords) {
      Array.copy( words, ind, newBS.words, ind, numWords - ind )
    }
    newBS
  }

  /**
   * 将指定索引位置的位设置为1。
   * @param index 要设置的位索引
   */
  def set(index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f)  // 对64取模得到在当前Long中的偏移，生成位掩码
    words(index >> 6) |= bitmask        // 定位到对应Long，按位或设置为1
  }

  /**
   * 将指定索引位置的位清除为0。
   * @param index 要清除的位索引
   */
  def unset(index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f)  // 对64取模得到在当前Long中的偏移，生成位掩码
    words(index >> 6) &= ~bitmask        // 定位到对应Long，按位与清除为0
  }

  /**
   * 获取指定索引位置位的值。
   * @param index 位索引
   * @return true表示该位为1，false表示该位为0
   */
  def get(index: Int): Boolean = {
    val bitmask = 1L << (index & 0x3f)   // 对64取模得到在当前Long中的偏移，生成位掩码
    (words(index >> 6) & bitmask) != 0  // 按位与判断是否非零
  }

  /**
   * 获取所有已设置为1的位索引的迭代器。
   * @return 已设置位索引迭代器
   */
  def iterator: Iterator[Int] = new Iterator[Int] {
    var ind = nextSetBit(0)
    override def hasNext: Boolean = ind >= 0
    override def next(): Int = {
      val tmp = ind
      ind = nextSetBit(ind + 1)
      tmp
    }
  }


  /**
   * 统计当前BitSet中值为1的位总数。
   * @return 被设置为1的位数
   */
  def cardinality(): Int = {
    var sum = 0
    var i = 0
    while (i < numWords) {
      // 使用Long.bitCount统计每个Long中1的个数
      sum += java.lang.Long.bitCount(words(i))
      i += 1
    }
    sum
  }

  /**
   * 从指定起始索引开始（包含）查找下一个值为1的位索引。
   * @param fromIndex 起始搜索索引（包含）
   * @return 下一个值为1的位索引，若不存在则返回-1
   */
  def nextSetBit(fromIndex: Int): Int = {
    var wordIndex = fromIndex >> 6
    if (wordIndex >= numWords) {
      // 已经超出数组范围，不存在下一个置点位
      return -1
    }

    // 先尝试在当前Long块中查找下一个置点位
    val subIndex = fromIndex & 0x3f
    var word = words(wordIndex) >> subIndex
    if (word != 0) {
      return (wordIndex << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
    }

    // 在后续的Long块中继续查找
    wordIndex += 1
    while (wordIndex < numWords) {
      word = words(wordIndex)
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
      }
      wordIndex += 1
    }

    // 未找到任何置点位
    -1
  }

  /**
   * 与另一个BitSet按位求并，并将结果覆盖到当前BitSet，修改当前BitSet。
   * 要求当前BitSet长度不大于另一个BitSet。
   * @param other 参与并运算的另一个BitSet
   */
  def union(other: BitSet): Unit = {
    require(this.numWords <= other.numWords)
    var ind = 0
    while (ind < this.numWords) {
      this.words(ind) = this.words(ind) | other.words(ind)
      ind += 1
    }
  }

  /**
   * 判断当前BitSet与另一个BitSet是否存在交集。
   * 要求两个BitSet长度相同。
   * @param other 另一个待比较BitSet
   * @return true表示存在公共的置点位，false表示无交集
   */
  def intersects(other: BitSet): Boolean = {
    assert(numWords == other.numWords)
    var ind = 0
    while (ind < numWords) {
      if ((words(ind) & other.words(ind)) != 0) {
        // 找到任意公共置点位，直接返回true
        return true
      }
      ind += 1
    }
    // 遍历完所有位未找到交集
    false
  }

  /**
   * 计算存储指定位数需要多少个Long（每个Long占64位）。
   * @param numBits 需要存储的总位数
   * @return 需要的Long数组长度
   */
  private def bit2words(numBits: Int) = ((numBits - 1) >> 6) + 1

  override def equals(other: Any): Boolean = other match {
    case otherSet: BitSet => Arrays.equals(words, otherSet.words)
    case _ => false
  }

  override def hashCode(): Int = {
    Arrays.hashCode(words)
  }
}
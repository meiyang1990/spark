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

/**
 * 不可变位集合操作不支持错误信息常量
 */
private object ErrorMessage {
  final val msg: String = "mutable operation is not supported"
}

/**
 * 不可变位集合，在构造阶段完成所有位的初始化，不支持后续修改操作
 * 用于Spark内部需要只读位集合的场景，避免误修改引发并发问题
 * @param numBits 位集合总大小，即可存储的位数
 * @param bitsToSet 需要在构造时置位的索引列表
 */
// An immutable BitSet that initializes set bits in its constructor.
class ImmutableBitSet(val numBits: Int, val bitsToSet: Int*) extends BitSet(numBits) {

  // Initialize the set bits.
  {
    val bitsIterator = bitsToSet.iterator
    // 遍历所有需要置位的索引，调用父类方法完成初始化
    while (bitsIterator.hasNext) {
      super.set(bitsIterator.next())
    }
  }

  /**
   * 清空整个位集合，不支持修改操作，抛出异常
   */
  override def clear(): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  /**
   * 清空指定索引之前的所有位，不支持修改操作，抛出异常
   */
  override def clearUntil(bitIndex: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  /**
   * 将指定索引位置位，不支持修改操作，抛出异常
   */
  override def set(index: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  /**
   * 将指定索引之前的所有位置位，不支持修改操作，抛出异常
   */
  override def setUntil(bitIndex: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  /**
   * 将指定索引位清零，不支持修改操作，抛出异常
   */
  override def unset(index: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  /**
   * 与另一个位集合求并集，不支持修改操作，抛出异常
   */
  override def union(other: BitSet): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }
}
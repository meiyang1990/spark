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

package org.apache.spark.util.random

import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}

import scala.util.hashing.MurmurHash3

/**
 * 实现XORShift伪随机数生成算法的随机数生成器
 * 基于Marsaglia 2003年提出的Xorshift算法实现，性能约为JDK自带java.util.Random的3.5倍
 * 不保证线程安全，Spark中每个线程独立使用本类实例，因此不需要线程安全
 * 相比JDK实现使用普通Long存储种子，避免了原子变量的同步开销
 * 
 * @param init 随机数种子初始值
 */
private[spark] class XORShiftRandom(init: Long) extends JavaRandom(init) {

  /** 使用当前系统纳秒时间作为默认种子创建随机数生成器 */
  def this() = this(System.nanoTime)

  // 经过哈希散列后的当前随机数种子
  private var seed = XORShiftRandom.hashSeed(init)

  // 只需要重写next方法，所有高层随机数方法(nextInt, nextDouble, nextGaussian等)都会调用该方法
  override protected def next(bits: Int): Int = {
    // 三轮XOR位移变换生成下一个种子
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    // 提取指定位数的结果返回
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  /**
   * 重置随机数种子
   * @param s 新的种子初始值
   */
  override def setSeed(s: Long): Unit = {
    seed = XORShiftRandom.hashSeed(s)
  }
}

/**
 * XORShiftRandom的伴生对象，提供种子哈希散列工具方法
 */
private[spark] object XORShiftRandom {

  /**
   * 对初始种子进行哈希散列，使得0/1比特均匀分布，避免种子质量不佳导致随机序列退化
   * @param seed 原始输入种子
   * @return 散列后的64位种子
   */
  private[random] def hashSeed(seed: Long): Long = {
    // 将64位种子转换为字节数组
    val bytes = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(seed).array()
    // 第一次MurmurHash得到低32位
    val lowBits = MurmurHash3.bytesHash(bytes, MurmurHash3.arraySeed)
    // 用低32位作为种子再次哈希得到高32位
    val highBits = MurmurHash3.bytesHash(bytes, lowBits)
    // 拼接高低32位得到最终64位种子
    (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
  }
}
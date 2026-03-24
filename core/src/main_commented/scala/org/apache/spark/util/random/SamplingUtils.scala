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

import scala.reflect.ClassTag
import scala.util.Random

/**
 * 随机采样工具类，提供蓄水池采样和采样率计算等辅助方法，用于Spark分布式数据采样场景
 */
private[spark] object SamplingUtils {

  /**
   * 实现蓄水池采样算法，同时返回采样结果和输入数据总数量
   *
   * @param input 输入数据迭代器
   * @param k 蓄水池大小，即需要采样的样本数量
   * @param seed 随机种子，默认使用随机生成的长整数
   * @return (采样结果数组, 输入数据总数量)
   */
  def reservoirSampleAndCount[T: ClassTag](
      input: Iterator[T],
      k: Int,
      seed: Long = Random.nextLong())
    : (Array[T], Long) = {
    val reservoir = new Array[T](k)
    // Put the first k elements in the reservoir.
    var i = 0
    while (i < k && input.hasNext) {
      val item = input.next()
      reservoir(i) = item
      i += 1
    }

    // If we have consumed all the elements, return them. Otherwise do the replacement.
    if (i < k) {
      // If input size < k, trim the array to return only an array of input size.
      val trimReservoir = new Array[T](i)
      System.arraycopy(reservoir, 0, trimReservoir, 0, i)
      (trimReservoir, i)
    } else {
      // If input size > k, continue the sampling process.
      var l = i.toLong
      val rand = new XORShiftRandom(seed)
      while (input.hasNext) {
        val item = input.next()
        l += 1
        // There are k elements in the reservoir, and the l-th element has been
        // consumed. It should be chosen with probability k/l. The expression
        // below is a random long chosen uniformly from [0,l)
        val replacementIndex = (rand.nextDouble() * l).toLong
        if (replacementIndex < k) {
          reservoir(replacementIndex.toInt) = item
        }
      }
      (reservoir, l)
    }
  }

  /**
   * 计算满足99.99%概率达到最小样本量要求的采样率，用于近似采样场景
   *
   * @param sampleSizeLowerBound 要求的最小样本量下限
   * @param total RDD总数据量大小
   * @param withReplacement 是否是有放回采样
   * @return 满足置信度要求的采样率
   */
  def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long,
      withReplacement: Boolean): Double = {
    if (withReplacement) {
      PoissonBounds.getUpperBound(sampleSizeLowerBound) / total
    } else {
      val fraction = sampleSizeLowerBound.toDouble / total
      BinomialBounds.getUpperBound(1e-4, total, fraction)
    }
  }
}

/**
 * 泊松分布边界计算工具类，用于有放回采样场景下，高置信度保证样本量的采样率计算
 */
private[spark] object PoissonBounds {

  /**
   * 计算泊松分布的下界，保证Pr[X > s]概率极小
   * @param s 目标样本量
   * @return 满足要求的泊松参数下界
   */
  def getLowerBound(s: Double): Double = {
    math.max(s - numStd(s) * math.sqrt(s), 1e-15)
  }

  /**
   * 计算泊松分布的上界，保证Pr[X < s]概率极小
   * @param s 目标样本量
   * @return 满足要求的泊松参数上界
   */
  def getUpperBound(s: Double): Double = {
    math.max(s + numStd(s) * math.sqrt(s), 1e-10)
  }

  /**
   * 根据样本量大小获取经验标准差倍数，用于保证99.99%置信度
   * @param s 目标样本量
   * @return 标准差倍数
   */
  private def numStd(s: Double): Double = {
    // TODO: Make it tighter.
    if (s < 6.0) {
      12.0
    } else if (s < 16.0) {
      9.0
    } else {
      6.0
    }
  }
}

/**
 * 二项分布边界计算工具类，用于无放回采样场景下，高置信度保证样本量的采样率计算
 */
private[spark] object BinomialBounds {

  // 最小支持采样率，避免随机数分辨率问题
  val minSamplingRate = 1e-10

  /**
   * 计算二项分布下界，保证成功次数极少超过期望的概率极小
   * @param delta 允许的失败概率
   * @param n 总试验次数（总数据量）
   * @param fraction 期望采样比例
   * @return 满足要求的采样率下界
   */
  def getLowerBound(delta: Double, n: Long, fraction: Double): Double = {
    val gamma = - math.log(delta) / n * (2.0 / 3.0)
    fraction + gamma - math.sqrt(gamma * gamma + 3 * gamma * fraction)
  }

  /**
   * 计算二项分布上界，保证成功次数极少低于期望的概率极小
   * @param delta 允许的失败概率
   * @param n 总试验次数（总数据量）
   * @param fraction 期望采样比例
   * @return 满足要求的采样率上界
   */
  def getUpperBound(delta: Double, n: Long, fraction: Double): Double = {
    val gamma = - math.log(delta) / n
    math.min(1,
      math.max(minSamplingRate, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
  }
}
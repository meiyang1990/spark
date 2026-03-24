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

import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
 * 分层采样工具类，为PairRDDFunctions的按Key采样方法提供辅助功能和数据结构
 *
 * 当需要保证精确采样数量时，本工具会额外扫描一次RDD，为每个分层计算精确阈值，
 * 通过维护大小为O(log(s))（s为目标采样数）的等待列表，高概率保证最终样本量符合预期。
 *
 * 算法原理：和简单随机采样一样，为每个条目生成[0.0,1.0]均匀分布随机值，所有小于
 * 等于等待列表最小值的条目被直接接受，直接接受阈值设计为满足s - 已接受数 = O(sqrt(s))，
 * 因此只需维护大小为O(sqrt(s))的等待列表，最后从等待列表中选取部分条目补充到样本即可得到
 * 精确大小s的样本，精确阈值通过对等待列表排序后选取第(s - 已接受数)个位置的值得到。
 *
 * 注意：由于计算阈值和实际采样使用相同种子，保证了计算出的阈值一定能得到目标样本量。
 * 更多算法理论背景请参考http://jmlr.org/proceedings/papers/v28/meng13a.html
 */

private[spark] object StratifiedSamplingUtils extends Logging {

  /**
   * 统计每个分层直接接受的条目数，并生成每个分层的等待列表
   * 仅在需要精确样本量时调用该方法
   *
   * @param rdd 输入键值对RDD
   * @param withReplacement 是否有放回采样
   * @param fractions 每个Key对应的采样比例
   * @param counts 每个Key的总计数（有放回采样时需要）
   * @param seed 随机种子
   * @return 每个Key对应的接受结果统计
   */
  def getAcceptanceResults[K, V](rdd: RDD[(K, V)],
      withReplacement: Boolean,
      fractions: Map[K, Double],
      counts: Option[Map[K, Long]],
      seed: Long): mutable.Map[K, AcceptanceResult] = {
    val combOp = getCombOp[K]
    val mappedPartitionRDD = rdd.mapPartitionsWithIndex { case (partition, iter) =>
      val zeroU: mutable.Map[K, AcceptanceResult] = new mutable.HashMap[K, AcceptanceResult]()
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + partition)
      val seqOp = getSeqOp(withReplacement, fractions, rng, counts)
      Iterator(iter.foldLeft(zeroU)(seqOp))
    }
    mappedPartitionRDD.reduce(combOp)
  }

  /**
   * 返回聚合时用于每个分区收集采样统计结果的函数
   *
   * @param withReplacement 是否有放回采样
   * @param fractions 每个Key对应的采样比例
   * @param rng 随机数生成器
   * @param counts 每个Key的总计数
   * @return 分区聚合函数
   */
  def getSeqOp[K, V](withReplacement: Boolean,
      fractions: Map[K, Double],
      rng: RandomDataGenerator,
      counts: Option[Map[K, Long]]):
    (mutable.Map[K, AcceptanceResult], (K, V)) => mutable.Map[K, AcceptanceResult] = {
    val delta = 5e-5
    (result: mutable.Map[K, AcceptanceResult], item: (K, V)) => {
      val key = item._1
      val fraction = fractions(key)
      if (!result.contains(key)) {
        result += (key -> new AcceptanceResult())
      }
      val acceptResult = result(key)

      if (withReplacement) {
        // 仅在第一次计算接受和等待列表边界，边界不随迭代变化
        // TODO 修改为流式版本
        if (acceptResult.areBoundsEmpty) {
          val n = counts.get(key)
          val sampleSize = math.ceil(n * fraction).toLong
          val lmbd1 = PoissonBounds.getLowerBound(sampleSize.toDouble)
          val lmbd2 = PoissonBounds.getUpperBound(sampleSize.toDouble)
          acceptResult.acceptBound = lmbd1 / n
          acceptResult.waitListBound = (lmbd2 - lmbd1) / n
        }
        val acceptBound = acceptResult.acceptBound
        val copiesAccepted = if (acceptBound == 0.0) 0L else rng.nextPoisson(acceptBound)
        if (copiesAccepted > 0) {
          acceptResult.numAccepted += copiesAccepted
        }
        val copiesWaitlisted = rng.nextPoisson(acceptResult.waitListBound)
        if (copiesWaitlisted > 0) {
          acceptResult.waitList ++= ArrayBuffer.fill(copiesWaitlisted)(rng.nextUniform())
        }
      } else {
        // 无放回采样使用流式算法，避免额外扫描RDD获取计数
        // 因此接受边界和等待列表边界每次迭代都会变化
        acceptResult.acceptBound =
          BinomialBounds.getLowerBound(delta, acceptResult.numItems, fraction)
        acceptResult.waitListBound =
          BinomialBounds.getUpperBound(delta, acceptResult.numItems, fraction)

        val x = rng.nextUniform()
        if (x < acceptResult.acceptBound) {
          acceptResult.numAccepted += 1
        } else if (x < acceptResult.waitListBound) {
          acceptResult.waitList += x
        }
      }
      acceptResult.numItems += 1
      result
    }
  }

  /**
   * 返回合并不同分区seqOp结果的合并函数
   *
   * @return 分区结果合并函数
   */
  def getCombOp[K]: (mutable.Map[K, AcceptanceResult], mutable.Map[K, AcceptanceResult])
    => mutable.Map[K, AcceptanceResult] = {
    (result1: mutable.Map[K, AcceptanceResult], result2: mutable.Map[K, AcceptanceResult]) => {
      // 取两个分区Key集合的并集，应对单个分区不包含所有Key的情况
      result1.keySet.union(result2.keySet).foreach { key =>
        // 使用result2保存合并结果，因为result1通常为空
        val entry1 = result1.get(key)
        if (result2.contains(key)) {
          result2(key).merge(entry1)
        } else {
          if (entry1.isDefined) {
            result2 += (key -> entry1.get)
          }
        }
      }
      result2
    }
  }

  /**
   * 根据getAcceptanceResults返回的结果，计算每个分层接受条目的阈值，以生成精确大小样本
   *
   * 对每个分层计算目标样本量=ceil(总条目数 * 采样比例)，对比直接接受数量和等待列表大小，
   * 大多数情况下直接接受数 ≤ 目标样本量 ≤ 直接接受数+等待列表大小，需要对等待列表排序找到阈值T
   * 使得随机值≤T的条目总数等于目标样本量，由于等待列表中所有值都大于直接接受阈值，第一次直接接受
   * 的所有条目都会保留在最终样本中。
   *
   * @param finalResult 合并后的各分层接受结果
   * @param fractions 每个Key对应的采样比例
   * @return 每个Key对应的最终接受阈值
   */
  def computeThresholdByKey[K](finalResult: Map[K, AcceptanceResult],
      fractions: Map[K, Double]): Map[K, Double] = {
    val thresholdByKey = new mutable.HashMap[K, Double]()
    for ((key, acceptResult) <- finalResult) {
      val sampleSize = math.ceil(acceptResult.numItems * fractions(key)).toLong
      if (acceptResult.numAccepted > sampleSize) {
        logWarning("Pre-accepted too many")
        thresholdByKey += (key -> acceptResult.acceptBound)
      } else {
        val numWaitListAccepted = (sampleSize - acceptResult.numAccepted).toInt
        if (numWaitListAccepted >= acceptResult.waitList.size) {
          logWarning("WaitList too short")
          thresholdByKey += (key -> acceptResult.waitListBound)
        } else {
          thresholdByKey += (key -> acceptResult.waitList.sorted.apply(numWaitListAccepted))
        }
      }
    }
    thresholdByKey
  }

  /**
   * 返回无放回分层采样时每个分区使用的采样函数
   * 当需要精确样本量时，会额外扫描一次RDD确定精确采样阈值，高概率保证样本量符合要求
   * 每个分区使用唯一随机种子
   *
   * @param rdd 输入键值对RDD
   * @param fractions 每个Key对应的采样比例
   * @param exact 是否需要精确样本量
   * @param seed 随机种子
   * @return 分区采样函数，输入分区索引和迭代器，输出过滤后的样本迭代器
   */
  def getBernoulliSamplingFunction[K, V](rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    var samplingRateByKey = fractions
    if (exact) {
      // 计算每个分层阈值后重新采样
      val finalResult = getAcceptanceResults(rdd, false, fractions, None, seed)
      samplingRateByKey = computeThresholdByKey(finalResult, fractions)
    }
    (idx: Int, iter: Iterator[(K, V)]) => {
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + idx)
      // 必须和getSeqOp中无放回场景使用相同的随机数调用顺序，保证生成相同随机序列
      iter.filter(t => rng.nextUniform() < samplingRateByKey(t._1))
    }
  }

  /**
   * 返回有放回分层采样时每个分区使用的采样函数
   * 当需要精确样本量时，会额外扫描两次RDD：第一次统计每个分层（同Key分组）的条目数，
   * 第二次使用计数计算精确采样阈值，高概率保证样本量符合要求
   * 每个分区使用唯一随机种子
   *
   * @param rdd 输入键值对RDD
   * @param fractions 每个Key对应的采样比例
   * @param exact 是否需要精确样本量
   * @param seed 随机种子
   * @return 分区采样函数，输入分区索引和迭代器，输出过滤后的样本迭代器
   */
  def getPoissonSamplingFunction[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    // TODO 实现不需要预计数的流式有放回采样
    if (exact) {
      val counts = Some(rdd.countByKey())
      val finalResult = getAcceptanceResults(rdd, true, fractions, counts, seed)
      val thresholdByKey = computeThresholdByKey(finalResult, fractions)
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val key = item._1
          val acceptBound = finalResult(key).acceptBound
          // 必须和getSeqOp中有放回场景使用相同的随机数调用顺序，保证生成相同随机序列
          val copiesAccepted = if (acceptBound == 0) 0L else rng.nextPoisson(acceptBound)
          val copiesWaitlisted = rng.nextPoisson(finalResult(key).waitListBound)
          val copiesInSample = copiesAccepted +
            (0 until copiesWaitlisted).count(i => rng.nextUniform() < thresholdByKey(key))
          if (copiesInSample > 0) {
            Iterator.fill(copiesInSample.toInt)(item)
          } else {
            Iterator.empty
          }
        }
      }
    } else {
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val count = rng.nextPoisson(fractions(item._1))
          if (count == 0) {
            Iterator.empty
          } else {
            Iterator.fill(count)(item)
          }
        }
      }
    }
  }

  /**
   * 同时生成均匀分布随机值和泊松分布随机值的随机数生成器
   */
  private class RandomDataGenerator {
    val uniform = new XORShiftRandom()
    // commons-math3不支持直接基于任意均值生成泊松样本，因此缓存不同均值对应的泊松分布对象
    val poissonCache = mutable.Map[Double, PoissonDistribution]()
    var poissonSeed = 0L

    def reSeed(seed: Long): Unit = {
      uniform.setSeed(seed)
      poissonSeed = seed
      poissonCache.clear()
    }

    def nextPoisson(mean: Double): Int = {
      val poisson = poissonCache.getOrElseUpdate(mean, {
        val newPoisson = new PoissonDistribution(mean)
        newPoisson.reseedRandomGenerator(poissonSeed)
        newPoisson
      })
      poisson.sample()
    }

    def nextUniform(): Double = {
      uniform.nextDouble()
    }
  }
}

/**
 * 存储每个分层的接受结果统计，保存直接接受数、等待列表、接受和等待边界
 * 用于seqOp聚合每个分区的采样统计结果
 *
 * @param numItems 当前分层总条目数
 * @param numAccepted 直接接受的条目数
 */
private[random] class AcceptanceResult(var numItems: Long = 0L, var numAccepted: Long = 0L)
  extends Serializable {

  val waitList = new ArrayBuffer[Double]
  var acceptBound: Double = Double.NaN // 直接接受条目的上限阈值
  var waitListBound: Double = Double.NaN // 加入等待列表的上限阈值

  def areBoundsEmpty: Boolean = acceptBound.isNaN || waitListBound.isNaN

  def merge(other: Option[AcceptanceResult]): Unit = {
    if (other.isDefined) {
      waitList ++= other.get.waitList
      numAccepted += other.get.numAccepted
      numItems += other.get.numItems
    }
  }
}
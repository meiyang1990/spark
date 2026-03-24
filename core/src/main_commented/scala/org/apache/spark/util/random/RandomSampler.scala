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

import java.util.Random

import scala.reflect.ClassTag

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.annotation.DeveloperApi

/**
 * 文件说明: 提供多种随机采样算法的实现，包括伯努利采样、泊松带放回采样、间隙采样优化等，支撑Spark分布式数据采样功能
 */

/**
 * :: DeveloperApi ::
 * 伪随机采样器抽象接口，定义通用采样能力，支持不同采样类型转换，仅可在采样前应用转换操作
 * 
 * @tparam T 输入元素类型
 * @tparam U 输出采样元素类型
 */
@DeveloperApi
trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {

  /**
   * 对输入迭代器执行随机采样
   * @param items 输入元素迭代器
   * @return 采样后元素迭代器
   */
  def sample(items: Iterator[T]): Iterator[U] =
    items.filter(_ => sample() > 0).asInstanceOf[Iterator[U]]

  /**
   * 判断下一个元素是否被采样，返回采样次数，0表示未被采样
   * @return 元素被采样的次数，0表示不采样
   */
  def sample(): Int

  /**
   * 克隆当前随机采样器对象
   * @return 克隆后的采样器对象
   */
  override def clone: RandomSampler[T, U] =
    throw new UnsupportedOperationException("clone() is not implemented.")
}

/**
 * 随机采样器工具对象，提供默认随机数生成器和全局配置参数
 */
private[spark]
object RandomSampler {
  /**
   * 创建默认的随机数生成器
   * @return XORShiftRandom随机数实例
   */
  def newDefaultRNG: Random = new XORShiftRandom

  /**
   * 间隙采样优化的最大采样比例阈值：当采样比例小于等于该值时使用间隙采样优化，否则使用传统伯努利采样
   * 最优值依赖随机数生成器成本，成本越高最优值越大，默认初始值0.5
   */
  val defaultMaxGapSamplingFraction = 0.4

  /**
   * 随机数生成的最小epsilon值，用于避免对0取对数的计算错误，取值接近RNG返回的最小正浮点数
   */
  val rngEpsilon = 5e-11

  /**
   * 浮点计算舍入误差容限，用于校验采样比例参数，避免浮点计算抖动导致的错误告警
   */
  val roundingEpsilon = 1e-6
}

/**
 * :: DeveloperApi ::
 * 基于伯努利试验的分区间采样器，用于根据随机数范围切分数据集
 * 
 * @param lb 接受区间的下界
 * @param ub 接受区间的上界
 * @param complement 是否使用区间的补集，默认为false
 * @tparam T 元素类型
 */
@DeveloperApi
class BernoulliCellSampler[T](lb: Double, ub: Double, complement: Boolean = false)
  extends RandomSampler[T, T] {

  /** 校验参数边界，考虑浮点误差容限 */
  require(
    lb <= (ub + RandomSampler.roundingEpsilon),
    s"Lower bound ($lb) must be <= upper bound ($ub)")
  require(
    lb >= (0.0 - RandomSampler.roundingEpsilon),
    s"Lower bound ($lb) must be >= 0.0")
  require(
    ub <= (1.0 + RandomSampler.roundingEpsilon),
    s"Upper bound ($ub) must be <= 1.0")

  // 内部随机数生成器
  private val rng: Random = new XORShiftRandom

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  override def sample(): Int = {
    if (ub - lb <= 0.0) {
      // 区间为空，根据是否取补集返回结果
      if (complement) 1 else 0
    } else {
      val x = rng.nextDouble()
      val n = if ((x >= lb) && (x < ub)) 1 else 0
      // 根据是否取补集返回最终结果
      if (complement) 1 - n else n
    }
  }

  /**
   * 返回当前采样器的补集采样器，对原区间取反
   * @return 补集伯努利单元采样器
   */
  def cloneComplement(): BernoulliCellSampler[T] =
    new BernoulliCellSampler[T](lb, ub, !complement)

  override def clone: BernoulliCellSampler[T] = new BernoulliCellSampler[T](lb, ub, complement)
}


/**
 * :: DeveloperApi ::
 * 基于伯努利试验的简单随机采样器，每个元素独立以指定概率被采样
 * 
 * @param fraction 采样概率（采样比例）
 * @tparam T 元素类型
 */
@DeveloperApi
class BernoulliSampler[T: ClassTag](fraction: Double) extends RandomSampler[T, T] {

  /** 校验采样比例在[0,1]区间，考虑浮点误差容限 */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon)
      && fraction <= (1.0 + RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be on interval [0, 1]")

  // 内部随机数生成器
  private val rng: Random = RandomSampler.newDefaultRNG

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  // 延迟初始化间隙采样优化器，仅在需要时创建
  private lazy val gapSampling: GapSampling =
    new GapSampling(fraction, rng, RandomSampler.rngEpsilon)

  override def sample(): Int = {
    if (fraction <= 0.0) {
      // 采样比例为0，不采样任何元素
      0
    } else if (fraction >= 1.0) {
      // 采样比例为1，全采样
      1
    } else if (fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
      // 采样比例低于阈值，使用间隙采样优化减少随机数生成次数
      gapSampling.sample()
    } else {
      // 传统伯努利采样，每个元素生成一次随机数判断
      if (rng.nextDouble() <= fraction) {
        1
      } else {
        0
      }
    }
  }

  override def clone: BernoulliSampler[T] = new BernoulliSampler[T](fraction)
}


/**
 * :: DeveloperApi ::
 * 基于泊松分布的带放回随机采样器，每个元素被采样次数服从泊松分布
 * 
 * @param fraction 带放回采样比例，对应泊松分布的均值
 * @param useGapSamplingIfPossible 是否在低采样比例时使用间隙采样优化，默认为true
 * @tparam T 元素类型
 */
@DeveloperApi
class PoissonSampler[T](
    fraction: Double,
    useGapSamplingIfPossible: Boolean) extends RandomSampler[T, T] {

  def this(fraction: Double) = this(fraction, useGapSamplingIfPossible = true)

  /** 校验采样比例非负，考虑浮点误差容限 */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be >= 0")

  // Apache Commons Math3泊松分布实例，fraction<=0时使用占位值1.0，实际不会用到
  private val rng = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
  // 间隙采样使用的随机数生成器
  private val rngGap = RandomSampler.newDefaultRNG

  override def setSeed(seed: Long): Unit = {
    rng.reseedRandomGenerator(seed)
    rngGap.setSeed(seed)
  }

  // 延迟初始化带放回间隙采样优化器，仅在需要时创建
  private lazy val gapSamplingReplacement =
    new GapSamplingReplacement(fraction, rngGap, RandomSampler.rngEpsilon)

  override def sample(): Int = {
    if (fraction <= 0.0) {
      // 采样比例为0，不采样
      0
    } else if (useGapSamplingIfPossible &&
               fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
      // 低采样比例，使用间隙采样优化
      gapSamplingReplacement.sample()
    } else {
      // 直接从泊松分布采样得到采样次数
      rng.sample()
    }
  }

  override def sample(items: Iterator[T]): Iterator[T] = {
    if (fraction <= 0.0) {
      // 采样比例为0，返回空迭代器
      Iterator.empty
    } else {
      // 判断是否使用间隙采样优化
      val useGapSampling = useGapSamplingIfPossible &&
        fraction <= RandomSampler.defaultMaxGapSamplingFraction

      // 对每个元素根据采样次数展开生成对应数量的副本
      items.flatMap { item =>
        val count = if (useGapSampling) gapSamplingReplacement.sample() else rng.sample()
        if (count == 0) Iterator.empty else Iterator.fill(count)(item)
      }
    }
  }

  override def clone: PoissonSampler[T] = new PoissonSampler[T](fraction, useGapSamplingIfPossible)
}


/**
 * 无放回采样的间隙采样优化实现，通过几何分布一次计算出需要跳过的元素数量，减少随机数生成次数
 * 适用于低采样比例场景，相比传统伯努利采样减少RNG调用次数提升性能
 * 
 * @param f 采样比例
 * @param rng 随机数生成器
 * @param epsilon 随机数最小epsilon值
 */
private[spark]
class GapSampling(
    f: Double,
    rng: Random = RandomSampler.newDefaultRNG,
    epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {

  require(f > 0.0  &&  f < 1.0, s"Sampling fraction ($f) must reside on open interval (0, 1)")
  require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")

  // 预计算ln(1-f)用于后续几何分布计算
  private val lnq = math.log1p(-f)

  /**
   * 返回当前元素是否需要采样，1表示采样，0表示跳过
   * @return 1采样，0跳过
   */
  def sample(): Int = {
    if (countForDropping > 0) {
      // 还有需要跳过的元素，计数减一返回不采样
      countForDropping -= 1
      0
    } else {
      // 计算下一批需要跳过的元素，当前元素采样
      advance()
      1
    }
  }

  // 当前剩余需要跳过的元素数量
  private var countForDropping: Int = 0

  /**
   * 根据几何分布计算下一批需要跳过的元素数量
   * 几何分布P(k) = (f)(1-f)^k，k为需要跳过的元素数量
   */
  private def advance(): Unit = {
    val u = math.max(rng.nextDouble(), epsilon)
    countForDropping = (math.log(u) / lnq).toInt
  }

  // 对象构造时预先计算第一个采样点需要跳过的元素数量，放在最后执行避免初始化问题
  advance()
  // Attempting to invoke this closer to the top with other object initialization
  // was causing it to break in strange ways, so I'm invoking it last, which seems to
  // work reliably.
}


/**
 * 带放回采样的间隙采样优化实现，结合几何分布跳过零复制元素和条件泊松分布生成采样次数
 * 适用于低采样比例带放回采样场景，减少随机数生成次数提升性能
 * 
 * @param f 采样比例，对应泊松分布均值
 * @param rng 随机数生成器
 * @param epsilon 随机数最小epsilon值
 */
private[spark]
class GapSamplingReplacement(
    val f: Double,
    val rng: Random = RandomSampler.newDefaultRNG,
    epsilon: Double = RandomSampler.rngEpsilon) extends Serializable {

  require(f > 0.0, s"Sampling fraction ($f) must be > 0")
  require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")

  // q = e^(-f)，对应泊松分布取0的概率
  protected val q = math.exp(-f)

  /**
   * 从条件泊松分布（条件：值>=1）采样，得到当前元素的采样次数
   * 基于维基百科泊松分布随机数生成算法改编
   * @return 当前元素需要采样的次数
   */
  protected def poissonGE1: Int = {
    // 模拟已经保证至少一次采样，满足条件分布>=1
    var pp = q + ((1.0 - q) * rng.nextDouble())
    var r = 1

    // 继续标准泊松采样流程
    pp *= rng.nextDouble()
    while (pp > q) {
      r += 1
      pp *= rng.nextDouble()
    }
    r
  }
  // 当前剩余需要跳过的元素数量
  private var countForDropping: Int = 0

  def sample(): Int = {
    if (countForDropping > 0) {
      // 还有需要跳过的元素，计数减一返回不采样
      countForDropping -= 1
      0
    } else {
      // 采样得到当前元素的重复次数，计算下一批需要跳过的元素
      val r = poissonGE1
      advance()
      r
    }
  }

  /**
   * 根据几何分布计算需要跳过的元素数量，q=e^(-f)是泊松分布出0的概率
   * 几何分布P(k) = (1-q)(q)^k，k为需要跳过的元素数量
   */
  private def advance(): Unit = {
    val u = math.max(rng.nextDouble(), epsilon)
    countForDropping = (math.log(u) / (-f)).toInt
  }

  // 对象构造时预先计算第一个采样点需要跳过的元素数量，放在最后执行避免初始化问题
  advance()
  // Attempting to invoke this closer to the top with other object initialization
  // was causing it to break in strange ways, so I'm invoking it last, which seems to
  // work reliably.
}
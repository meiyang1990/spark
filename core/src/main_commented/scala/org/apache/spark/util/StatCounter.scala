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

package org.apache.spark.util

import org.apache.spark.annotation.Since

/**
 * 文件概述：数值统计工具类，基于Welford和Chan算法实现数值稳定的增量统计计算，
 * 支持合并多个统计结果，可在分布式场景下安全聚合统计信息，提供计数、均值、方差、最值等基础统计量。
 *
 * 数值统计计算器，以数值稳定的方式跟踪一组数值的统计信息（计数、均值、方差），
 * 支持多个统计结果合并，基于Welford和Chan的增量方差计算算法实现。
 *
 * @constructor 使用给定初始值集合初始化统计计算器
 * @param values 初始数值集合
 */
class StatCounter(values: IterableOnce[Double]) extends Serializable {
  // 当前已统计的数值总数
  private var n: Long = 0
  // 当前累计均值
  private var mu: Double = 0
  // 方差分子部分，即sum((x - mean)^2)
  private var m2: Double = 0
  // 当前统计到的最大值
  private var maxValue: Double = Double.NegativeInfinity
  // 当前统计到的最小值
  private var minValue: Double = Double.PositiveInfinity

  merge(values)

  /** 初始化空的统计计算器，不包含任何数值 */
  def this() = this(Nil)

  /**
   * 添加单个数值到当前统计，更新内部统计信息
   * @param value 待添加的数值
   * @return 当前统计计算器对象，支持链式调用
   */
  def merge(value: Double): StatCounter = {
    val delta = value - mu
    n += 1
    mu += delta / n
    m2 += delta * (value - mu)
    maxValue = math.max(maxValue, value)
    minValue = math.min(minValue, value)
    this
  }

  /**
   * 添加多个数值到当前统计，批量更新内部统计信息
   * @param values 待添加的数值集合
   * @return 当前统计计算器对象，支持链式调用
   */
  def merge(values: IterableOnce[Double]): StatCounter = {
    values.iterator.foreach(v => merge(v))
    this
  }

  /**
   * 将另一个统计计算器的结果合并到当前统计计算器，聚合所有统计信息
   * 用于分布式场景下，合并不同分区计算得到的局部统计结果
   * @param other 待合并的另一个统计计算器
   * @return 当前统计计算器对象，支持链式调用
   */
  def merge(other: StatCounter): StatCounter = {
    if (other == this) {
      // 避免自我合并导致的字段覆盖异常，先复制一份再合并
      merge(other.copy())
    } else {
      if (n == 0) {
        // 当前为空，直接复制另一个统计结果
        mu = other.mu
        m2 = other.m2
        n = other.n
        maxValue = other.maxValue
        minValue = other.minValue
      } else if (other.n != 0) {
        val delta = other.mu - mu
        // 根据两个统计量的大小选择数值更稳定的均值计算方式
        if (other.n * 10 < n) {
          mu = mu + (delta * other.n) / (n + other.n)
        } else if (n * 10 < other.n) {
          mu = other.mu - (delta * n) / (n + other.n)
        } else {
          mu = (mu * n + other.mu * other.n) / (n + other.n)
        }
        // 合并方差计算项
        m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n)
        n += other.n
        // 更新最值
        maxValue = math.max(maxValue, other.maxValue)
        minValue = math.min(minValue, other.minValue)
      }
      this
    }
  }

  /**
   * 克隆当前统计计算器，生成一个统计信息完全相同的新对象
   * @return 克隆得到的新统计计算器
   */
  def copy(): StatCounter = {
    val other = new StatCounter
    other.n = n
    other.mu = mu
    other.m2 = m2
    other.maxValue = maxValue
    other.minValue = minValue
    other
  }

  def count: Long = n

  def mean: Double = mu

  def sum: Double = n * mu

  def max: Double = maxValue

  def min: Double = minValue

  /** 返回总体方差 */
  def variance: Double = popVariance

  /**
   * 计算总体方差
   * @return 总体方差，无数据时返回NaN
   */
  @Since("2.1.0")
  def popVariance: Double = {
    if (n == 0) {
      Double.NaN
    } else {
      m2 / n
    }
  }

  /**
   * 计算样本方差，使用n-1分母修正无偏估计
   * @return 样本方差，数据量小于等于1时返回NaN
   */
  def sampleVariance: Double = {
    if (n <= 1) {
      Double.NaN
    } else {
      m2 / (n - 1)
    }
  }

  /** 返回总体标准差 */
  def stdev: Double = popStdev

  /**
   * 计算总体标准差
   * @return 总体标准差
   */
  @Since("2.1.0")
  def popStdev: Double = math.sqrt(popVariance)

  /**
   * 计算样本标准差，使用n-1分母修正无偏估计
   * @return 样本标准差
   */
  def sampleStdev: Double = math.sqrt(sampleVariance)

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f, max: %f, min: %f)".format(count, mean, stdev, max, min)
  }
}

/**
 * StatCounter伴生对象，提供便捷的工厂方法用于创建StatCounter实例
 */
object StatCounter {
  /**
   * 从数值集合构建StatCounter
   * @param values 输入数值集合
   * @return 初始化完成的StatCounter实例
   */
  def apply(values: IterableOnce[Double]): StatCounter = new StatCounter(values)

  /**
   * 从可变参数列表构建StatCounter
   * @param values 输入数值可变参数
   * @return 初始化完成的StatCounter实例
   */
  def apply(values: Double*): StatCounter = new StatCounter(values)
}
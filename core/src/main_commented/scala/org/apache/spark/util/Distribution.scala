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

import java.io.PrintStream

/**
 * 小样本数值集合的统计分布计算工具，提供分位数等统计摘要
 * 
 * 仅在内存中计算，不适用于大数据集的统计计算
 * 
 * 要求输入非空数据集
 *
 * @param data 待统计的数值数组
 * @param startIdx 统计范围起始索引（包含）
 * @param endIdx 统计范围结束索引（不包含）
 */
private[spark] class Distribution(val data: Array[Double], val startIdx: Int, val endIdx: Int) {
  require(startIdx < endIdx)
  def this(data: Iterable[Double]) = this(data.toArray, 0, data.size)
  // 对数据数组中指定范围排序，为计算分位数做准备
  java.util.Arrays.sort(data, startIdx, endIdx)
  val length = endIdx - startIdx

  // 默认分位数概率：最小值、下四分位数、中位数、上四分位数、最大值
  val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  /**
   * 根据给定概率计算对应分位数值
   * @param probabilities 分位数概率集合，取值范围0到1，默认为defaultProbabilities
   * @return 对应分位数的数值序列
   */
  def getQuantiles(probabilities: Iterable[Double] = defaultProbabilities)
      : IndexedSeq[Double] = {
    probabilities.toIndexedSeq.map { p: Double => data(closestIndex(p)) }
  }

  // 根据概率计算对应分位数在排序数组中的索引
  private def closestIndex(p: Double) = {
    math.min((p * length).toInt + startIdx, endIdx - 1)
  }

  /**
   * 打印默认五个分位数到输出流
   * @param out 输出流，默认为标准输出
   */
  def showQuantiles(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    getQuantiles(defaultProbabilities).foreach{q => out.print(s"$q\t")}
    out.println
    // scalastyle:on println
  }

  // 基于当前数据范围生成统计计数器，提供基础统计信息
  def statCounter: StatCounter = StatCounter(data.slice(startIdx, endIdx))

  /**
   * 打印完整统计分布摘要到输出流，包含基础统计量和分位数
   * @param out 输出流，默认为标准输出
   */
  def summary(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println(statCounter)
    showQuantiles(out)
    // scalastyle:on println
  }
}

/**
 * Distribution工具伴生对象，提供Distribution创建和静态工具方法
 */
private[spark] object Distribution {

  /**
   * 根据可迭代数据集创建Distribution实例，处理空数据场景
   * @param data 输入数值集合
   * @return 非空数据返回Some(Distribution)，空数据返回None
   */
  def apply(data: Iterable[Double]): Option[Distribution] = {
    if (data.nonEmpty) {
      Some(new Distribution(data))
    } else {
        None
    }
  }

  /**
   * 静态工具方法，直接打印已计算好的分位数到输出流
   * @param out 输出流，默认为标准输出
   * @param quantiles 已计算好的分位数集合，按min 25% 50% 75% max顺序排列
   */
  def showQuantiles(out: PrintStream = System.out, quantiles: Iterable[Double]): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    quantiles.foreach{q => out.print(s"$q\t")}
    out.println
    // scalastyle:on println
  }
}
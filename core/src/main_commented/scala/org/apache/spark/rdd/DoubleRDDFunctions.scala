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

package org.apache.spark.rdd

import org.apache.spark.TaskContext
import org.apache.spark.annotation.Since
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.MeanEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.partial.SumEvaluator
import org.apache.spark.util.StatCounter

/**
 * 为Double类型RDD提供扩展统计函数，通过隐式转换自动增强功能
 * 封装了求和、均值、方差、标准差、直方图等常见数值统计操作
 */
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  /** 对RDD中所有元素求和 */
  def sum(): Double = self.withScope {
    self.fold(0.0)(_ + _)
  }

  /**
   * 在单次操作中同时计算并返回包含计数、均值、方差的统计对象，避免多次遍历RDD
   * @return 包含统计结果的StatCounter对象
   */
  def stats(): StatCounter = self.withScope {
    self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b))
  }

  /** 计算RDD所有元素的均值 */
  def mean(): Double = self.withScope {
    stats().mean
  }

  /** 计算RDD所有元素的总体方差 */
  def variance(): Double = self.withScope {
    stats().variance
  }

  /** 计算RDD所有元素的总体标准差 */
  def stdev(): Double = self.withScope {
    stats().stdev
  }

  /**
   * 计算RDD所有元素的样本标准差，使用N-1作为分母修正估计偏差
   */
  def sampleStdev(): Double = self.withScope {
    stats().sampleStdev
  }

  /**
   * 计算RDD所有元素的样本方差，使用N-1作为分母修正估计偏差
   */
  def sampleVariance(): Double = self.withScope {
    stats().sampleVariance
  }

  /**
   * 计算RDD所有元素的总体标准差
   */
  @Since("2.1.0")
  def popStdev(): Double = self.withScope {
    stats().popStdev
  }

  /**
   * 计算RDD所有元素的总体方差
   */
  @Since("2.1.0")
  def popVariance(): Double = self.withScope {
    stats().popVariance
  }

  /**
   * 在指定超时内返回近似均值，适用于大数据集快速预估
   * @param timeout 最大允许执行时间（毫秒）
   * @param confidence 置信度，默认0.95
   * @return 带置信区间的近似均值结果
   */
  def meanApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * 在指定超时内返回近似求和结果，适用于大数据集快速预估
   * @param timeout 最大允许执行时间（毫秒）
   * @param confidence 置信度，默认0.95
   * @return 带置信区间的近似求和结果
   */
  def sumApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * 基于指定分桶数量在RDD的最小和最大值之间生成均匀分桶，计算数据直方图
   * @param bucketCount 分桶数量，必须不小于1
   * @return (分桶边界数组, 每个分桶的元素计数数组)
   */
  def histogram(bucketCount: Int): (Array[Double], Array[Long]) = self.withScope {
    // 解决Scala内置Range的已知问题，参考#SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    }
    // 计算RDD全局最大最小值
    val (max: Double, min: Double) = self.mapPartitions { items =>
      Iterator(
        items.foldRight((Double.NegativeInfinity, Double.PositiveInfinity)
        )((e: Double, x: (Double, Double)) => (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    // 检查数据合法性，包含NaN或无穷大时抛出异常
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw SparkCoreErrors.histogramOnEmptyRDDOrContainingInfinityOrNaNError()
    }
    // 生成分桶边界，处理最大值等于最小值的特殊情况
    val range = if (min != max) {
      // 使用自定义Range解决Scala Range.Double的已知bug，参考#SI-8782
      customRange(min, max, bucketCount)
    } else {
      List(min, min)
    }
    val buckets = range.toArray
    (buckets, histogram(buckets, true))
  }

  /**
   * 基于用户提供的分桶边界计算数据直方图，分桶左闭右开，最后一个分桶为闭区间
   * @param buckets 分桶边界数组，必须已排序且无重复，长度至少为2
   * @param evenBuckets 是否为均匀间隔分桶，启用后可使用O(1)快速定位，默认false
   * @return 每个分桶的元素计数数组
   */
  def histogram(
      buckets: Array[Double],
      evenBuckets: Boolean = false): Array[Long] = self.withScope {
    // 检查分桶参数合法性
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    // 单个分区的直方图计算函数，使用传入的bucketFunction定位元素分桶
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
        Iterator[Array[Long]] = {
      // 初始化计数器数组
      val counters = new Array[Long](buckets.length - 1)
      // 遍历分区中元素，对落入分桶的元素计数
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => counters(x) += 1
          case _ => // 元素不在任何分桶内，不计数
        }
      }
      Iterator(counters)
    }
    // 合并各分区的计数结果
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] = {
      a1.indices.foreach(i => a1(i) += a2(i))
      a1
    }
    // 基础分桶定位函数，使用二分查找，时间复杂度O(log n)，适用于任意分桶
    def basicBucketFunction(e: Double): Option[Int] = {
      val location = java.util.Arrays.binarySearch(buckets, e)
      if (location < 0) {
        // 未找到精确匹配，计算插入点
        val insertionPoint = -location-1
        // 检查插入点是否在有效范围内
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint-1)
        } else {
          None
        }
      } else if (location < buckets.length - 1) {
        // 精确匹配，落在当前分桶
        Some(location)
      } else {
        // 精确匹配最后一个边界，归入最后一个分桶
        Some(location - 1)
      }
    }
    // 均匀分桶的快速定位函数，时间复杂度O(1)
    def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
      // 过滤不在范围内的元素和NaN
      if (e.isNaN || e < min || e > max) {
        None
      } else {
        // 根据相对位置直接计算分桶编号
        val bucketNumber = (((e - min) / (max - min)) * count).toInt
        // 最大值归入最后一个分桶
        Some(math.min(bucketNumber, count - 1))
      }
    }
    // 根据是否均匀分桶选择对应的定位函数，仅在驱动端决策一次
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
    } else {
      basicBucketFunction _
    }
    // 处理空RDD场景，返回全零计数数组
    if (self.partitions.length == 0) {
      new Array[Long](buckets.length - 1)
    } else {
      // 对各分区计算结果归约得到最终直方图计数
      self.mapPartitions(histogramPartition(bucketFunction)).reduce(mergeCounters)
    }
  }

}
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

import java.text.NumberFormat
import java.util.{Arrays, Locale}

import scala.concurrent.duration._

import org.apache.spark.SparkException
import org.apache.spark.util.Utils

/**
 * Spark指标工具类，提供指标聚合、格式化输出能力，用于物理算子和任务的指标展示
 */
private[spark] object MetricUtils {

  val SUM_METRIC: String = "sum"
  val SIZE_METRIC: String = "size"
  val TIMING_METRIC: String = "timing"
  val NS_TIMING_METRIC: String = "nsTiming"
  val AVERAGE_METRIC: String = "average"
  private val baseForAvgMetric: Int = 10
  private val METRICS_NAME_SUFFIX = "(min, med, max (stageId: taskId))"

  /**
   * 将平均指标值格式化为字符串
   * @param value 原始累计指标值
   * @return 格式化后的平均指标字符串
   */
  private def toNumberFormat(value: Long): String = {
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    numberFormat.format(value.toDouble / baseForAvgMetric)
  }

  /**
   * 判断指标类型是否需要记录最大值
   * @param metricsType 指标类型
   * @return 是否需要记录最大值
   */
  def metricNeedsMax(metricsType: String): Boolean = {
    metricsType != SUM_METRIC
  }

  /**
   * A function that defines how we aggregate the final accumulator results among all tasks,
   * and represent it in string for a SQL physical operator.
    */
  /**
   * 聚合所有任务的累加器结果，格式化为适合SQL物理算子展示的字符串
   * @param metricsType 指标类型
   * @param values 所有任务的指标值数组
   * @param maxMetrics 最大值对应的任务信息数组，包含stageId、attemptId、taskId
   * @return 格式化后的指标展示字符串
   */
  def stringValue(metricsType: String, values: Array[Long], maxMetrics: Array[Long]): String = {
    // 构造任务/驱动程序信息字符串
    val taskInfo = if (maxMetrics.isEmpty) {
      "(driver)"
    } else {
      s"(stage ${maxMetrics(1)}.${maxMetrics(2)}: task ${maxMetrics(3)})"
    }
    if (metricsType == SUM_METRIC) {
      // 求和指标，直接输出总和
      val numberFormat = NumberFormat.getIntegerInstance(Locale.US)
      numberFormat.format(values.sum)
    } else if (metricsType == AVERAGE_METRIC) {
      val validValues = values.filter(_ > 0)
      // 有效数据少于等于1条时，不需要展示分位数统计，直接输出结果
      if (validValues.length <= 1) {
        toNumberFormat(validValues.headOption.getOrElse(0))
      } else {
        // 排序后计算并输出最小、中位数、最大三个分位数
        val Seq(min, med, max) = {
          Arrays.sort(validValues)
          Seq(
            toNumberFormat(validValues(0)),
            toNumberFormat(validValues(validValues.length / 2)),
            toNumberFormat(validValues(validValues.length - 1)))
        }
        s"$METRICS_NAME_SUFFIX:\n($min, $med, $max $taskInfo)"
      }
    } else {
      // 根据指标类型选择对应的格式化函数
      val strFormat: Long => String = if (metricsType == SIZE_METRIC) {
        Utils.bytesToString
      } else if (metricsType == TIMING_METRIC) {
        Utils.msDurationToString
      } else if (metricsType == NS_TIMING_METRIC) {
        // 纳秒转换为毫秒后格式化
        duration => Utils.msDurationToString(duration.nanos.toMillis)
      } else {
        throw SparkException.internalError(s"unexpected metrics type: $metricsType")
      }

      val validValues = values.filter(_ >= 0)
      // 有效数据少于等于1条时，不需要展示分位数统计，直接输出结果
      if (validValues.length <= 1) {
        strFormat(validValues.headOption.getOrElse(0))
      } else {
        // 排序后计算并输出总和、最小、中位数、最大四个统计值
        val Seq(sum, min, med, max) = {
          Arrays.sort(validValues)
          Seq(
            strFormat(validValues.sum),
            strFormat(validValues(0)),
            strFormat(validValues(validValues.length / 2)),
            strFormat(validValues(validValues.length - 1)))
        }
        s"total $METRICS_NAME_SUFFIX\n$sum ($min, $med, $max $taskInfo)"
      }
    }
  }
}
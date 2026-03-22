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

package org.apache.spark.internal.io

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import scala.util.{DynamicVariable, Random}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, JobID}

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics

/**
 * 文件级注释：Spark Hadoop 写操作工具类，提供使用 Hadoop OutputFormat 保存RDD数据时的通用公共工具方法
 * 同时兼容旧版 mapred API 和新版 mapreduce API
 */
/**
 * A helper object that provide common utils used during saving an RDD using a Hadoop OutputFormat
 * (both from the old mapred API and the new mapreduce API)
 */
private[spark]
object SparkHadoopWriterUtils {

  // 输出字节指标更新间隔：每写入N条记录更新一次输出度量
  private val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256
  // 随机数生成器，用于生成唯一JobTracker ID
  private val RAND = new Random()

  // 用于生成JobTracker ID的日期格式化器
  // For job tracker IDs
  private val DATE_TIME_FORMATTER =
    DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss", Locale.US)
      .withZone(ZoneId.systemDefault())

  /**
   * 根据当前时间和作业编号生成Hadoop Job ID
   * 
   * @param time 当前时间
   * @param id 作业编号
   * @return 生成的Hadoop JobID对象
   */
  def createJobID(time: Date, id: Int): JobID = {
    val jobTrackerID = createJobTrackerID(time)
    createJobID(jobTrackerID, id)
  }

  /**
   * 根据JobTracker ID和作业编号生成Hadoop Job ID
   * 
   * @param jobTrackerID 唯一的JobTracker ID
   * @param id 作业编号
   * @return 生成的Hadoop JobID对象
   */
  def createJobID(jobTrackerID: String, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    new JobID(jobTrackerID, id)
  }

  /**
   * 生成唯一的JobTracker ID，基于当前时间加随机数保证唯一性
   * @param time 当前时间
   * @return 生成的JobTracker ID字符串
   */
  def createJobTrackerID(time: Date): String = {
    val base = DATE_TIME_FORMATTER.format(time.toInstant)
    var l1 = RAND.nextLong()
    if (l1 < 0) {
      l1 = -l1
    }
    base + l1
  }

  /**
   * 根据输入路径字符串创建规范化的HDFS Path对象
   * 
   * @param path 输入路径字符串
   * @param conf Hadoop Job配置
   * @return 规范化后的Qualified Path对象
   */
  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  // Note: this needs to be a function instead of a 'val' so that the disableOutputSpecValidation
  // setting can take effect:
  /**
   * 检查是否需要启用输出路径规范验证
   * 
   * @param conf Spark配置
   * @return true表示启用验证，false表示禁用验证
   */
  def isOutputSpecValidationEnabled(conf: SparkConf): Boolean = {
    val validationDisabled = disableOutputSpecValidation.value
    val enabledInConf = conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
    enabledInConf && !validationDisabled
  }

  // TODO: these don't seem like the right abstractions.
  // We should abstract the duplicate code in a less awkward way.

  /**
   * 初始化Hadoop输出度量指标，获取字节写入统计回调
   * 
   * @param context 当前任务上下文
   * @return 输出度量对象和字节数获取回调函数的元组
   */
  def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, () => Long) = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    (context.taskMetrics().outputMetrics, bytesWrittenCallback)
  }

  /**
   * 按间隔更新输出度量指标，避免频繁更新开销
   * 
   * @param outputMetrics 输出度量对象
   * @param callback 获取当前已写入字节数的回调函数
   * @param recordsWritten 当前已写入记录数
   */
  def maybeUpdateOutputMetrics(
      outputMetrics: OutputMetrics,
      callback: () => Long,
      recordsWritten: Long): Unit = {
    if (recordsWritten % RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      outputMetrics.setBytesWritten(callback())
      outputMetrics.setRecordsWritten(recordsWritten)
    }
  }

  /**
   * 动态变量，用于按场景单独禁用输出路径规范验证，参考SPARK-4835
   * 默认为false（不禁用）
   */
  /**
   * Allows for the `spark.hadoop.validateOutputSpecs` checks to be disabled on a case-by-case
   * basis; see SPARK-4835 for more details.
   */
  val disableOutputSpecValidation: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}
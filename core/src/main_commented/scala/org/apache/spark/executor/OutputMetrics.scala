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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.LongAccumulator

/**
 * 文件说明：Spark执行器输出指标模块，定义数据输出方式枚举和输出指标收集类，
 * 用于收集任务输出到外部系统的统计指标，供Spark UI监控和性能分析使用。
 */

/**
 * :: DeveloperApi ::
 * 数据写入方式枚举，定义Spark任务将数据写入外部系统的方式类型。
 * 指标收集会根据不同写入方式分类统计，当前仅支持Hadoop生态文件系统写入。
 * 注意：所有操作非线程安全。
 */
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  // Hadoop生态文件系统（HDFS等）写入方式
  val Hadoop = Value
}


/**
 * :: DeveloperApi ::
 * 输出指标聚合类，通过累加器收集Spark任务写入外部系统的运行指标。
 * 收集到的指标会在作业完成后汇总上报，最终在Spark UI展示，用于监控作业输出性能和数据量。
 * 核心指标包含：总写入字节数、总写入记录数。
 */
@DeveloperApi
class OutputMetrics private[spark] () extends Serializable {
  // 记录写入总字节数的累加器，仅executor包内可见
  private[executor] val _bytesWritten = new LongAccumulator
  // 记录写入总记录数的累加器，仅executor包内可见
  private[executor] val _recordsWritten = new LongAccumulator

  /**
   * 获取任务写入的总字节数
   * @return 累计写入字节数
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * 获取任务写入的总记录数
   * @return 累计写入记录数
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * 设置写入字节数，覆盖累加器当前值，仅spark内部使用
   * @param v 要设置的字节数数值
   */
  private[spark] def setBytesWritten(v: Long): Unit = _bytesWritten.setValue(v)

  /**
   * 设置写入记录数，覆盖累加器当前值，仅spark内部使用
   * @param v 要设置的记录数数值
   */
  private[spark] def setRecordsWritten(v: Long): Unit = _recordsWritten.setValue(v)
}
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
 * :: DeveloperApi ::
 * Method by which output data was written.
 * Operations are not thread-safe.
 *
 * 数据写入方式枚举。定义数据写入外部系统的方式。
 * 当前只支持 Hadoop（HDFS 等）文件系统的写入方式。
 * 注意：操作非线程安全
 */
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  // Hadoop 文件系统写入
  val Hadoop = Value
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about writing data to external systems.
 *
 * 输出指标类，用于收集将数据写入外部系统的相关指标。
 * 这些指标通过累加器收集，最终在 Spark UI 中展示，
 * 帮助用户了解作业的数据输出量和写入性能。
 */
@DeveloperApi
class OutputMetrics private[spark] () extends Serializable {
  // 写入的字节数累加器
  private[executor] val _bytesWritten = new LongAccumulator
  // 写入的记录数累加器
  private[executor] val _recordsWritten = new LongAccumulator

  /**
   * Total number of bytes written.
   * 返回写入的总字节数
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written.
   * 返回写入的总记录数
   */
  def recordsWritten: Long = _recordsWritten.sum

  // 设置写入字节数（覆盖原值）
  private[spark] def setBytesWritten(v: Long): Unit = _bytesWritten.setValue(v)
  // 设置写入记录数（覆盖原值）
  private[spark] def setRecordsWritten(v: Long): Unit = _recordsWritten.setValue(v)
}

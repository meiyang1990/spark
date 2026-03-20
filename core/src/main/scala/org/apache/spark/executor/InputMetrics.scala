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
 * Method by which input data was read. Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 * Operations are not thread-safe.
 *
 * 数据读取方式枚举。定义数据从不同来源读取的方式：
 * - Memory: 从内存中读取
 * - Disk: 从本地磁盘读取
 * - Hadoop: 从 Hadoop 文件系统（如 HDFS）读取
 * - Network: 从远程 BlockManager 通过网络读取（远端数据可能存储在内存或磁盘中）
 * 注意：操作非线程安全
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  // 内存、磁盘、Hadoop 文件系统、网络
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represents metrics about reading data from external systems.
 *
 * 输入指标类，用于收集从外部系统读取数据的相关指标。
 * 这些指标通过累加器收集，可以跨多个任务聚合，
 * 最终在 Spark UI 中展示，帮助用户了解作业的数据输入情况。
 */
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
  // 读取的字节数累加器
  private[executor] val _bytesRead = new LongAccumulator
  // 读取的记录数累加器
  private[executor] val _recordsRead = new LongAccumulator

  /**
   * Total number of bytes read.
   * 返回读取的总字节数
   */
  def bytesRead: Long = _bytesRead.sum

  /**
   * Total number of records read.
   * 返回读取的总记录数
   */
  def recordsRead: Long = _recordsRead.sum

  // 增加读取字节数
  private[spark] def incBytesRead(v: Long): Unit = _bytesRead.add(v)
  // 增加读取记录数
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)
  // 设置读取字节数（覆盖原值）
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)
  // For test only
  // 设置读取记录数（仅用于测试）
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
}

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
 * 文件: core/src/main/scala/org/apache/spark/executor/InputMetrics.scala
 * 功能: 定义Spark任务输入数据读取指标相关的类和枚举，用于收集和聚合任务输入数据的统计信息
 */

/**
 * :: DeveloperApi ::
 * 数据读取方式枚举，定义输入数据的来源类型，用于标识数据从何种途径读取
 * 网络读取表示数据通过网络从远程BlockManager获取，远端数据可能存储在磁盘或内存中
 * 注意：当前枚举操作非线程安全
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  // 内存、磁盘、Hadoop文件系统、网络远程读取
  val Memory, Disk, Hadoop, Network = Value
}


/**
 * :: DeveloperApi ::
 * 输入指标容器，基于累加器收集任务从外部系统读取数据的各类统计指标
 * 指标会在所有执行节点聚合后，最终展示在Spark UI中，用于监控和分析作业输入性能
 */
@DeveloperApi
class InputMetrics private[spark] () extends Serializable {
  // 累计读取字节数的累加器
  private[executor] val _bytesRead = new LongAccumulator
  // 累计读取记录数的累加器
  private[executor] val _recordsRead = new LongAccumulator

  /**
   * 获取累计读取的总字节数
   * @return 累计读取字节数
   */
  def bytesRead: Long = _bytesRead.sum

  /**
   * 获取累计读取的总记录数
   * @return 累计读取记录数
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * 增加读取字节数计数
   * @param v 新增字节数
   */
  private[spark] def incBytesRead(v: Long): Unit = _bytesRead.add(v)

  /**
   * 增加读取记录数计数
   * @param v 新增记录数
   */
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)

  /**
   * 直接设置累计读取字节数，覆盖原有值
   * @param v 新的字节数累计值
   */
  private[spark] def setBytesRead(v: Long): Unit = _bytesRead.setValue(v)

  /**
   * 直接设置累计读取记录数，覆盖原有值，仅用于测试场景
   * @param v 新的记录数累计值
   */
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)
}
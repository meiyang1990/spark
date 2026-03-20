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
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 *
 * Shuffle 写入指标类，用于收集 Shuffle 数据写入过程的相关指标。
 * 在 MapReduce 风格的分布式计算中，Shuffle 阶段负责将 Map 任务的输出
 * 按照 key 进行分区并写入磁盘，供后续 Reduce 任务读取。
 * 这些指标帮助用户了解 Shuffle 写入的数据量和耗时。
 * 注意：操作非线程安全
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  // Shuffle 写入的字节数累加器
  private[executor] val _bytesWritten = new LongAccumulator
  // Shuffle 写入的记录数累加器
  private[executor] val _recordsWritten = new LongAccumulator
  // Shuffle 写入耗时累加器（纳秒）
  private[executor] val _writeTime = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
   * 返回此任务为 Shuffle 写入的字节数
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   * 返回此任务写入 Shuffle 的总记录数
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   * 返回任务在写入磁盘或缓冲区缓存时的阻塞时间（纳秒）
   */
  def writeTime: Long = _writeTime.sum

  // 增加写入字节数
  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  // 增加写入记录数
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  // 增加写入耗时
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  // 减少写入字节数（用于回滚场景）
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  // 减少写入记录数（用于回滚场景）
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
}

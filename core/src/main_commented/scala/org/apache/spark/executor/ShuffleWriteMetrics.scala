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
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator

/**
 * 文件级注释：Shuffle写入阶段指标收集类，属于Spark核心执行模块，用于聚合存储Shuffle数据写入过程的各项性能指标，供监控和调优使用
 *
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 *
 * 用于收集Shuffle数据写入过程的各类统计指标，在Shuffle阶段Map任务输出数据时记录写入的字节数、记录数和耗时，帮助监控Shuffle性能瓶颈。
 * 注意：此类操作非线程安全，仅在单个任务写线程中更新指标，不会并发修改。
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  // 记录Shuffle写入总字节数的累加器
  private[executor] val _bytesWritten = new LongAccumulator
  // 记录Shuffle写入总记录数的累加器
  private[executor] val _recordsWritten = new LongAccumulator
  // 记录Shuffle写入总阻塞耗时的累加器，单位为纳秒
  private[executor] val _writeTime = new LongAccumulator

  /**
   * 获取当前任务Shuffle写入的总字节数
   * @return 写入总字节数
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * 获取当前任务Shuffle写入的总记录数
   * @return 写入总记录数
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * 获取当前任务Shuffle写入的总阻塞耗时（单位：纳秒），耗时包含写入磁盘和缓冲区缓存的阻塞等待时间
   * @return 总阻塞写耗时（纳秒）
   */
  def writeTime: Long = _writeTime.sum

  /**
   * 增量增加Shuffle写入字节数，对外部SPI接口开放调用
   * @param v 增量字节数
   */
  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)

  /**
   * 增量增加Shuffle写入记录数，对外部SPI接口开放调用
   * @param v 增量记录数
   */
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)

  /**
   * 增量增加Shuffle写入耗时，对外部SPI接口开放调用
   * @param v 增量耗时（纳秒）
   */
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)

  /**
   * 扣减Shuffle写入字节数，用于任务失败回滚或取消写操作场景修正指标
   * @param v 需要扣减的字节数
   */
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }

  /**
   * 扣减Shuffle写入记录数，用于任务失败回滚或取消写操作场景修正指标
   * @param v 需要扣减的记录数
   */
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }
}
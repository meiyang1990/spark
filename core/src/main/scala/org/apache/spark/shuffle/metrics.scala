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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle

/**
 * Shuffle 读取指标报告器接口，用于报告每次 Shuffle 的读取指标。
 * 此接口假设所有方法在单线程中调用，即具体实现无需同步。
 *
 * 所有方法都有额外的 Spark 可见性修饰符，以允许公开的具体实现
 * 同时将这些方法标记为 private[spark]。
 */
private[spark] trait ShuffleReadMetricsReporter {
  /** 增加从远程获取的数据块数 */
  private[spark] def incRemoteBlocksFetched(v: Long): Unit
  /** 增加从本地获取的数据块数 */
  private[spark] def incLocalBlocksFetched(v: Long): Unit
  /** 增加从远程读取的字节数 */
  private[spark] def incRemoteBytesRead(v: Long): Unit
  /** 增加从远程读取并写入磁盘的字节数 */
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit
  /** 增加从本地读取的字节数 */
  private[spark] def incLocalBytesRead(v: Long): Unit
  /** 增加获取数据的等待时间 */
  private[spark] def incFetchWaitTime(v: Long): Unit
  /** 增加读取的记录数 */
  private[spark] def incRecordsRead(v: Long): Unit
  /** 增加损坏的合并数据块 chunk 数 */
  private[spark] def incCorruptMergedBlockChunks(v: Long): Unit
  /** 增加合并获取回退次数 */
  private[spark] def incMergedFetchFallbackCount(v: Long): Unit
  /** 增加从远程获取的合并数据块数 */
  private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit
  /** 增加从本地获取的合并数据块数 */
  private[spark] def incLocalMergedBlocksFetched(v: Long): Unit
  /** 增加从远程获取的合并 chunk 数 */
  private[spark] def incRemoteMergedChunksFetched(v: Long): Unit
  /** 增加从本地获取的合并 chunk 数 */
  private[spark] def incLocalMergedChunksFetched(v: Long): Unit
  /** 增加从远程读取的合并字节数 */
  private[spark] def incRemoteMergedBytesRead(v: Long): Unit
  /** 增加从本地读取的合并字节数 */
  private[spark] def incLocalMergedBytesRead(v: Long): Unit
  /** 增加远程请求持续时间 */
  private[spark] def incRemoteReqsDuration(v: Long): Unit
  /** 增加远程合并请求持续时间 */
  private[spark] def incRemoteMergedReqsDuration(v: Long): Unit
}


/**
 * Shuffle 写入指标报告器接口。
 * 此接口假设所有方法在单线程中调用，即具体实现无需同步。
 *
 * 所有方法都有额外的 Spark 可见性修饰符，以允许公开的具体实现
 * 同时将这些方法标记为 private[spark]。
 */
private[spark] trait ShuffleWriteMetricsReporter {
  /** 增加已写入字节数 */
  private[spark] def incBytesWritten(v: Long): Unit
  /** 增加已写入记录数 */
  private[spark] def incRecordsWritten(v: Long): Unit
  /** 增加写入时间 */
  private[spark] def incWriteTime(v: Long): Unit
  /** 减少已写入字节数（用于回滚） */
  private[spark] def decBytesWritten(v: Long): Unit
  /** 减少已写入记录数（用于回滚） */
  private[spark] def decRecordsWritten(v: Long): Unit
}

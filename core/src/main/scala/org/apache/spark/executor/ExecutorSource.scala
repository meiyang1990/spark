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

import java.util.concurrent.ThreadPoolExecutor

import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.metrics.source.Source

/**
 * Executor 指标数据源，向 Spark Metrics 系统暴露 Executor 级别的监控指标。
 *
 * Spark 使用 Dropwizard Metrics 库进行指标收集和暴露，支持多种 Sink（如 JMX、Graphite、Prometheus）。
 * ExecutorSource 负责注册以下类型的指标：
 *
 * 1. 【线程池指标】- 监控 Task 执行线程池的状态
 *    - activeTasks: 正在执行的任务数
 *    - completeTasks: 已完成的任务总数
 *    - startedTasks: 已启动的任务总数
 *    - currentPool_size: 当前线程池大小
 *    - maxPool_size: 线程池最大容量
 *
 * 2. 【文件系统指标】- 按存储协议（如 hdfs、file、s3a）分类
 *    - read_bytes/write_bytes: 读写字节数
 *    - read_ops/write_ops: 读写操作次数
 *    - largeRead_ops: 大块读取操作次数
 *
 * 3. 【Task 执行指标】- 聚合所有已完成 Task 的指标
 *    - 包括 CPU 时间、GC 时间、Shuffle IO、输入输出等
 *
 * @param threadPool Task 执行线程池
 * @param executorId Executor 唯一标识
 * @param fileSystemSchemes 需要监控的文件系统协议列表（如 Array("hdfs", "file")）
 */
private[spark]
class ExecutorSource(
    threadPool: ThreadPoolExecutor,
    executorId: String,
    fileSystemSchemes: Array[String]) extends Source {

  // 根据文件系统协议获取对应的统计信息
  private def fileStats(scheme: String) : Option[FileSystem.Statistics] =
    FileSystem.getAllStatistics.asScala.find(s => s.getScheme.equals(scheme))

  // 注册文件系统相关的 Gauge 指标
  private def registerFileSystemStat[T](
        scheme: String, name: String, f: FileSystem.Statistics => T, defaultValue: T) = {
    metricRegistry.register(MetricRegistry.name("filesystem", scheme, name), new Gauge[T] {
      override def getValue: T = fileStats(scheme).map(f).getOrElse(defaultValue)
    })
  }

  override val metricRegistry = new MetricRegistry()

  override val sourceName = "executor"

  // ==================== 线程池状态指标（Gauge 类型，实时采样）====================

  // 正在执行的任务数
  metricRegistry.register(MetricRegistry.name("threadpool", "activeTasks"), new Gauge[Int] {
    override def getValue: Int = threadPool.getActiveCount()
  })

  // 已完成的任务总数
  metricRegistry.register(MetricRegistry.name("threadpool", "completeTasks"), new Gauge[Long] {
    override def getValue: Long = threadPool.getCompletedTaskCount()
  })

  // 已启动的任务总数（包括正在执行和已完成的）
  metricRegistry.register(MetricRegistry.name("threadpool", "startedTasks"), new Gauge[Long] {
    override def getValue: Long = threadPool.getTaskCount()
  })

  // 当前线程池中的线程数
  metricRegistry.register(MetricRegistry.name("threadpool", "currentPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getPoolSize()
  })

  // 线程池历史峰值线程数
  metricRegistry.register(MetricRegistry.name("threadpool", "maxPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getMaximumPoolSize()
  })

  // ==================== 文件系统 IO 指标 ====================
  // 为每种配置的文件系统协议注册读写指标
  for (scheme <- fileSystemSchemes) {
    registerFileSystemStat(scheme, "read_bytes", _.getBytesRead(), 0L)
    registerFileSystemStat(scheme, "write_bytes", _.getBytesWritten(), 0L)
    registerFileSystemStat(scheme, "read_ops", _.getReadOps(), 0)
    registerFileSystemStat(scheme, "largeRead_ops", _.getLargeReadOps(), 0)
    registerFileSystemStat(scheme, "write_ops", _.getWriteOps(), 0)
  }

  // ==================== Task 累计指标（Counter 类型，单调递增）====================
  // 这些指标在每个 Task 完成后累加，反映 Executor 的整体工作量

  // 成功完成的 Task 数量
  val SUCCEEDED_TASKS = metricRegistry.counter(MetricRegistry.name("succeededTasks"))
  // CPU 时间累计（纳秒）
  val METRIC_CPU_TIME = metricRegistry.counter(MetricRegistry.name("cpuTime"))
  // 执行时间累计（毫秒）
  val METRIC_RUN_TIME = metricRegistry.counter(MetricRegistry.name("runTime"))
  // GC 时间累计（毫秒）
  val METRIC_JVM_GC_TIME = metricRegistry.counter(MetricRegistry.name("jvmGCTime"))
  // 反序列化时间累计
  val METRIC_DESERIALIZE_TIME =
    metricRegistry.counter(MetricRegistry.name("deserializeTime"))
  // 反序列化 CPU 时间累计
  val METRIC_DESERIALIZE_CPU_TIME =
    metricRegistry.counter(MetricRegistry.name("deserializeCpuTime"))
  // 结果序列化时间累计
  val METRIC_RESULT_SERIALIZE_TIME =
    metricRegistry.counter(MetricRegistry.name("resultSerializationTime"))

  // ==================== Shuffle 读取指标 ====================
  val METRIC_SHUFFLE_FETCH_WAIT_TIME =
    metricRegistry.counter(MetricRegistry.name("shuffleFetchWaitTime"))
  val METRIC_SHUFFLE_TOTAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleTotalBytesRead"))
  val METRIC_SHUFFLE_REMOTE_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesRead"))
  val METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesReadToDisk"))
  val METRIC_SHUFFLE_LOCAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleLocalBytesRead"))
  val METRIC_SHUFFLE_RECORDS_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleRecordsRead"))
  val METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBlocksFetched"))
  val METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleLocalBlocksFetched"))
  val METRIC_SHUFFLE_REMOTE_REQS_DURATION =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteReqsDuration"))

  // ==================== Shuffle 写入指标 ====================
  val METRIC_SHUFFLE_WRITE_TIME =
    metricRegistry.counter(MetricRegistry.name("shuffleWriteTime"))
  val METRIC_SHUFFLE_BYTES_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("shuffleBytesWritten"))
  val METRIC_SHUFFLE_RECORDS_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("shuffleRecordsWritten"))

  // ==================== Push-based Shuffle 指标 ====================
  val METRIC_PUSH_BASED_SHUFFLE_CORRUPT_MERGED_BLOCK_CHUNKS =
    metricRegistry.counter(MetricRegistry.name("shuffleCorruptMergedBlockChunks"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_FETCH_FALLBACK_COUNT =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedFetchFallbackCount"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteBlocksFetched"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedLocalBlocksFetched"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_CHUNKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteChunksFetched"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_CHUNKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedLocalChunksFetched"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteBytesRead"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedLocalBytesRead"))
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_REQS_DURATION =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteReqsDuration"))

  // ==================== 输入输出指标 ====================
  val METRIC_INPUT_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("bytesRead"))
  val METRIC_INPUT_RECORDS_READ =
    metricRegistry.counter(MetricRegistry.name("recordsRead"))
  val METRIC_OUTPUT_BYTES_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("bytesWritten"))
  val METRIC_OUTPUT_RECORDS_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("recordsWritten"))

  // ==================== 其他指标 ====================
  val METRIC_RESULT_SIZE =
    metricRegistry.counter(MetricRegistry.name("resultSize"))
  val METRIC_DISK_BYTES_SPILLED =
    metricRegistry.counter(MetricRegistry.name("diskBytesSpilled"))
  val METRIC_MEMORY_BYTES_SPILLED =
    metricRegistry.counter(MetricRegistry.name("memoryBytesSpilled"))
}

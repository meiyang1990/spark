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

import java.util.concurrent.ThreadPoolExecutor

import scala.jdk.CollectionConverters._

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.metrics.source.Source

/**
 * 文件说明: Executor监控指标数据源，负责向Spark指标系统暴露Executor运行时的各类监控指标
 * 本类属于Spark核心模块的executor组件，为监控系统提供Executor维度的运行状态数据，包括线程池、文件系统IO、Task执行和Shuffle相关指标
 */
private[spark]
class ExecutorSource(
    threadPool: ThreadPoolExecutor,
    executorId: String,
    fileSystemSchemes: Array[String]) extends Source {

  /**
   * 根据指定文件系统协议获取Hadoop对应的统计信息对象
   * @param scheme 文件系统协议（如hdfs、s3a、file等）
   * @return 协议对应的统计信息对象，不存在则返回None
   */
  private def fileStats(scheme: String) : Option[FileSystem.Statistics] =
    FileSystem.getAllStatistics.asScala.find(s => s.getScheme.equals(scheme))

  /**
   * 向指标注册表注册文件系统相关的Gauge指标
   * @param scheme 文件系统协议
   * @param name 指标名称
   * @param f 从统计信息对象中提取指标值的函数
   * @param defaultValue 当统计信息不存在时的默认值
   * @tparam T 指标值类型
   */
  private def registerFileSystemStat[T](
        scheme: String, name: String, f: FileSystem.Statistics => T, defaultValue: T) = {
    metricRegistry.register(MetricRegistry.name("filesystem", scheme, name), new Gauge[T] {
      override def getValue: T = fileStats(scheme).map(f).getOrElse(defaultValue)
    })
  }

  override val metricRegistry = new MetricRegistry()

  override val sourceName = "executor"

  // 正在执行的任务数
  metricRegistry.register(MetricRegistry.name("threadpool", "activeTasks"), new Gauge[Int] {
    override def getValue: Int = threadPool.getActiveCount()
  })

  // 已完成的任务总数
  metricRegistry.register(MetricRegistry.name("threadpool", "completeTasks"), new Gauge[Long] {
    override def getValue: Long = threadPool.getCompletedTaskCount()
  })

  // 已启动的总任务数（包含已完成和正在执行的）
  metricRegistry.register(MetricRegistry.name("threadpool", "startedTasks"), new Gauge[Long] {
    override def getValue: Long = threadPool.getTaskCount()
  })

  // 当前线程池实际工作线程数
  metricRegistry.register(MetricRegistry.name("threadpool", "currentPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getPoolSize()
  })

  // 线程池允许的最大线程数
  metricRegistry.register(MetricRegistry.name("threadpool", "maxPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getMaximumPoolSize()
  })

  // 遍历所有需要监控的文件系统协议，注册各类IO指标
  for (scheme <- fileSystemSchemes) {
    registerFileSystemStat(scheme, "read_bytes", _.getBytesRead(), 0L)
    registerFileSystemStat(scheme, "write_bytes", _.getBytesWritten(), 0L)
    registerFileSystemStat(scheme, "read_ops", _.getReadOps(), 0)
    registerFileSystemStat(scheme, "largeRead_ops", _.getLargeReadOps(), 0)
    registerFileSystemStat(scheme, "write_ops", _.getWriteOps(), 0)
  }

  // 成功完成的任务计数器
  val SUCCEEDED_TASKS = metricRegistry.counter(MetricRegistry.name("succeededTasks"))
  // CPU总耗时计数器（单位：纳秒）
  val METRIC_CPU_TIME = metricRegistry.counter(MetricRegistry.name("cpuTime"))
  // 任务总运行时间计数器（单位：毫秒）
  val METRIC_RUN_TIME = metricRegistry.counter(MetricRegistry.name("runTime"))
  // JVM垃圾回收总耗时计数器（单位：毫秒）
  val METRIC_JVM_GC_TIME = metricRegistry.counter(MetricRegistry.name("jvmGCTime"))
  // 任务反序列化总耗时计数器
  val METRIC_DESERIALIZE_TIME =
    metricRegistry.counter(MetricRegistry.name("deserializeTime"))
  // 任务反序列化CPU总耗时计数器
  val METRIC_DESERIALIZE_CPU_TIME =
    metricRegistry.counter(MetricRegistry.name("deserializeCpuTime"))
  // 任务结果序列化总耗时计数器
  val METRIC_RESULT_SERIALIZE_TIME =
    metricRegistry.counter(MetricRegistry.name("resultSerializationTime"))

  // Shuffle获取等待总耗时计数器
  val METRIC_SHUFFLE_FETCH_WAIT_TIME =
    metricRegistry.counter(MetricRegistry.name("shuffleFetchWaitTime"))
  // Shuffle读取总字节数计数器
  val METRIC_SHUFFLE_TOTAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleTotalBytesRead"))
  // Shuffle远程读取字节数计数器
  val METRIC_SHUFFLE_REMOTE_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesRead"))
  // Shuffle远程读取并写到磁盘的字节数计数器
  val METRIC_SHUFFLE_REMOTE_BYTES_READ_TO_DISK =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBytesReadToDisk"))
  // Shuffle本地读取字节数计数器
  val METRIC_SHUFFLE_LOCAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleLocalBytesRead"))
  // Shuffle读取记录数计数器
  val METRIC_SHUFFLE_RECORDS_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleRecordsRead"))
  // Shuffle远程获取块数计数器
  val METRIC_SHUFFLE_REMOTE_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteBlocksFetched"))
  // Shuffle本地获取块数计数器
  val METRIC_SHUFFLE_LOCAL_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleLocalBlocksFetched"))
  // Shuffle远程请求总耗时计数器
  val METRIC_SHUFFLE_REMOTE_REQS_DURATION =
    metricRegistry.counter(MetricRegistry.name("shuffleRemoteReqsDuration"))

  // Shuffle写入总耗时计数器
  val METRIC_SHUFFLE_WRITE_TIME =
    metricRegistry.counter(MetricRegistry.name("shuffleWriteTime"))
  // Shuffle写入总字节数计数器
  val METRIC_SHUFFLE_BYTES_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("shuffleBytesWritten"))
  // Shuffle写入记录数计数器
  val METRIC_SHUFFLE_RECORDS_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("shuffleRecordsWritten"))

  // 基于推送的Shuffle损坏合并块分块计数器
  val METRIC_PUSH_BASED_SHUFFLE_CORRUPT_MERGED_BLOCK_CHUNKS =
    metricRegistry.counter(MetricRegistry.name("shuffleCorruptMergedBlockChunks"))
  // 基于推送的Shuffle合并读取回退次数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_FETCH_FALLBACK_COUNT =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedFetchFallbackCount"))
  // 基于推送的Shuffle远程合并块获取数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteBlocksFetched"))
  // 基于推送的Shuffle本地合并块获取数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BLOCKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedLocalBlocksFetched"))
  // 基于推送的Shuffle远程合并分块获取数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_CHUNKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteChunksFetched"))
  // 基于推送的Shuffle本地合并分块获取数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_CHUNKS_FETCHED =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedLocalChunksFetched"))
  // 基于推送的Shuffle远程合并读取字节数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteBytesRead"))
  // 基于推送的Shuffle本地合并读取字节数计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_LOCAL_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedLocalBytesRead"))
  // 基于推送的Shuffle远程合并请求总耗时计数器
  val METRIC_PUSH_BASED_SHUFFLE_MERGED_REMOTE_REQS_DURATION =
    metricRegistry.counter(MetricRegistry.name("shuffleMergedRemoteReqsDuration"))

  // 输入读取总字节数计数器
  val METRIC_INPUT_BYTES_READ =
    metricRegistry.counter(MetricRegistry.name("bytesRead"))
  // 输入读取记录数计数器
  val METRIC_INPUT_RECORDS_READ =
    metricRegistry.counter(MetricRegistry.name("recordsRead"))
  // 输出写入总字节数计数器
  val METRIC_OUTPUT_BYTES_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("bytesWritten"))
  // 输出写入记录数计数器
  val METRIC_OUTPUT_RECORDS_WRITTEN =
    metricRegistry.counter(MetricRegistry.name("recordsWritten"))

  // 任务返回结果总大小计数器
  val METRIC_RESULT_SIZE =
    metricRegistry.counter(MetricRegistry.name("resultSize"))
  // 溢写到磁盘的字节数计数器
  val METRIC_DISK_BYTES_SPILLED =
    metricRegistry.counter(MetricRegistry.name("diskBytesSpilled"))
  // 溢读到内存的字节数计数器
  val METRIC_MEMORY_BYTES_SPILLED =
    metricRegistry.counter(MetricRegistry.name("memoryBytesSpilled"))
}
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

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util._

/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 *
 * Task 执行指标类，是 Spark 任务监控体系的核心组件。
 *
 * 【设计目的】
 * 收集 Task 执行过程中的各项性能指标，包括：
 * - 反序列化时间、执行时间、CPU 时间
 * - 结果大小和序列化时间
 * - GC 时间
 * - 内存和磁盘溢写量
 * - 峰值内存使用
 * - Input/Output/Shuffle 相关的 IO 指标
 *
 * 【数据流转】
 * 1. Executor 端：每个 Task 持有独立的 TaskMetrics 实例，执行过程中更新各项指标
 * 2. 定期汇报：通过 Executor 心跳将指标增量发送给 Driver
 * 3. 任务完成：将最终指标随 TaskResult 一起发送给 Driver
 * 4. Driver 端：合并来自各 Task 的指标，用于 Spark UI 展示和监控告警
 *
 * 【技术实现】
 * 所有指标都基于 Accumulator（累加器）机制实现，这是 Spark 提供的分布式聚合原语，
 * 保证了指标能够正确地从 Executor 传递到 Driver 并进行合并。
 *
 * 注意：TaskMetrics 对象本身不会被发送到 Driver，只有其中的 Accumulator 值会被传输。
 */
@DeveloperApi
class TaskMetrics private[spark] () extends Serializable {
  // ==================== Task 执行时间相关指标 ====================
  // 每个指标都通过 Accumulator 实现，支持分布式聚合

  // Task 反序列化耗时（毫秒）：从接收到 Task 字节流到开始执行的时间
  private val _executorDeserializeTime = new LongAccumulator
  // Task 反序列化的 CPU 时间（纳秒）：更精确地衡量反序列化的计算开销
  private val _executorDeserializeCpuTime = new LongAccumulator
  // Task 实际执行时间（毫秒）：包括计算和 Shuffle 数据拉取时间
  private val _executorRunTime = new LongAccumulator
  // Task 执行的 CPU 时间（纳秒）：排除 IO 等待，纯计算时间
  private val _executorCpuTime = new LongAccumulator
  // 返回给 Driver 的结果大小（字节）
  private val _resultSize = new LongAccumulator
  // JVM 垃圾回收耗时（毫秒）：GC 时间过长说明内存压力大
  private val _jvmGCTime = new LongAccumulator
  // 结果序列化耗时（毫秒）
  private val _resultSerializationTime = new LongAccumulator

  // ==================== 内存溢写相关指标 ====================
  // 内存溢写量（字节）：当内存不足时数据溢写到磁盘前的内存占用
  private val _memoryBytesSpilled = new LongAccumulator
  // 磁盘溢写量（字节）：实际写入磁盘的数据量（通常经过压缩，小于内存溢写量）
  private val _diskBytesSpilled = new LongAccumulator

  // ==================== 执行内存峰值指标 ====================
  // 执行内存峰值：Shuffle、聚合、Join 等操作使用的内部数据结构的峰值内存
  private val _peakExecutionMemory = new LongAccumulator
  // 堆内执行内存峰值
  private val _peakOnHeapExecutionMemory = new LongAccumulator
  // 堆外执行内存峰值
  private val _peakOffHeapExecutionMemory = new LongAccumulator

  // ==================== Block 状态追踪 ====================
  // 记录 Task 执行过程中更新的 Block 状态，用于 SparkListenerTaskEnd 事件
  // 注意：此指标内存开销较大，可通过配置关闭
  private val _updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)]

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum

  /**
   * CPU Time taken on the executor to deserialize this task in nanoseconds.
   */
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.sum

  /**
   * CPU Time the executor spends actually running the task
   * (including fetching shuffle data) in nanoseconds.
   */
  def executorCpuTime: Long = _executorCpuTime.sum

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   * This is not equal to peakOnHeapExecutionMemory + peakOffHeapExecutionMemory
   */
  // TODO: SPARK-48789: the naming is confusing since this does not really reflect the whole
  //  execution memory. We'd better deprecate this once we have a replacement.
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Peak on heap execution memory as tracked by TaskMemoryManager.
   */
  def peakOnHeapExecutionMemory: Long = _peakOnHeapExecutionMemory.sum

  /**
   * Peak off heap execution memory as tracked by TaskMemoryManager.
   */
  def peakOffHeapExecutionMemory: Long = _peakOffHeapExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   *
   * Tracking the _updatedBlockStatuses can use a lot of memory.
   * It is not used anywhere inside of Spark so we would ideally remove it, but its exposed to
   * the user in SparkListenerTaskEnd so the api is kept for compatibility.
   * Tracking can be turned off to save memory via config
   * TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    _updatedBlockStatuses.value.asScala.toSeq
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)
  private[spark] def setExecutorDeserializeCpuTime(v: Long): Unit =
    _executorDeserializeCpuTime.setValue(v)
  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)
  private[spark] def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)
  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)
  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)
  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)
  private[spark] def setPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.setValue(v)
  private[spark] def setPeakOnHeapExecutionMemory(v: Long): Unit =
    _peakOnHeapExecutionMemory.setValue(v)
  private[spark] def setPeakOffHeapExecutionMemory(v: Long): Unit =
    _peakOffHeapExecutionMemory.setValue(v)
  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)
  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)
  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)
  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)
  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)
  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  // 读写锁：用于保护外部累加器列表 _externalAccums 的线程安全访问
  // 使用读写锁而非普通锁，允许多个读操作并发执行，提高性能
  private val (readLock, writeLock) = {
    val lock = new ReentrantReadWriteLock()
    (lock.readLock(), lock.writeLock())
  }

  // ==================== IO 子指标对象 ====================
  // 以下四个对象分别收集不同类型的 IO 操作指标

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   *
   * 输入指标：记录从外部数据源（如 HDFS、本地文件）或持久化 RDD 读取数据的指标
   */
  val inputMetrics: InputMetrics = new InputMetrics()

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   *
   * 输出指标：记录向外部存储系统（如 HDFS）写入数据的指标
   */
  val outputMetrics: OutputMetrics = new OutputMetrics()

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   *
   * Shuffle 读取指标：汇总当前 Task 所有 Shuffle 依赖的读取数据指标
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics()

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   *
   * Shuffle 写入指标：记录 Map 阶段输出 Shuffle 数据的指标
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics()

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   *
   * 临时 Shuffle 读取指标列表，每个 Shuffle 依赖对应一个。
   * 采用"先分后合"策略避免并发问题：各 Reader 独立收集，最后统一合并。
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   *
   * 为特定的 Shuffle 依赖创建临时指标收集器。
   * 调用方必须在 Task 结束前调用 mergeShuffleReadMetrics() 进行合并，否则数据会丢失。
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   *
   * 合并所有临时 Shuffle 读取指标到最终的 shuffleReadMetrics 中。
   * 在 Executor 心跳和 Task 结束时调用。
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics.toSeq)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get(IS_TESTING.key).map(_ => new LongAccumulator)


  import InternalAccumulator._

  // 指标名称到累加器的映射表，用于：
  // 1. 在 Driver 端根据名称查找对应的累加器进行合并
  // 2. 批量注册所有内部累加器
  // 3. 从 AccumulableInfo 列表重建 TaskMetrics
  // 使用 LinkedHashMap 保持插入顺序，便于调试和日志输出
  @transient private[spark] lazy val nameToAccums = LinkedHashMap(
    EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
    EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
    EXECUTOR_RUN_TIME -> _executorRunTime,
    EXECUTOR_CPU_TIME -> _executorCpuTime,
    RESULT_SIZE -> _resultSize,
    JVM_GC_TIME -> _jvmGCTime,
    RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
    MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
    DISK_BYTES_SPILLED -> _diskBytesSpilled,
    PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
    PEAK_ON_HEAP_EXECUTION_MEMORY -> _peakOnHeapExecutionMemory,
    PEAK_OFF_HEAP_EXECUTION_MEMORY -> _peakOffHeapExecutionMemory,
    UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
    shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
    shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
    shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
    shuffleRead.REMOTE_BYTES_READ_TO_DISK -> shuffleReadMetrics._remoteBytesReadToDisk,
    shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
    shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
    shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
    shuffleRead.CORRUPT_MERGED_BLOCK_CHUNKS -> shuffleReadMetrics._corruptMergedBlockChunks,
    shuffleRead.MERGED_FETCH_FALLBACK_COUNT -> shuffleReadMetrics._mergedFetchFallbackCount,
    shuffleRead.REMOTE_MERGED_BLOCKS_FETCHED -> shuffleReadMetrics._remoteMergedBlocksFetched,
    shuffleRead.LOCAL_MERGED_BLOCKS_FETCHED -> shuffleReadMetrics._localMergedBlocksFetched,
    shuffleRead.REMOTE_MERGED_CHUNKS_FETCHED -> shuffleReadMetrics._remoteMergedChunksFetched,
    shuffleRead.LOCAL_MERGED_CHUNKS_FETCHED -> shuffleReadMetrics._localMergedChunksFetched,
    shuffleRead.REMOTE_MERGED_BYTES_READ -> shuffleReadMetrics._remoteMergedBytesRead,
    shuffleRead.LOCAL_MERGED_BYTES_READ -> shuffleReadMetrics._localMergedBytesRead,
    shuffleRead.REMOTE_REQS_DURATION -> shuffleReadMetrics._remoteReqsDuration,
    shuffleRead.REMOTE_MERGED_REQS_DURATION -> shuffleReadMetrics._remoteMergedReqsDuration,
    shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
    shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
    shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
    input.BYTES_READ -> inputMetrics._bytesRead,
    input.RECORDS_READ -> inputMetrics._recordsRead,
    output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
    output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
  ) ++ testAccum.map(TEST_ACCUM -> _)

  @transient private[spark] lazy val internalAccums: Seq[AccumulatorV2[_, _]] =
    nameToAccums.values.toIndexedSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  /**
   * 将所有内部累加器注册到 SparkContext。
   * 注册后累加器才能正确地将值从 Executor 传递到 Driver。
   * countFailedValues=true 表示即使 Task 失败也要统计其指标值。
   */
  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)
    }
  }

  /**
   * External accumulators registered with this task.
   *
   * 外部累加器列表：用户通过 SparkContext.register() 注册的自定义累加器。
   * 与内部累加器不同，这些累加器由用户代码定义和更新。
   */
  @transient private[spark] lazy val _externalAccums = new ArrayBuffer[AccumulatorV2[_, _]]

  /**
   * Perform an `op` conversion on the `_externalAccums` within the read lock.
   *
   * Note `op` is expected to not modify the `_externalAccums` and not being
   * lazy evaluation for safe concern since `ArrayBuffer` is lazily evaluated.
   * And we intentionally keeps `_externalAccums` as mutable instead of converting
   * it to immutable for the performance concern.
   *
   * 在读锁保护下对外部累加器列表执行只读操作。
   * 使用读锁允许多个线程并发读取，提高性能。
   */
  private[spark] def withExternalAccums[T](op: ArrayBuffer[AccumulatorV2[_, _]] => T)
    : T = withReadLock {
    op(_externalAccums)
  }

  // 在读锁保护下执行操作
  private def withReadLock[B](fn: => B): B = {
    readLock.lock()
    try {
      fn
    } finally {
      readLock.unlock()
    }
  }

  // 在写锁保护下执行操作
  private def withWriteLock[B](fn: => B): B = {
    writeLock.lock()
    try {
      fn
    } finally {
      writeLock.unlock()
    }
  }

  /**
   * 注册外部累加器，在写锁保护下添加到列表中
   */
  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = withWriteLock {
    _externalAccums += a
  }

  /**
   * 获取所有累加器（内部 + 外部），在读锁保护下执行
   */
  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = withReadLock {
    internalAccums ++ _externalAccums
  }

  /**
   * 获取所有非零值的内部累加器。
   * 用于优化网络传输：只发送有实际数据的累加器。
   * 特例：_resultSize 即使为零也要发送，因为其值会在 Driver 端更新。
   */
  private[spark] def nonZeroInternalAccums(): Seq[AccumulatorV2[_, _]] = {
    // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
    // value will be updated at driver side.
    internalAccums.filter(a => !a.isZero || a == _resultSize)
  }
}


/**
 * TaskMetrics 伴生对象，提供工厂方法用于创建和重建 TaskMetrics 实例。
 *
 * 主要用途：
 * 1. 创建空的 TaskMetrics（用于测试或特殊场景）
 * 2. 从 Executor 发来的 AccumulableInfo 列表重建 TaskMetrics（Driver 端使用）
 * 3. 从 Accumulator 列表重建 TaskMetrics
 */
private[spark] object TaskMetrics extends Logging {
  import InternalAccumulator._

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   *
   * 创建一个空的 TaskMetrics，其累加器不会注册到 SparkContext。
   * 主要用于测试场景或 Driver 端重建指标对象时使用。
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
    }
    tm
  }

  /**
   * 创建并注册 TaskMetrics 的所有内部累加器
   */
  def registered: TaskMetrics = {
    val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   *
   * 从 AccumulableInfo 列表重建 TaskMetrics（仅在 Driver 端调用）。
   * AccumulableInfo 是 Executor 通过心跳或任务结果发送的指标快照。
   * 此方法将这些快照值填充到新的 TaskMetrics 对象中，用于指标展示。
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    // 只处理有名称和更新值的指标
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        // Block 状态是集合类型，需要特殊处理
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        // 其他指标都是 Long 类型
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   *
   * 从累加器列表重建 TaskMetrics（仅在 Driver 端调用）。
   * 与 fromAccumulatorInfos 不同，此方法接收的是完整的 Accumulator 对象，
   * 会保留累加器的元数据并执行合并操作。
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    for (acc <- accums) {
      val name = acc.name
      if (name.isDefined && tm.nameToAccums.contains(name.get)) {
        // 内部累加器：合并值到对应的指标
        val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
        tmAcc.metadata = acc.metadata
        tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        // 外部累加器：添加到外部累加器列表
        tm._externalAccums += acc
      }
    }
    tm
  }
}

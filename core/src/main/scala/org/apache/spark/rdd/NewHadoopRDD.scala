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

package org.apache.spark.rdd

import java.io.{FileNotFoundException, IOException}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileInputFormat, FileSplit, InvalidInputException}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.security.AccessControlException

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.rdd.NewHadoopRDD.NewHadoopMapPartitionsWithSplitRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * NewHadoopRDD 的分区实现类
 * 
 * 封装了新 MapReduce API 的 InputSplit，与 HadoopPartition 不同的是：
 * - HadoopPartition 使用旧版 mapred API 的 InputSplit
 * - NewHadoopPartition 使用新版 mapreduce API 的 InputSplit
 * 
 * @param rddId 所属 RDD 的唯一标识
 * @param index 分区索引
 * @param rawSplit 原始的 InputSplit，需要同时实现 Writable 接口以支持序列化
 */
private[spark] class NewHadoopPartition(
    rddId: Int,
    val index: Int,
    rawSplit: InputSplit with Writable)
  extends Partition {

  // 将 InputSplit 包装成可序列化的形式，便于在网络上传输给各个 Executor
  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * 基于新版 MapReduce API（org.apache.hadoop.mapreduce）读取 Hadoop 数据源的 RDD 实现
 * 
 * 【新旧 API 对比】
 * - 旧版 API (mapred)：HadoopRDD 使用，InputFormat/RecordReader 接口较简单
 * - 新版 API (mapreduce)：本类使用，更灵活的上下文对象和更好的扩展性
 * 
 * 【主要特性】
 * - 支持所有实现了新版 InputFormat 接口的数据源（HDFS、HBase、S3、自定义格式等）
 * - 自动处理损坏文件和缺失文件（通过配置控制）
 * - 支持配置广播以减少大规模作业的内存开销
 * - 自动收集输入指标（字节数、记录数）
 * 
 * @param sc SparkContext 实例
 * @param inputFormatClass 数据格式类，如 TextInputFormat、SequenceFileInputFormat 等
 * @param keyClass 键的类型
 * @param valueClass 值的类型
 * @param ignoreCorruptFiles 是否忽略损坏的文件
 * @param ignoreMissingFiles 是否忽略缺失的文件
 *
 * @note 不建议直接实例化此类，应该使用 SparkContext.newAPIHadoopRDD() 方法
 */
@DeveloperApi
class NewHadoopRDD[K, V](
    sc : SparkContext,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    @transient private val _conf: Configuration,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean)
  extends RDD[(K, V)](sc, Nil) with Logging {

  def this(
      sc : SparkContext,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      _conf: Configuration) = {
    this(
      sc,
      inputFormatClass,
      keyClass,
      valueClass,
      _conf,
      ignoreCorruptFiles = sc.conf.get(IGNORE_CORRUPT_FILES),
      ignoreMissingFiles = sc.conf.get(IGNORE_MISSING_FILES))
  }


  // Hadoop Configuration 对象通常约 10KB，较大，因此使用广播变量分发
  // 避免每个 Task 都携带一份完整的 Configuration，减少序列化开销
  private val confBroadcast = SerializableConfiguration.broadcast(sc, _conf)

  // 作业跟踪 ID，格式为 yyyyMMddHHmmss，用于生成唯一的 TaskAttemptID
  private val jobTrackerId: String = {
    val dateTimeFormatter =
      DateTimeFormatter
        .ofPattern("yyyyMMddHHmmss", Locale.US)
        .withZone(ZoneId.systemDefault())
    dateTimeFormatter.format(Instant.now())
  }

  // Hadoop 作业 ID，用于创建 TaskAttemptID 和 JobContext
  @transient protected val jobId = new JobID(jobTrackerId, id)

  // 是否克隆 JobConf
  // 默认关闭，因为克隆开销较大；但在多线程并发修改配置时（SPARK-2546）需要开启
  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  // 是否忽略空的 InputSplit，默认不忽略
  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  /**
   * 获取 Hadoop Configuration 对象
   * 
   * 如果配置了 cloneConf=true，会返回配置的克隆副本以避免线程安全问题
   * 否则直接返回广播变量中的配置对象
   */
  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration 对象非线程安全，多作业并发读写时可能导致问题
      // （参见 SPARK-2546、SPARK-10611）
      // 解决方案：克隆 Configuration 对象，但克隆开销较大
      // 因此默认关闭，仅在遇到线程安全问题时通过配置开启
      NewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        // 传入的 Configuration 实际上是 JobConf 并可能包含凭证
        // 为了保留凭证，需要创建 JobConf 而不是普通 Configuration
        if (conf.isInstanceOf[JobConf]) {
          new JobConf(conf)
        } else {
          new Configuration(conf)
        }
      }
    } else {
      conf
    }
  }

  /**
   * 获取所有分区
   * 
   * 【执行流程】
   * 1. 实例化 InputFormat 对象
   * 2. 调用 InputFormat.getSplits() 获取所有数据切片
   * 3. 过滤空切片（如果配置了 ignoreEmptySplits）
   * 4. 将每个切片封装为 NewHadoopPartition
   * 
   * 【性能优化】
   * - 设置 LIST_STATUS_NUM_THREADS 并行化文件列表操作，加速大目录扫描
   * - 对于单个大文件，会发出警告建议增加分区数
   */
  override def getPartitions: Array[Partition] = {
    // 实例化 InputFormat（如 TextInputFormat、SequenceFileInputFormat）
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    // 设置文件列表扫描的并行度，对于包含大量文件的目录非常重要
    _conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    // 如果 InputFormat 实现了 Configurable 接口，设置配置
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(_conf)
      case _ =>
    }
    try {
      // 获取所有数据切片
      val allRowSplits = inputFormat.getSplits(new JobContextImpl(_conf, jobId)).asScala
      // 根据配置决定是否过滤空切片
      val rawSplits = if (ignoreEmptySplits) {
        allRowSplits.filter(_.getLength > 0)
      } else {
        allRowSplits
      }

      // 单个大文件警告：如果只有一个切片且文件很大，建议用户增加分区数
      if (rawSplits.length == 1 && rawSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = rawSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        // 文件大小超过阈值时发出警告
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(_conf)
          // 检查文件是否可切分：可切分文件可以增加分区数；不可切分文件只能单分区
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(log"Loading one large file ${MDC(PATH, path.toString)} " +
              log"with only one partition, " +
              log"we can increase partition numbers for improving performance.")
          } else {
            // 不可切分的压缩格式（如 gzip）只能单分区处理
            logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} " +
              log"with only one " +
              log"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }

      // 将每个 InputSplit 封装为 NewHadoopPartition
      val result = new Array[Partition](rawSplits.size)
      for (i <- rawSplits.indices) {
        result(i) =
            new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
      }
      result
    } catch {
      // 如果配置了忽略缺失文件，输入路径不存在时返回空分区数组
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(log"${MDC(PATH, _conf.get(FileInputFormat.INPUT_DIR))} " +
          log"doesn't exist and no partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }

  /**
   * 计算指定分区的数据
   * 
   * 【核心流程】
   * 1. 创建 RecordReader 实例
   * 2. 初始化 RecordReader（打开文件、定位到切片起始位置）
   * 3. 迭代读取键值对数据
   * 4. 任务完成或取消时关闭资源
   * 
   * 【指标收集】
   * - 自动统计读取的字节数和记录数
   * - 通过 Hadoop FileSystem 的线程本地统计获取精确字节数
   * 
   * 【错误处理】
   * - ignoreMissingFiles：文件不存在时跳过
   * - ignoreCorruptFiles：文件损坏时跳过（但 FileNotFoundException 不跳过）
   * - 访问权限异常和块缺失异常始终抛出
   */
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      private val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo(log"Task (TID ${MDC(TASK_ID, context.taskAttemptId())}) input split: " +
        log"${MDC(INPUT_SPLIT, split.serializableHadoopSplit)}")
      private val conf = getConf

      // 输入指标统计器
      private val inputMetrics = context.taskMetrics().inputMetrics
      // 记录分区开始时已读取的字节数，用于计算增量
      private val existingBytesRead = inputMetrics.bytesRead

      // 设置 InputFileBlockHolder，供 input_file_name() 等函数使用
      // 记录当前读取文件的路径、起始位置和长度
      split.serializableHadoopSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // 获取读取字节数的回调函数
      // 必须在创建 RecordReader 之前获取，因为构造函数可能会读取数据
      private val getBytesReadCallback: Option[() => Long] =
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
          case _ => None
        }

      // 更新已读字节数
      // 从 Hadoop FileSystem 的线程本地统计中获取，这是最准确的方式
      // 注意：coalesce 操作可能在同一线程中处理多个分区（SPARK-13071）
      // 因此需要记录 existingBytesRead 并计算增量
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      // 创建 InputFormat 实例（新版 MapReduce API）
      private val format = inputFormatClass.getConstructor().newInstance()
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      // 创建 TaskAttemptID，用于 Hadoop 框架的任务跟踪
      private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      private var finished = false
      // 创建并初始化 RecordReader
      private var reader =
        try {
          // createResourceUninterruptiblyIfInTaskThread：确保资源创建不会被中断
          // 即使任务被取消，也要完成资源创建以便后续正确清理
          Utils.createResourceUninterruptiblyIfInTaskThread {
            Utils.tryInitializeResource(
              format.createRecordReader(split.serializableHadoopSplit.value, hadoopAttemptContext)
            ) { reader =>
              // 初始化 RecordReader：打开文件、定位到切片起始位置
              reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
              reader
            }
          }
        } catch {
          // 文件不存在且配置了忽略
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(log"Skipped missing file: ${MDC(PATH, split.serializableHadoopSplit)}", e)
            finished = true
            null
          // 文件不存在但未配置忽略，则抛出异常
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          // 访问权限异常和块缺失异常始终抛出（不可忽略）
          case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
          // 文件损坏且配置了忽略
          case e: IOException if ignoreCorruptFiles =>
            logWarning(
              log"Skipped the rest content in the corrupted file: " +
                log"${MDC(PATH, split.serializableHadoopSplit)}",
              e)
            finished = true
            null
        }

      // 注册任务完成回调，确保资源被正确释放
      // 即使任务被取消或失败，也会执行清理逻辑
      context.addTaskCompletionListener[Unit] { context =>
        // 关闭前更新字节数统计，确保最后的读取量被记录
        updateBytesRead()
        close()
      }

      // 是否已经读取了下一个键值对
      private var havePair = false

      /**
       * 检查是否还有下一条记录
       * 使用懒加载模式：只有在 havePair=false 时才实际读取下一条
       */
      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            // 调用 RecordReader 的 nextKeyValue 读取下一条记录
            finished = !reader.nextKeyValue
          } catch {
            // 与初始化时相同的错误处理逻辑
            case e: FileNotFoundException if ignoreMissingFiles =>
              logWarning(log"Skipped missing file: ${MDC(PATH, split.serializableHadoopSplit)}", e)
              finished = true
            case e: FileNotFoundException if !ignoreMissingFiles => throw e
            case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
            case e: IOException if ignoreCorruptFiles =>
              logWarning(
                log"Skipped the rest content in the corrupted file: " +
                  log"${MDC(PATH, split.serializableHadoopSplit)}",
                e)
              finished = true
          }
          if (finished) {
            // 尽早释放资源：对于读取多个文件的任务，及时释放可以减少资源占用
            // 虽然任务完成时也会调用 close，但提前释放更好
            close()
          }
          havePair = !finished
        }
        !finished
      }

      /**
       * 返回当前记录并移动到下一条
       * 每读取一定数量的记录后更新字节数统计
       */
      override def next(): (K, V) = {
        if (!hasNext) {
          throw SparkCoreErrors.endOfStreamError()
        }
        havePair = false
        // 更新记录数统计
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        // 每读取一定数量的记录后更新字节数统计（减少更新开销）
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        // 返回当前记录的键值对
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      /**
       * 关闭 RecordReader 并释放资源
       * 包含完整的错误处理和字节数统计补偿逻辑
       */
      private def close(): Unit = {
        if (reader != null) {
          // 清除 InputFileBlockHolder
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              // JVM 正在关闭时不记录警告（避免在关闭过程中产生额外日志）
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          // 最后一次更新字节数统计
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
                     split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
            // 如果无法从 FS 统计获取字节数，退回使用切片大小（可能不精确）
            try {
              inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    // 使用 InterruptibleIterator 包装，支持任务取消时中断迭代
    new InterruptibleIterator(context, iter)
  }

  /**
   * 在每个分区上执行函数，同时提供该分区对应的 InputSplit 信息
   * 
   * 这是一个高级 API，允许用户根据 InputSplit 的元数据（如文件名、偏移量）
   * 来自定义处理逻辑
   * 
   * @param f 转换函数，接收 (InputSplit, Iterator[(K, V)]) 返回 Iterator[U]
   * @param preservesPartitioning 是否保持分区器
   */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  /**
   * 获取分区的首选位置
   * 
   * 返回 InputSplit 中记录的数据本地化位置
   * 优先使用带拓扑信息的位置（如机架感知），否则使用普通主机名
   * 过滤掉 "localhost"（本地模式的占位符）
   */
  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.convertSplitLocationInfo(split.getLocationInfo)
    locs.getOrElse(split.getLocations.filter(_ != "localhost").toImmutableArraySeq)
  }

  /**
   * 持久化 RDD
   * 
   * 警告：以反序列化方式缓存 NewHadoopRDD 可能导致意外行为
   * 因为 Hadoop 的 RecordReader 会复用同一个 Writable 对象来读取所有记录
   * 如果需要缓存，建议先使用 map 转换创建记录的副本
   */
  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

}

/**
 * NewHadoopRDD 的伴生对象
 * 
 * 包含配置实例化的同步锁和内部辅助类
 */
private[spark] object NewHadoopRDD {
  /**
   * Configuration 的构造函数不是线程安全的（参见 SPARK-1097 和 HADOOP-10456）
   * 因此在调用 new Configuration() 之前需要在此锁上同步
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /**
   * 类似于 MapPartitionsRDD，但在转换函数中传入 InputSplit 而不是分区索引
   * 
   * 允许用户根据 InputSplit 的元数据自定义处理逻辑
   * 例如：根据文件名进行不同的解析策略
   */
  private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    // 如果指定了保持分区，则继承父 RDD 的分区器
    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    // 直接使用父 RDD 的分区
    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[NewHadoopPartition]
      // 从分区中提取 InputSplit，传递给用户函数
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
}

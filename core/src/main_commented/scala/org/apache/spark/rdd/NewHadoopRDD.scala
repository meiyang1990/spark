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
 * 文件级别注释：基于Hadoop新版MapReduce API的输入RDD实现，支持从Hadoop兼容存储系统读取数据
 * 核心职责：封装新版MapReduce API的输入分片和读取逻辑，为上层计算提供分布式数据抽象
 */

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


  // Hadoop Configuration 对象较大，使用广播变量分发避免每个Task携带副本，减少序列化和内存开销
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

  // 是否克隆JobConf，默认关闭，多线程并发修改配置时需要开启
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
      NewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        // 保留原有凭证，根据类型创建对应配置实例
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
   * 调用InputFormat获取输入分片，封装为Spark分区对象，支持过滤空分片和大文件警告
   */
  override def getPartitions: Array[Partition] = {
    // 实例化InputFormat对象
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    // 设置文件列表扫描并行度，默认使用CPU核数
    _conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    // 如果InputFormat支持配置注入，设置当前配置
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(_conf)
      case _ =>
    }
    try {
      // 调用Hadoop API获取所有输入分片
      val allRowSplits = inputFormat.getSplits(new JobContextImpl(_conf, jobId)).asScala
      // 根据配置过滤空分片
      val rawSplits = if (ignoreEmptySplits) {
        allRowSplits.filter(_.getLength > 0)
      } else {
        allRowSplits
      }

      // 单个大文件场景检查，发出性能优化建议
      if (rawSplits.length == 1 && rawSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = rawSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        // 文件大小超过警告阈值时输出提示
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(_conf)
          // 可切分文件提示增加分区提升性能
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(log"Loading one large file ${MDC(PATH, path.toString)} " +
              log"with only one partition, " +
              log"we can increase partition numbers for improving performance.")
          } else {
            // 不可切分压缩文件说明只能单分区处理
            logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} " +
              log"with only one " +
              log"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }

      // 将每个Hadoop InputSplit封装为Spark的NewHadoopPartition
      val result = new Array[Partition](rawSplits.size)
      for (i <- rawSplits.indices) {
        result(i) =
            new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
      }
      result
    } catch {
      // 忽略缺失文件配置：路径不存在时返回空分区
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(log"${MDC(PATH, _conf.get(FileInputFormat.INPUT_DIR))} " +
          log"doesn't exist and no partitions returned from this path.", e)
        Array.empty[Partition]
    }
  }

  /**
   * 计算指定分区的数据，迭代读取Hadoop输入分片的键值对
   * 处理错误异常，统计输入指标，确保资源正确释放
   */
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      private val split = theSplit.asInstanceOf[NewHadoopPartition]
      logInfo(log"Task (TID ${MDC(TASK_ID, context.taskAttemptId())}) input split: " +
        log"${MDC(INPUT_SPLIT, split.serializableHadoopSplit)}")
      private val conf = getConf

      // 获取任务输入指标统计器
      private val inputMetrics = context.taskMetrics().inputMetrics
      // 记录分区计算开始前已读取字节数，用于计算增量
      private val existingBytesRead = inputMetrics.bytesRead

      // 设置当前输入文件块信息，供input_file_name等函数获取当前文件信息
      split.serializableHadoopSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // 获取Hadoop本地线程读取字节数统计回调，仅对文件分片有效
      private val getBytesReadCallback: Option[() => Long] =
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
          case _ => None
        }

      // 更新任务输入字节数统计
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      // 实例化InputFormat
      private val format = inputFormatClass.getConstructor().newInstance()
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      // 创建Hadoop任务尝试ID
      private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      private var finished = false
      // 创建并初始化RecordReader，处理各种异常场景
      private var reader =
        try {
          Utils.createResourceUninterruptiblyIfInTaskThread {
            Utils.tryInitializeResource(
              format.createRecordReader(split.serializableHadoopSplit.value, hadoopAttemptContext)
            ) { reader =>
              // 初始化RecordReader，定位到分片起始位置
              reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
              reader
            }
          }
        } catch {
          // 文件不存在且配置忽略，跳过该分片
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(log"Skipped missing file: ${MDC(PATH, split.serializableHadoopSplit)}", e)
            finished = true
            null
          // 文件不存在未配置忽略，抛出异常
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          // 权限错误和块缺失错误不忽略，直接抛出
          case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
          // 文件损坏且配置忽略，跳过该分片
          case e: IOException if ignoreCorruptFiles =>
            logWarning(
              log"Skipped the rest content in the corrupted file: " +
                log"${MDC(PATH, split.serializableHadoopSplit)}",
              e)
            finished = true
            null
        }

      // 注册任务完成回调，确保资源释放和统计更新
      context.addTaskCompletionListener[Unit] { context =>
        updateBytesRead()
        close()
      }

      // 是否已读取下一条记录
      private var havePair = false

      /**
       * 检查是否还有下一条记录，处理读取过程中的异常
       */
      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            // 读取下一条键值对
            finished = !reader.nextKeyValue
          } catch {
            // 与初始化阶段一致的异常处理逻辑
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
            // 提前关闭释放资源
            close()
          }
          havePair = !finished
        }
        !finished
      }

      /**
       * 返回当前键值对，更新输入统计信息
       */
      override def next(): (K, V) = {
        if (!hasNext) {
          throw SparkCoreErrors.endOfStreamError()
        }
        havePair = false
        // 增加记录数统计
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        // 每读取一定数量记录更新一次字节数统计，平衡性能和准确性
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        // 返回当前键值对
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      /**
       * 关闭RecordReader，释放资源，更新最终输入统计
       */
      private def close(): Unit = {
        if (reader != null) {
          // 清空当前文件块信息
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              // JVM关闭过程中不输出警告
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
            // 无法获取FS统计时，回退到使用分片长度估算
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
    // 包装为可中断迭代器，支持任务取消中断
    new InterruptibleIterator(context, iter)
  }
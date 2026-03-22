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
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.BlockMissingException
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.mapreduce.TaskType
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.security.AccessControlException
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.rdd.HadoopRDD.HadoopMapPartitionsWithSplitRDD
import org.apache.spark.scheduler.{HDFSCacheTaskLocation, HostTaskLocation}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{NextIterator, SerializableConfiguration, ShutdownHookManager, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * 文件级注释：HadoopRDD实现类，提供基于Hadoop旧MapReduce API读取Hadoop数据源的能力，
 * 是Spark从HDFS、S3等存储系统读取数据的核心RDD实现
 */

/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 *
 * HadoopPartition 是 HadoopRDD 的分区类，封装了 Hadoop 的 InputSplit。
 * 每个 InputSplit 对应 HDFS 文件的一个 Block（或多个小文件合并）
 */
private[spark] class HadoopPartition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  // 包装 InputSplit 使其可序列化
  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)

  /**
   * Get any environment variables that should be added to the users environment when running pipes
   * 获取 pipe 操作需要的环境变量（如输入文件路径）
   * @return a Map with the environment variables and corresponding values, it could be empty
   */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = inputSplit.value match {
      case is: FileSplit =>
        // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
        // since it's not removed yet
        Map("map_input_file" -> is.getPath().toString(),
          "mapreduce_map_input_file" -> is.getPath().toString())
      case _ =>
        Map()
    }
    envVars
  }
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * HadoopRDD 是 Spark 读取 Hadoop 数据源（HDFS、HBase、S3 等）的核心 RDD。
 * 使用的是旧版 MapReduce API（org.apache.hadoop.mapred）。
 *
 * 工作原理：
 *   1. 通过 InputFormat.getSplits() 获取输入分片（每个分片通常对应一个 HDFS Block）
 *   2. 每个分片成为一个 RDD 分区
 *   3. 通过 RecordReader 读取分片中的数据
 *
 * 数据本地化：
 *   HadoopRDD 会根据 InputSplit 的位置信息返回首选计算位置，
 *   调度器会尽量将任务调度到数据所在的节点，减少网络传输
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *   variable references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *   Otherwise, a new JobConf will be created on each executor using the enclosed Configuration.
 *   广播的 Hadoop 配置（避免每个任务都序列化一份完整配置）
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
 *     creates.
 *     可选的 JobConf 初始化函数
 * @param inputFormatClass Storage format of the data to be read.
 *     输入格式类（如 TextInputFormat、SequenceFileInputFormat）
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
 *     最小分区数
 * @param ignoreCorruptFiles Whether to ignore corrupt files.
 *     是否忽略损坏的文件
 * @param ignoreMissingFiles Whether to ignore missing files.
 *     是否忽略缺失的文件
 *
 * @note Instantiating this class directly is not recommended, please use
 * `org.apache.spark.SparkContext.hadoopRDD()`
 */
@DeveloperApi
class HadoopRDD[K, V](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int,
    ignoreCorruptFiles: Boolean,
    ignoreMissingFiles: Boolean)
  extends RDD[(K, V)](sc, Nil) with Logging {

  if (initLocalJobConfFuncOpt.isDefined) {
    sparkContext.clean(initLocalJobConfFuncOpt.get)
  }

  def this(
      sc: SparkContext,
      broadcastedConf: Broadcast[SerializableConfiguration],
      initLocalJobConfFuncOpt: Option[JobConf => Unit],
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int) = {
    this(
      sc,
      broadcastedConf,
      initLocalJobConfFuncOpt,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions,
      ignoreCorruptFiles = sc.conf.get(IGNORE_CORRUPT_FILES),
      ignoreMissingFiles = sc.conf.get(IGNORE_MISSING_FILES)
    )
  }

  def this(
      sc: SparkContext,
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int) = {
    this(
      sc,
      SerializableConfiguration.broadcast(sc, conf),
      initLocalJobConfFuncOpt = None,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  protected val jobConfCacheKey: String = "rdd_%d_job_conf".format(id)

  protected val inputFormatCacheKey: String = "rdd_%d_input_format".format(id)

  // 用于构建JobTracker ID
  private val createTime = new Date()

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  private val ignoreEmptySplits = sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  /**
   * 获取 JobConf 配置对象，会在 Executor 上使用。
   * 为了避免 Configuration 的线程安全问题，可能需要克隆配置
   */
  // Returns a JobConf that will be used on executors to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf: Configuration = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      conf match {
        case jobConf: JobConf =>
          logDebug("Re-using user-broadcasted JobConf")
          jobConf
        case _ =>
          Option(HadoopRDD.getCachedMetadata(jobConfCacheKey))
            .map { conf =>
              logDebug("Re-using cached JobConf")
              conf.asInstanceOf[JobConf]
            }
            .getOrElse {
              // Create a JobConf that will be cached and used across this RDD's getJobConf()
              // calls in the local process. The local cache is accessed through
              // HadoopRDD.putCachedMetadata().
              // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
              // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
              // HADOOP-10456).
              HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
                logDebug("Creating new JobConf and caching it for later re-use")
                val newJobConf = new JobConf(conf)
                initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
                HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
                newJobConf
              }
            }
      }
    }
  }

  /**
   * 通过反射创建 InputFormat 实例
   */
  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = try {
    ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
  } catch {
    case r: RuntimeException
      if r.getCause != null && r.getCause.isInstanceOf[InstantiationException] =>
      throw new RuntimeException(s"Failed to instantiate ${inputFormatClass.getName}", r.getCause)
  }

  /**
   * 获取 RDD 的分区数组。
   * 通过 InputFormat.getSplits() 获取输入分片，每个分片对应一个分区。
   * 分区数取决于 HDFS 文件的 Block 数和 minPartitions 参数
   */
  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // 添加凭据，该方法可能在SparkContext初始化前被调用
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      // 根据配置过滤空分片
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      // 单个大文件警告提示，建议用户增加分区提升性能
      if (inputSplits.length == 1 && inputSplits(0).isInstanceOf[FileSplit]) {
        val fileSplit = inputSplits(0).asInstanceOf[FileSplit]
        val path = fileSplit.getPath
        if (fileSplit.getLength > conf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
          val codecFactory = new CompressionCodecFactory(jobConf)
          if (Utils.isFileSplittable(path, codecFactory)) {
            logWarning(log"Loading one large file ${MDC(PATH, path.toString)} " +
              log"with only one partition, " +
              log"we can increase partition numbers for improving performance.")
          } else {
            logWarning(log"Loading one large unsplittable file ${MDC(PATH, path.toString)} " +
              log"with only one " +
              log"partition, because the file is compressed by unsplittable compression codec.")
          }
        }
      }
      // 为每个分片创建HadoopPartition分区对象
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    } catch {
      // 忽略不存在的输入路径，返回空分区
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(log"${MDC(PATH, jobConf.get(FileInputFormat.INPUT_DIR))} " +
          log"doesn't exist and no partitions returned from this path.", e)
        Array.empty[Partition]
      // 路径不是文件类型，抛出异常提示用户
      case e: IOException if e.getMessage.startsWith("Not a file:") =>
        val path = e.getMessage.split(":").map(_.trim).apply(2)
        throw SparkCoreErrors.pathNotSupportedError(path)
    }
  }

  /**
   * 计算分区数据，这是 HadoopRDD 的核心方法。
   *
   * 执行流程：
   *   1. 获取分区对应的 InputSplit
   *   2. 创建 RecordReader 来读取数据
   *   3. 返回一个迭代器，逐条读取记录
   *   4. 任务完成时关闭 RecordReader 并更新输入指标
   *
   * 特殊处理：
   *   - 支持忽略损坏文件（ignoreCorruptFiles）
   *   - 支持忽略缺失文件（ignoreMissingFiles）
   *   - 统计读取的字节数和记录数
   */
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      private val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo(log"Task (TID ${MDC(TASK_ID, context.taskAttemptId())}) input split: " +
        log"${MDC(INPUT_SPLIT, split.inputSplit)}")
      private val jobConf = getJobConf()

      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // 设置当前输入分块信息，供后续任务使用
      split.inputSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      // 获取当前线程读取字节数的回调函数，用于输入指标统计
      private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
        case _: FileSplit | _: CombineFileSplit =>
          Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      /**
       * 更新输入字节数统计，避免覆盖同一线程中前序分区的统计结果
       */
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private var reader: RecordReader[K, V] = null
      private val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(
        HadoopRDD.DATE_TIME_FORMATTER.format(createTime.toInstant),
        context.stageId(), theSplit.index, context.attemptNumber(), jobConf)

      // 初始化RecordReader，处理各种异常场景
      reader =
        try {
          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
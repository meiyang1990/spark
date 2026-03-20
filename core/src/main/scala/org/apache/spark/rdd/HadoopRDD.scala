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

  // used to build JobTracker ID
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
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
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
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    } catch {
      case e: InvalidInputException if ignoreMissingFiles =>
        logWarning(log"${MDC(PATH, jobConf.get(FileInputFormat.INPUT_DIR))} " +
          log"doesn't exist and no partitions returned from this path.", e)
        Array.empty[Partition]
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

      // Sets InputFileBlockHolder for the file block's information
      split.inputSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
        case _: FileSplit | _: CombineFileSplit =>
          Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
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

      reader =
        try {
          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(log"Skipped missing file: ${MDC(PATH, split.inputSplit)}", e)
            finished = true
            null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(log"Skipped the rest content in the corrupted file: " +
              log"${MDC(PATH, split.inputSplit)}", e)
            finished = true
            null
        }
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener[Unit] { context =>
        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(log"Skipped missing file: ${MDC(PATH, split.inputSplit)}", e)
            finished = true
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles => throw e
          case e @ (_ : AccessControlException | _ : BlockMissingException) => throw e
          case e: IOException if ignoreCorruptFiles =>
            logWarning(log"Skipped the rest content in the corrupted file: " +
              log"${MDC(PATH, split.inputSplit)}", e)
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
                     split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new HadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  /**
   * 获取分区的首选计算位置，实现数据本地化。
   * 通过 InputSplit 的位置信息（HDFS Block 所在的 DataNode）来确定
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val hsplit = split.asInstanceOf[HadoopPartition].inputSplit.value
    val locs = hsplit match {
      case lsplit: InputSplitWithLocationInfo =>
        HadoopRDD.convertSplitLocationInfo(lsplit.getLocationInfo)
      case _ => None
    }
    locs.getOrElse(hsplit.getLocations.filter(_ != "localhost").toImmutableArraySeq)
  }

  /**
   * HadoopRDD 不支持 checkpoint，因为它直接从外部数据源读取
   */
  override def checkpoint(): Unit = {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  /**
   * 持久化时的警告：由于 Hadoop RecordReader 会复用对象，
   * 以反序列化形式缓存可能导致意外行为
   */
  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching HadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def getConf: Configuration = getJobConf()
}

/**
 * HadoopRDD 伴生对象，提供工具方法和缓存管理
 */
private[spark] object HadoopRDD extends Logging {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
   *
   * Hadoop Configuration 的构造函数不是线程安全的，需要使用锁来同步
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  private val DATE_TIME_FORMATTER =
    DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss", Locale.US)
      .withZone(ZoneId.systemDefault())

  /**
   * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
   * the local process.
   */
  def getCachedMetadata(key: String): AnyRef = SparkEnv.get.hadoopJobMetadata.get(key)

  private def putCachedMetadata(key: String, value: AnyRef): Unit =
    SparkEnv.get.hadoopJobMetadata.put(key, value)

  /** Add Hadoop configuration specific to a single partition and attempt. */
  def addLocalConfiguration(jobTrackerId: String, jobId: Int, splitId: Int, attemptId: Int,
                            conf: JobConf): Unit = {
    val jobID = new JobID(jobTrackerId, jobId)
    val taId = new TaskAttemptID(new TaskID(jobID, TaskType.MAP, splitId), attemptId)

    conf.set("mapreduce.task.id", taId.getTaskID.toString)
    conf.set("mapreduce.task.attempt.id", taId.toString)
    conf.setBoolean("mapreduce.task.ismap", true)
    conf.setInt("mapreduce.task.partition", splitId)
    conf.set("mapreduce.job.id", jobID.toString)
  }

  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class HadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[HadoopPartition]
      val inputSplit = partition.inputSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }

  private[spark] def convertSplitLocationInfo(
       infos: Array[SplitLocationInfo]): Option[Seq[String]] = {
    Option(infos).map(_.flatMap { loc =>
      val locationStr = loc.getLocation
      if (locationStr != null && locationStr != "localhost") {
        if (loc.isInMemory) {
          logDebug(s"Partition $locationStr is cached by Hadoop.")
          Some(HDFSCacheTaskLocation(locationStr).toString)
        } else {
          Some(HostTaskLocation(locationStr).toString)
        }
      } else {
        None
      }
    }.toImmutableArraySeq)
  }
}

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

package org.apache.spark.internal.io

import java.text.NumberFormat
import java.util.{Date, Locale, UUID}

import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.{JobContext => NewJobContext,
OutputFormat => NewOutputFormat, RecordWriter => NewRecordWriter,
TaskAttemptContext => NewTaskAttemptContext, TaskAttemptID => NewTaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.{TaskAttemptContextImpl => NewTaskAttemptContextImpl}

import org.apache.spark.{SerializableWritable, SparkConf, SparkException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{DURATION, JOB_ID, TASK_ATTEMPT_ID}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * Spark Hadoop写入工具类，使用Hadoop OutputFormat将RDD数据写入存储系统
 * 是Spark基于Hadoop生态进行数据输出的核心辅助模块
 */
private[spark]
object SparkHadoopWriter extends Logging {
  import SparkHadoopWriterUtils._

  /**
   * 将RDD数据通过Hadoop OutputFormat写入存储系统的主入口方法
   * 遵循两阶段提交协议：先执行所有任务写入，任务成功后再提交整个作业
   * 
   * @param rdd 待写入的键值对RDD
   * @param config Hadoop写入配置工具，封装不同Hadoop API版本的适配逻辑
   */
  def write[K, V: ClassTag](
      rdd: RDD[(K, V)],
      config: HadoopWriteConfigUtil[K, V]): Unit = {
    // 从RDD获取Spark上下文和作业ID
    val sparkContext = rdd.context
    val commitJobId = rdd.id

    // 创建作业ID和作业上下文，初始化输出格式
    val jobTrackerId = createJobTrackerID(new Date())
    val jobContext = config.createJobContext(jobTrackerId, commitJobId)
    config.initOutputFormat(jobContext)

    // 检查配置完整性，确保输出格式、键值类型已正确设置
    config.assertConf(jobContext, rdd.conf)

    // 写入唯一UUID，供提交器生成唯一标识，避免冲突
    jobContext.getConfiguration.set("spark.sql.sources.writeJobUUID",
      UUID.randomUUID.toString)

    // 创建文件提交协议，执行作业级初始化
    val committer = config.createCommitter(commitJobId)
    committer.setupJob(jobContext)

    // 执行分布式写入任务
    try {
      val ret = sparkContext.runJob(rdd, (context: TaskContext, iter: Iterator[(K, V)]) => {
        // 根据阶段尝试号和任务尝试号生成唯一尝试ID，用于Spark任务重试场景
        val attemptId = (context.stageAttemptNumber() << 16) | context.attemptNumber()

        executeTask(
          context = context,
          config = config,
          jobTrackerId = jobTrackerId,
          commitJobId = commitJobId,
          sparkPartitionId = context.partitionId(),
          sparkAttemptNumber = attemptId,
          committer = committer,
          iterator = iter)
      })

      logInfo(log"Start to commit write Job ${MDC(JOB_ID, jobContext.getJobID)}.")
      val (_, duration) = Utils
        .timeTakenMs { committer.commitJob(jobContext, ret.toImmutableArraySeq) }
      logInfo(log"Write Job ${MDC(JOB_ID, jobContext.getJobID)} committed." +
        log" Elapsed time: ${MDC(DURATION, duration)} ms.")
    } catch {
      case cause: Throwable =>
        logError(log"Aborting job ${MDC(JOB_ID, jobContext.getJobID)}.", cause)
        committer.abortJob(jobContext)
        throw new SparkException("Job aborted.", cause)
    }
  }

  /**
   * 在单个Executor任务中写入单个RDD分区数据
   * 
   * @param context Spark任务上下文
   * @param config Hadoop写入配置工具
   * @param jobTrackerId 作业跟踪ID
   * @param commitJobId 提交作业ID
   * @param sparkPartitionId RDD分区ID
   * @param sparkAttemptNumber Spark任务尝试号
   * @param committer 文件提交协议
   * @param iterator 分区数据迭代器
   * @return 任务提交结果消息
   */
  private def executeTask[K, V: ClassTag](
      context: TaskContext,
      config: HadoopWriteConfigUtil[K, V],
      jobTrackerId: String,
      commitJobId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[(K, V)]): TaskCommitMessage = {
    // 创建任务尝试上下文，执行任务级初始化
    val taskContext = config.createTaskAttemptContext(
      jobTrackerId, commitJobId, sparkPartitionId, sparkAttemptNumber)
    committer.setupTask(taskContext)

    // 初始化写入器
    config.initWriter(taskContext, sparkPartitionId)
    var recordsWritten = 0L

    // 初始化输出度量，必须在initWriter之后执行，因为initWriter会初始化FileSystem统计信息
    val (outputMetrics, callback) = initHadoopOutputMetrics(context)

    // 遍历分区所有记录写入
    try {
      val ret = Utils.tryWithSafeFinallyAndFailureCallbacks {
        while (iterator.hasNext) {
          val pair = iterator.next()
          config.write(pair)

          // 每隔一定记录数更新输出度量，减少性能开销
          maybeUpdateOutputMetrics(outputMetrics, callback, recordsWritten)
          recordsWritten += 1
        }

        config.closeWriter(taskContext)
        committer.commitTask(taskContext)
      }(catchBlock = {
        // 写入失败时释放资源，终止当前任务
        try {
          config.closeWriter(taskContext)
        } finally {
          committer.abortTask(taskContext)
          logError(log"Task ${MDC(TASK_ATTEMPT_ID, taskContext.getTaskAttemptID)} aborted.")
        }
      })

      // 最终更新输出度量指标
      outputMetrics.setBytesWritten(callback())
      outputMetrics.setRecordsWritten(recordsWritten)

      ret
    } catch {
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows", t)
    }
  }
}

/**
 * 适配旧版Hadoop mapred API的写入配置工具类
 * 从JobConf读取配置，负责创建输出格式、提交器和写入器
 * 兼容旧版Hadoop MapReduce API
 */
private[spark]
class HadoopMapRedWriteConfigUtil[K, V: ClassTag](conf: SerializableJobConf)
  extends HadoopWriteConfigUtil[K, V] with Logging {

  private var outputFormat: Class[_ <: OutputFormat[K, V]] = null
  private var writer: RecordWriter[K, V] = null

  private def getConf: JobConf = conf.value

  // --------------------------------------------------------------------------
  // 创建JobContext和TaskAttemptContext
  // --------------------------------------------------------------------------

  override def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext = {
    val jobAttemptId = new SerializableWritable(new JobID(jobTrackerId, jobId))
    new JobContextImpl(getConf, jobAttemptId.value)
  }

  override def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): NewTaskAttemptContext = {
    // 添加本地化配置，供当前任务读取
    HadoopRDD.addLocalConfiguration(jobTrackerId, jobId, splitId, taskAttemptId, conf.value)
    // 创建任务上下文
    val attemptId = new TaskAttemptID(jobTrackerId, jobId, TaskType.MAP, splitId, taskAttemptId)
    new TaskAttemptContextImpl(getConf, attemptId)
  }

  // --------------------------------------------------------------------------
  // 创建文件提交器
  // --------------------------------------------------------------------------

  override def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol = {
    // 添加本地化配置
    HadoopRDD.addLocalConfiguration("", 0, 0, 0, getConf)
    // 创建mapred API专用的提交协议
    FileCommitProtocol.instantiate(
      className = classOf[HadoopMapRedCommitProtocol].getName,
      jobId = jobId.toString,
      outputPath = getConf.get("mapred.output.dir")
    ).asInstanceOf[HadoopMapReduceCommitProtocol]
  }

  // --------------------------------------------------------------------------
  // 创建写入器
  // --------------------------------------------------------------------------

  override def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit = {
    // 初始化文件名格式化器，保证文件名对齐
    val numfmt = NumberFormat.getInstance(Locale.US)
    numfmt.setMinimumIntegerDigits(5)
    numfmt.setGroupingUsed(false)

    // 生成分区文件名和输出路径
    val outputName = "part-" + numfmt.format(splitId)
    val path = FileOutputFormat.getOutputPath(getConf)
    val fs: FileSystem = {
      if (path != null) {
        path.getFileSystem(getConf)
      } else {
        // scalastyle:off FileSystemGet
        FileSystem.get(getConf)
        // scalastyle:on FileSystemGet
      }
    }

    // 获取RecordWriter实例
    writer = getConf.getOutputFormat
      .getRecordWriter(fs, getConf, outputName, Reporter.NULL)
      .asInstanceOf[RecordWriter[K, V]]

    require(writer != null, "Unable to obtain RecordWriter")
  }

  override def write(pair: (K, V)): Unit = {
    require(writer != null, "Must call createWriter before write.")
    writer.write(pair._1, pair._2)
  }

  override def closeWriter(taskContext: NewTaskAttemptContext): Unit = {
    if (writer != null) {
      writer.close(Reporter.NULL)
    }
  }

  // --------------------------------------------------------------------------
  // 初始化输出格式
  // --------------------------------------------------------------------------

  override def initOutputFormat(jobContext: NewJobContext): Unit = {
    if (outputFormat == null) {
      outputFormat = getConf.getOutputFormat.getClass
        .asInstanceOf[Class[_ <: OutputFormat[K, V]]]
    }
  }

  private def getOutputFormat(): OutputFormat[K, V] = {
    require(outputFormat != null, "Must call initOutputFormat first.")

    outputFormat.getConstructor().newInstance()
  }

  // --------------------------------------------------------------------------
  // 验证Hadoop配置完整性
  // --------------------------------------------------------------------------

  override def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit = {
    val outputFormatInstance = getOutputFormat()
    val keyClass = getConf.getOutputKeyClass
    val valueClass = getConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    // 添加Hadoop凭证，支持安全认证
    SparkHadoopUtil.get.addCredentials(getConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    // 启用输出路径验证时，检查输出目录是否存在
    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(conf)) {
      // FileOutputFormat ignores the filesystem parameter
      // scalastyle:off FileSystemGet
      val ignoredFs = FileSystem.get(getConf)
      // scalastyle:on FileSystemGet
      getOutputFormat().checkOutputSpecs(ignoredFs, getConf)
    }
  }
}

/**
 * 适配新版Hadoop mapreduce API的写入配置工具类
 * 从Configuration读取配置，负责创建输出格式、提交器和写入器
 * 兼容新版Hadoop MapReduce API
 */
private[spark]
class HadoopMapReduceWriteConfigUtil[K, V: ClassTag](conf: SerializableConfiguration)
  extends HadoopWriteConfigUtil[K, V] with Logging {

  private var outputFormat: Class[_ <: NewOutputFormat[K, V]] = null
  private var writer: NewRecordWriter[K, V] = null

  private def getConf: Configuration = conf.value

  // --------------------------------------------------------------------------
  // 创建JobContext和TaskAttemptContext
  // --------------------------------------------------------------------------

  override def createJobContext(jobTrackerId: String, jobId: Int): NewJobContext = {
    val jobAttemptId = new NewTaskAttemptID(jobTrackerId, jobId, TaskType.MAP, 0, 0)
    new NewTaskAttemptContextImpl(getConf, jobAttemptId)
  }

  override def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): NewTaskAttemptContext = {
    val attemptId = new NewTaskAttemptID(
      jobTrackerId, jobId, TaskType.REDUCE, splitId, taskAttemptId)
    new NewTaskAttemptContextImpl(getConf, attemptId)
  }

  // --------------------------------------------------------------------------
  // 创建文件提交器
  // --------------------------------------------------------------------------

  override def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol = {
    // 创建mapreduce API专用的提交协议
    FileCommitProtocol.instantiate(
      className = classOf[HadoopMapReduceCommitProtocol].getName,
      jobId = jobId.toString,
      outputPath = getConf.get("mapreduce.output.fileoutputformat.outputdir")
    ).asInstanceOf[HadoopMapReduceCommitProtocol]
  }

  // --------------------------------------------------------------------------
  // 创建写入器
  // --------------------------------------------------------------------------

  override def initWriter(taskContext: NewTaskAttemptContext, splitId: Int): Unit = {
    val taskFormat = getOutputFormat()
    // 如果输出格式实现了Configurable接口，注入Hadoop配置
    taskFormat match {
      case c: Configurable => c.setConf(getConf)
      case _ => ()
    }

    // 获取RecordWriter实例
    writer = taskFormat.getRecordWriter(taskContext)
      .asInstanceOf[NewRecordWriter[K, V]]

    require(writer != null, "Unable to obtain RecordWriter")
  }

  override def write(pair: (K, V)): Unit = {
    require(writer != null, "Must call createWriter before write.")
    writer.write(pair._1, pair._2)
  }

  override def closeWriter(taskContext: NewTaskAttemptContext): Unit = {
    if (writer != null) {
      writer.close(taskContext)
      writer = null
    } else {
      logWarning("Writer has been closed.")
    }
  }

  // --------------------------------------------------------------------------
  // 初始化输出格式
  // --------------------------------------------------------------------------

  override def initOutputFormat(jobContext: NewJobContext): Unit = {
    if (outputFormat == null) {
      outputFormat = jobContext.getOutputFormatClass
        .asInstanceOf[Class[_ <: NewOutputFormat[K, V]]]
    }
  }

  private def getOutputFormat(): NewOutputFormat[K, V] = {
    require(outputFormat != null, "Must call initOutputFormat first.")

    outputFormat.getConstructor().newInstance()
  }

  // --------------------------------------------------------------------------
  // 验证Hadoop配置完整性
  // --------------------------------------------------------------------------

  override def assertConf(jobContext: NewJobContext, conf: SparkConf): Unit = {
    // 启用输出路径验证时，检查输出目录是否存在
    if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(conf)) {
      getOutputFormat().checkOutputSpecs(jobContext)
    }
  }
}
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

import java.io.IOException
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.mapred.SparkHadoopMapRedUtil

/**
 * 文件提交协议实现，基于Hadoop MapReduce API的底层OutputCommitter实现。
 * 与原生Hadoop OutputCommitter不同，本实现支持序列化，适配Spark分布式执行模型。
 * 主要负责Spark写作业的输出文件提交，支持动态分区覆盖写入场景。
 * 
 * @param jobId 作业或Stage的ID标识
 * @param path 作业输出最终路径，如果为null则提交器不执行实际操作
 * @param dynamicPartitionOverwrite 是否开启动态分区覆盖模式，开启后Spark会在运行时动态覆盖已有分区目录
 */
class HadoopMapReduceCommitProtocol(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._

  /** Hadoop OutputCommitter不可序列化，因此标记为瞬态 */
  @transient private var committer: OutputCommitter = _

  /**
   * 检查是否存在有效的输出路径用于提交文件。
   * 作业提交/终止都在Driver端执行，此时addedAbsPathFiles一定为null，必须检查输出路径是否合法。
   * 对于不写入分布式文件系统的提交器，path不一定需要是合法的Hadoop Path格式。
   */
  private val hasValidPath = Try { new Path(path) }.isSuccess

  /**
   * 跟踪当前Task写入的绝对路径输出文件，这些文件不在Hadoop OutputCommitter管理范围内，
   * 需要在作业提交阶段手动移动到最终位置。
   * 键为临时输出路径，值为最终目标输出路径。
   */
  @transient private var addedAbsPathFiles: mutable.Map[String, String] = null

  /**
   * 跟踪当前Task写入的默认路径下的分区路径，例如a=1/b=2。
   * 当开启dynamicPartitionOverwrite时，这些分区文件会先写入临时目录，作业结束后移动到最终目录。
   */
  @transient private var partitionPaths: mutable.Set[String] = null

  /**
   * 当前写作业的临时 staging 目录，用于处理绝对路径输出文件或动态分区覆盖写入场景的临时存储。
   */
  @transient protected lazy val stagingDir = getStagingDir(path, jobId)

  /**
   * 初始化并获取当前Task尝试对应的OutputCommitter实例
   * @param context Task尝试上下文
   * @return 初始化完成的OutputCommitter实例
   */
  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val format = context.getOutputFormatClass.getConstructor().newInstance()
    // 如果OutputFormat实现了Configurable接口，需要为其设置配置对象
    format match {
      case c: Configurable => c.setConf(context.getConfiguration)
      case _ => ()
    }
    format.getOutputCommitter(context)
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], spec: FileNameSpec): String = {
    val filename = getFilename(taskContext, spec)

    val stagingDir: Path = committer match {
      // FileOutputCommitter自带自己的工作临时路径
      case f: FileOutputCommitter =>
        if (dynamicPartitionOverwrite) {
          assert(dir.isDefined,
            "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
          partitionPaths += dir.get
        }
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }

    dir.map { d =>
      new Path(new Path(stagingDir, d), filename).toString
    }.getOrElse {
      new Path(stagingDir, filename).toString
    }
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, spec: FileNameSpec): String = {
    val filename = getFilename(taskContext, spec)
    val absOutputPath = new Path(absoluteDir, filename).toString

    // 使用UUID避免同一Task写入不同目录时的文件名冲突，比哈希更简单可靠
    val tmpOutputPath = new Path(stagingDir, UUID.randomUUID().toString() + "-" + filename).toString

    addedAbsPathFiles(tmpOutputPath) = absOutputPath
    tmpOutputPath
  }

  /**
   * 生成输出文件的标准文件名
   * @param taskContext Task尝试上下文
   * @param spec 文件名规格说明，包含前缀和后缀
   * @return 格式化后的标准文件名
   */
  protected def getFilename(taskContext: TaskAttemptContext, spec: FileNameSpec): String = {
    // 文件名格式类似：part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // %05d不会截断分片编号，超过100000个Task依然可以正常生成文件名，不会溢出
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val basename = taskContext.getConfiguration.get("mapreduce.output.basename", "part")
    f"${spec.prefix}$basename-$split%05d-$jobId${spec.suffix}"
  }

  /**
   * 作业级初始化，为整个作业创建并初始化OutputCommitter
   * @param jobContext 作业上下文
   */
  override def setupJob(jobContext: JobContext): Unit = {
    // 创建Hadoop标准作业和任务ID
    val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // 将ID信息写入作业配置，供底层Hadoop组件使用
    jobContext.getConfiguration.set("mapreduce.job.id", jobId.toString)
    jobContext.getConfiguration.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapreduce.task.ismap", true)
    jobContext.getConfiguration.setInt("mapreduce.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)
  }

  /**
   * 提交整个作业，汇总所有Task的提交结果，完成最终文件移动和清理
   * @param jobContext 作业上下文
   * @param taskCommits 所有Task提交返回的消息，包含需要移动的文件和分区信息
   */
  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)

    if (hasValidPath) {
      // 聚合所有Task的绝对路径文件映射和分区路径集合
      val (allAbsPathFiles, allPartitionPaths) =
        taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)

      val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
      logDebug(s"Committing files staged for absolute locations $filesToMove")
      // 获取所有绝对路径的父目录
      val absParentPaths = filesToMove.values.map(new Path(_).getParent).toSet
      // 动态覆盖模式下先删除已有目录
      if (dynamicPartitionOverwrite) {
        logDebug(s"Clean up absolute partition directories for overwriting: $absParentPaths")
        absParentPaths.foreach(fs.delete(_, true))
      }
      logDebug(s"Create absolute parent directories: $absParentPaths")
      // 创建父目录确保存在
      absParentPaths.foreach(fs.mkdirs)
      // 逐个将临时文件重命名到最终绝对路径
      for ((src, dst) <- filesToMove) {
        if (!fs.rename(new Path(src), new Path(dst))) {
          throw new IOException(s"Failed to rename $src to $dst when committing files staged for " +
            s"absolute locations")
        }
      }

      // 处理动态分区覆盖场景的分区文件移动
      if (dynamicPartitionOverwrite) {
        val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _)
        logDebug(s"Clean up default partition directories for overwriting: $partitionPaths")
        for (part <- partitionPaths) {
          val finalPartPath = new Path(path, part)
          // 删除已有分区目录，如果删除失败且父目录不存在则创建父目录
          if (!fs.delete(finalPartPath, true) && !fs.exists(finalPartPath.getParent)) {
            // 根据Hadoop FileSystem API规范，delete返回false仅表示目标不存在
            // 重命名要求父目录必须存在，因此如果父目录不存在需要提前创建
            fs.mkdirs(finalPartPath.getParent)
          }
          val stagingPartPath = new Path(stagingDir, part)
          // 将临时分区目录重命名到最终位置
          if (!fs.rename(stagingPartPath, finalPartPath)) {
            throw new IOException(s"Failed to rename $stagingPartPath to $finalPartPath when " +
              s"committing files staged for overwriting dynamic partitions")
          }
        }
      }

      // 删除整个临时staging目录
      fs.delete(stagingDir, true)
    }
  }

  /**
   * 终止作业，清理临时资源，所有IO异常仅记录日志不抛出，避免掩盖原始错误
   * @param jobContext 作业上下文
   */
  override def abortJob(jobContext: JobContext): Unit = {
    try {
      committer.abortJob(jobContext, JobStatus.State.FAILED)
    } catch {
      case e: IOException =>
        logWarning(log"Exception while aborting ${MDC(JOB_ID, jobContext.getJobID)}", e)
    }
    try {
      if (hasValidPath) {
        val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
        fs.delete(stagingDir, true)
      }
    } catch {
      case e: IOException =>
        logWarning(log"Exception while aborting ${MDC(JOB_ID, jobContext.getJobID)}", e)
    }
  }

  /**
   * Task级初始化，为当前Task初始化OutputCommitter和状态跟踪容器
   * @param taskContext Task尝试上下文
   */
  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    addedAbsPathFiles = mutable.Map[String, String]()
    partitionPaths = mutable.Set[String]()
  }

  /**
   * 提交当前Task，返回收集到的需要后续处理的文件和分区信息
   * @param taskContext Task尝试上下文
   * @return Task提交消息，包含绝对路径文件映射和分区路径集合
   */
  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    logTrace(s"Commit task ${attemptId}")
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    new TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
  }

  /**
   * 终止当前Task，清理临时资源，所有IO异常仅记录日志不抛出，避免掩盖原始错误
   * @param taskContext Task尝试上下文
   */
  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    try {
      committer.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning(log"Exception while aborting " +
          log"${MDC(TASK_ATTEMPT_ID, taskContext.getTaskAttemptID)}", e)
    }
    // 尽力清理已经创建的临时文件
    try {
      for ((src, _) <- addedAbsPathFiles) {
        val tmp = new Path(src)
        tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
      }
    } catch {
      case e: IOException =>
        logWarning(log"Exception while aborting " +
          log"${MDC(TASK_ATTEMPT_ID, taskContext.getTaskAttemptID)}", e)
    }
  }
}
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

import org.apache.hadoop.fs._
import org.apache.hadoop.mapreduce._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Unstable
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * 文件提交协议抽象基类，定义Spark作业输出结果的提交规范，控制任务和作业层面文件写入的原子性与一致性。
 * 
 * 核心设计要点：
 * 1. 实现类必须可序列化，因为驱动端创建的提交器实例会被序列化到执行器任务中使用
 * 2. 实现类需要提供指定参数的构造函数：(jobId: String, path: String) 或 (jobId: String, path: String, dynamicPartitionOverwrite: Boolean)
 * 3. 每个提交器实例只能对应一个Spark作业，不可跨作业复用
 *
 * 标准调用流程：
 * 1. 驱动端调用 setupJob 完成作业初始化
 * 2. 每个执行器任务先调用 setupTask，任务成功后调用 commitTask，失败调用 abortTask
 * 3. 所有任务成功完成后，驱动端调用 commitJob 完成整个作业提交；作业失败则调用 abortJob 清理
 *
 * @note 本类作为开放API供下游自定义实现，仍属于不稳定API，可能会在后续版本变更或移动
 */
@Unstable
abstract class FileCommitProtocol extends Logging {
  import FileCommitProtocol._

  /**
   * 初始化作业，必须在驱动端调用，所有其他方法调用前完成
   */
  def setupJob(jobContext: JobContext): Unit

  /**
   * 提交整个作业，所有任务写入成功后在驱动端调用
   */
  def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit

  /**
   * 终止作业，作业写入失败后在驱动端调用，负责清理已写入的临时文件
   *
   * 该方法尽力而为完成清理，不保证一定成功，因为驱动可能在调用前崩溃或被终止
   */
  def abortJob(jobContext: JobContext): Unit

  /**
   * 初始化任务，必须在执行端调用，所有任务相关方法调用前完成
   */
  def setupTask(taskContext: TaskAttemptContext): Unit

  /**
   * 通知提交协议创建新的输出文件，返回任务写入需要使用的临时文件完整路径，必须在执行器任务中调用
   *
   * 返回的临时文件可以位于任意路径，提交协议仅保证作业提交完成后，文件会移动到参数指定的最终位置
   *
   * 完整文件路径包含五个部分：
   *  1. 基础输出路径
   *  2. 基础路径下的子目录，用于分区划分
   *  3. 文件前缀，通常是包含任务ID的作业唯一标识
   *  4. 分桶ID
   *  5. 文件扩展名，例如 ".snappy.parquet"
   *
   * 参数 dir 对应第二部分，ext 对应第四和第五部分，其余部分由提交协议实现决定
   *
   * 重要提示：如果单个任务向同一目录写入多个文件，调用方需要保证 ext 包含唯一标识。提交协议仅保证不同任务写入的文件不会冲突
   */
  @deprecated("use newTaskTempFile(..., spec: FileNameSpec) instead", "3.3.0")
  def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    throw SparkException.mustOverrideOneMethodError("newTaskTempFile")
  }

  /**
   * 通知提交协议创建新的输出文件，返回任务写入需要使用的临时文件完整路径，必须在执行器任务中调用
   *
   * 返回的临时文件可以位于任意路径，提交协议仅保证作业提交完成后，文件会移动到参数指定的最终位置
   *
   * 参数 dir 指定基础输出路径下的子目录，用于分区划分；spec 指定文件名称信息，其余部分由提交协议实现决定
   *
   * 重要提示：如果单个任务向同一目录写入多个文件，调用方需要保证 spec 包含唯一标识。提交协议仅保证不同任务写入的文件不会冲突
   *
   * @since 3.2.0
   */
  def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String], spec: FileNameSpec): String = {
    if (spec.prefix.isEmpty) {
      newTaskTempFile(taskContext, dir, spec.suffix)
    } else {
      throw new UnsupportedOperationException(s"${getClass.getSimpleName}.newTaskTempFile does " +
        s"not support file name prefix: ${spec.prefix}")
    }
  }

  /**
   * 类似 newTaskTempFile，但允许文件提交到绝对输出路径，不同实现对一致性保证可能较弱
   *
   * 重要提示：如果单个任务向同一目录写入多个文件，调用方需要保证 ext 包含唯一标识。提交协议仅保证不同任务写入的文件不会冲突
   */
  @deprecated("use newTaskTempFileAbsPath(..., spec: FileNameSpec) instead", "3.3.0")
  def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw SparkException.mustOverrideOneMethodError("newTaskTempFileAbsPath")
  }

  /**
   * 类似 newTaskTempFile，但允许文件提交到绝对输出路径，不同实现对一致性保证可能较弱
   *
   * 参数 absoluteDir 指定文件最终的绝对目录；spec 指定文件名称信息，其余部分由提交协议实现决定
   *
   * 重要提示：如果单个任务向同一目录写入多个文件，调用方需要保证 spec 包含唯一标识。提交协议仅保证不同任务写入的文件不会冲突
   *
   * @since 3.2.0
   */
  def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext, absoluteDir: String, spec: FileNameSpec): String = {
    if (spec.prefix.isEmpty) {
      newTaskTempFileAbsPath(taskContext, absoluteDir, spec.suffix)
    } else {
      throw new UnsupportedOperationException(
        s"${getClass.getSimpleName}.newTaskTempFileAbsPath does not support file name prefix: " +
          s"${spec.prefix}")
    }
  }

  /**
   * 提交单个任务，任务写入成功后在执行器任务中调用，返回任务提交信息给驱动端
   */
  def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage

  /**
   * 终止单个任务，任务写入失败后在执行器任务中调用，负责清理该任务写入的临时文件
   *
   * 该方法尽力而为完成清理，不保证一定成功，因为执行器可能在调用前崩溃或被终止
   */
  def abortTask(taskContext: TaskAttemptContext): Unit

  /**
   * 指定需要随作业提交删除的文件，默认实现直接立即删除文件
   */
  def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    fs.delete(path, recursive)
  }

  /**
   * 任务提交后在驱动端调用，可用于在作业完成前处理任务提交消息。如果整个作业成功，这些消息会再次传递给 commitJob
   */
  def onTaskCommit(taskCommit: TaskCommitMessage): Unit = {
    logDebug(s"onTaskCommit($taskCommit)")
  }
}


/**
 * FileCommitProtocol 的伴生对象，提供文件提交协议实例创建、公共路径工具方法
 */
object FileCommitProtocol extends Logging {
  /**
   * 任务提交消息封装，用于执行器向驱动端传递任务提交结果信息，可序列化
   * @param obj 实际存放任务提交信息的对象
   */
  class TaskCommitMessage(val obj: Any) extends Serializable

  /**
   * 空任务提交消息单例，用于不需要传递额外提交信息的场景
   */
  object EmptyTaskCommitMessage extends TaskCommitMessage(null)

  /**
   * 根据类名反射实例化FileCommitProtocol对象
   * @param className 提交协议实现类全限定名
   * @param jobId 作业ID
   * @param outputPath 输出基础路径
   * @param dynamicPartitionOverwrite 是否启用动态分区覆盖模式
   * @return 实例化好的文件提交协议对象
   */
  def instantiate(
      className: String,
      jobId: String,
      outputPath: String,
      dynamicPartitionOverwrite: Boolean = false): FileCommitProtocol = {

    logDebug(s"Creating committer $className; job $jobId; output=$outputPath;" +
      s" dynamic=$dynamicPartitionOverwrite")
    // 加载提交协议实现类
    val clazz = Utils.classForName[FileCommitProtocol](className)
    // 首先尝试三参数构造函数 (jobId: String, outputPath: String, dynamicPartitionOverwrite: Boolean)
    // 如果不存在，回退到两参数构造函数 (jobId: string, outputPath: String)
    try {
      val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String], classOf[Boolean])
      logDebug("Using (String, String, Boolean) constructor")
      ctor.newInstance(jobId, outputPath, dynamicPartitionOverwrite.asInstanceOf[java.lang.Boolean])
    } catch {
      case _: NoSuchMethodException =>
        logDebug("Falling back to (String, String) constructor")
        // 启用动态分区覆盖但没有对应构造函数，抛出异常
        require(!dynamicPartitionOverwrite,
          "Dynamic Partition Overwrite is enabled but" +
            s" the committer ${className} does not have the appropriate constructor")
        val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[String])
        ctor.newInstance(jobId, outputPath)
    }
  }

  /**
   * 获取作业暂存目录路径，用于存放临时文件
   * @param path 基础输出路径
   * @param jobId 作业ID
   * @return 暂存目录路径对象
   */
  def getStagingDir(path: String, jobId: String): Path = {
    new Path(path, ".spark-staging-" + jobId)
  }
}

/**
 * Spark输出文件名称规格定义，用于FileCommitProtocol构建完整文件路径
 *
 * @param prefix 文件名称前缀
 * @param suffix 文件名称后缀（通常包含分桶ID和扩展名）
 */
final case class FileNameSpec(prefix: String, suffix: String)
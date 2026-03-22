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

package org.apache.spark.errors

import java.io.{File, IOException}
import java.util.concurrent.TimeoutException

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException, SparkUnsupportedOperationException, TaskNotSerializableException}
import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.io.CompressionCodec.FALLBACK_COMPRESSION_CODEC
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.scheduler.{BarrierJobRunWithDynamicAllocationException, BarrierJobSlotsNumberCheckFailed, BarrierJobUnsupportedRDDChainException}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleManager}
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockNotFoundException, BlockSavedOnDecommissionedBlockManagerException, RDDBlockId, UnrecognizedBlockId}

/**
 * 存储Spark Core模块执行过程中抛出的各类异常的工厂对象，集中管理核心模块所有错误异常的构造
 */
private[spark] object SparkCoreErrors {
  /**
   * 构造Py4J服务端发生意外错误的异常
   * @param other 发生错误的对象实例
   * @return 构造好的异常对象
   */
  def unexpectedPy4JServerError(other: Object): Throwable = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_3000",
      messageParameters = Map("class" -> s"${other.getClass}")
    )
  }

  /**
   * 构造读取守护进程端口号时遇到EOF的异常
   * @param daemonModule 守护进程模块名称
   * @param daemonExitValue 守护进程退出码，可选参数
   * @return 构造好的异常对象
   */
  def eofExceptionWhileReadPortNumberError(
      daemonModule: String,
      daemonExitValue: Option[Int] = None): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3001",
      messageParameters = Map(
        "daemonModule" -> daemonModule,
        "additionalMessage" ->
          daemonExitValue.map(v => s" and terminated with code: $v.").getOrElse("")
      ), cause = null
    )
  }

  /**
   * 构造不支持的数据类型异常
   * @param other 不支持的数据对象
   * @return 构造好的异常对象
   */
  def unsupportedDataTypeError(other: Any): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3002",
      messageParameters = Map("other" -> s"$other"),
      cause = null
    )
  }

  /**
   * 构造RDD块不存在异常
   * @param blockId 块ID
   * @param id RDD分区ID
   * @return 构造好的异常对象
   */
  def rddBlockNotFoundError(blockId: BlockId, id: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3003",
      messageParameters = Map("blockId" -> s"$blockId", "id" -> s"$id"),
      cause = null
    )
  }

  /**
   * 构造块已被移除异常
   * @param string 块描述信息
   * @return 构造好的异常对象
   */
  def blockHaveBeenRemovedError(string: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3004",
      messageParameters = Map("string" -> string),
      cause = null
    )
  }

  /**
   * 构造空RDD或包含无穷值/NaN的RDD执行直方图计算异常
   * @return 构造好的异常对象
   */
  def histogramOnEmptyRDDOrContainingInfinityOrNaNError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3005")
  }

  /**
   * 构造空RDD操作异常
   * @return 构造好的异常对象
   */
  def emptyRDDError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3006")
  }

  /**
   * 构造路径不支持异常，目录在递归关闭时不被支持
   * @param path 输入路径字符串
   * @return 构造好的异常对象
   */
  def pathNotSupportedError(path: String): Throwable = {
    new IOException(s"Path: ${path} is a directory, which is not supported by the " +
      "record reader when `mapreduce.input.fileinputformat.input.dir.recursive` is false.")
  }

  /**
   * 构造检查点RDD块ID不存在异常
   * @param rddBlockId RDD块ID
   * @return 构造好的异常对象
   */
  def checkpointRDDBlockIdNotFoundError(rddBlockId: RDDBlockId): Throwable = {
    new SparkException(
      errorClass = "CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND",
      messageParameters = Map("rddBlockId" -> s"$rddBlockId"),
      cause = null
    )
  }

  /**
   * 构造流已结束异常
   * @return 构造好的异常对象
   */
  def endOfStreamError(): Throwable = {
    new java.util.NoSuchElementException("End of stream")
  }

  /**
   * 构造数组key无法使用map端聚合异常
   * @return 构造好的异常对象
   */
  def cannotUseMapSideCombiningWithArrayKeyError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3008", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造哈希分区器无法分区数组key异常
   * @return 构造好的异常对象
   */
  def hashPartitionerCannotPartitionArrayKeyError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3009", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造reduceByKeyLocally不支持数组key异常
   * @return 构造好的异常对象
   */
  def reduceByKeyLocallyNotSupportArrayKeysError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3010", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造RDD缺少SparkContext异常
   * @return 构造好的异常对象
   */
  def rddLacksSparkContextError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3011", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造无法修改存储级别异常
   * @return 构造好的异常对象
   */
  def cannotChangeStorageLevelError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3012")
  }

  /**
   * 构造只能对相同分区大小的RDD执行zip操作异常
   * @return 构造好的异常对象
   */
  def canOnlyZipRDDsWithSamePartitionSizeError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3013", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造空集合操作异常
   * @return 构造好的异常对象
   */
  def emptyCollectionError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3014")
  }

  /**
   * 构造countByValueApprox不支持数组类型异常
   * @return 构造好的异常对象
   */
  def countByValueApproxNotSupportArraysError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3015", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造SparkContext未设置检查点目录异常
   * @return 构造好的异常对象
   */
  def checkpointDirectoryHasNotBeenSetInSparkContextError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3016", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造无效检查点文件异常
   * @param path 检查点文件路径
   * @return 构造好的异常对象
   */
  def invalidCheckpointFileError(path: Path): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3017",
      messageParameters = Map("path" -> s"$path"),
      cause = null
    )
  }

  /**
   * 构造创建检查点路径失败异常
   * @param checkpointDirPath 检查点目录路径
   * @return 构造好的异常对象
   */
  def failToCreateCheckpointPathError(checkpointDirPath: Path): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3018",
      messageParameters = Map("checkpointDirPath" -> s"$checkpointDirPath"),
      cause = null
    )
  }

  /**
   * 构造检查点RDD与原始RDD分区数不一致异常
   * @param originalRDDId 原始RDD ID
   * @param originalRDDLength 原始RDD分区数
   * @param newRddId 检查点后RDD ID
   * @param newRddLength 检查点后RDD分区数
   * @return 构造好的异常对象
   */
  def checkpointRDDHasDifferentNumberOfPartitionsFromOriginalRDDError(
      originalRDDId: Int,
      originalRDDLength: Int,
      newRDDId: Int,
      newRDDLength: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3019",
      messageParameters = Map(
        "originalRDDId" -> s"$originalRDDId",
        "originalRDDLength" -> s"$originalRDDLength",
        "newRDDId" -> s"$newRDDId",
        "newRDDLength" -> s"$newRDDLength"
      ),
      cause = null
    )
  }

  /**
   * 构造检查点保存失败异常
   * @param task 任务ID
   * @param path 输出路径
   * @return 构造好的异常对象
   */
  def checkpointFailedToSaveError(task: Int, path: Path): Throwable = {
    new IOException("Checkpoint failed: failed to save output of task: " +
      s"$task and final output path does not exist: $path")
  }

  /**
   * 构造必须指定检查点目录异常
   * @return 构造好的异常对象
   */
  def mustSpecifyCheckpointDirError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3020", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造向Standalone调度器请求关闭执行器异常
   * @param e 原始异常
   * @return 构造好的异常对象
   */
  def askStandaloneSchedulerToShutDownExecutorsError(e: Exception): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3021", messageParameters = Map.empty, cause = e
    )
  }

  /**
   * 构造停止Standalone调度器Driver端点异常
   * @param e 原始异常
   * @return 构造好的异常对象
   */
  def stopStandaloneSchedulerDriverEndpointError(e: Exception): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3022", messageParameters = Map.empty, cause = e
    )
  }

  /**
   * 构造没有空闲执行器异常
   * @param id 执行器ID
   * @return 构造好的异常对象
   */
  def noExecutorIdleError(id: String): Throwable = {
    new NoSuchElementException(id)
  }

  /**
   * 构造Spark作业被取消异常
   * @param jobId 作业ID
   * @param reason 取消原因
   * @param e 原始异常
   * @return 构造好的SparkException异常
   */
  def sparkJobCancelled(jobId: Int, reason: String, e: Exception): SparkException = {
    new SparkException(
      errorClass = "SPARK_JOB_CANCELLED",
      messageParameters = Map("jobId" -> jobId.toString, "reason" -> reason),
      cause = e
    )
  }

  /**
   * 构造作业因所属作业组被取消而取消的异常
   * @param jobId 作业ID
   * @param jobGroupId 作业组ID
   * @return 构造好的SparkException异常
   */
  def sparkJobCancelledAsPartOfJobGroupError(jobId: Int, jobGroupId: String): SparkException = {
    sparkJobCancelled(jobId, s"part of cancelled job group $jobGroupId", null)
  }

  /**
   * 构造屏障阶段包含不支持的RDD链模式异常
   * @return 构造好的异常对象
   */
  def barrierStageWithRDDChainPatternError(): Throwable = {
    new BarrierJobUnsupportedRDDChainException
  }

  /**
   * 构造动态分配下不支持屏障作业异常
   * @return 构造好的异常对象
   */
  def barrierStageWithDynamicAllocationError(): Throwable = {
    new BarrierJobRunWithDynamicAllocationException
  }

  /**
   * 构造分区数超过最大并发任务数异常，屏障作业要求所有分区同时运行
   * @param numPartitions 分区数
   * @param maxNumConcurrentTasks 集群最大并发任务数
   * @return 构造好的异常对象
   */
  def numPartitionsGreaterThanMaxNumConcurrentTasksError(
      numPartitions: Int,
      maxNumConcurrentTasks: Int): Throwable = {
    new BarrierJobSlotsNumberCheckFailed(numPartitions, maxNumConcurrentTasks)
  }

  /**
   * 构造无法在零分区RDD上提交MapStage异常
   * @return 构造好的异常对象
   */
  def cannotRunSubmitMapStageOnZeroPartitionRDDError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3023", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造访问不存在的累加器异常
   * @param id 累加器ID
   * @return 构造好的异常对象
   */
  def accessNonExistentAccumulatorError(id: Long): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3024", messageParameters = Map("id" -> s"$id"), cause = null
    )
  }

  /**
   * 构造仅ShuffleMapStage允许重新提交任务状态异常
   * @return 构造好的异常对象
   */
  def sendResubmittedTaskStatusForShuffleMapStagesOnlyError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3025", messageParameters = Map.empty, cause = null
    )
  }

  /**
   * 构造超时后事件队列仍非空异常
   * @param timeoutMillis 超时毫秒数
   * @return 构造好的异常对象
   */
  def nonEmptyEventQueueAfterTimeoutError(timeoutMillis: Long): Throwable = {
    new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
  }

  /**
   * 构造对未完成任务调用duration方法异常
   * @return 构造好的异常对象
   */
  def durationCalledOnUnfinishedTaskError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3026")
  }

  /**
   * 构造通用Spark错误异常
   * @param errorMsg 错误信息
   * @return 构造好的异常对象
   */
  def sparkError(errorMsg: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3028",
      messageParameters = Map("errorMsg" -> errorMsg),
      cause = null
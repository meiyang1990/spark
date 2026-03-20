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

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * Stage是一组并行任务的集合，这些任务执行相同的计算函数，具有相同的Shuffle依赖。
 * 调度器运行的每个任务DAG在Shuffle边界处被拆分为多个Stage，
 * 然后DAGScheduler按拓扑顺序执行这些Stage。
 *
 * Stage分为两种类型：
 * 1. ShuffleMapStage：中间Stage，其任务结果作为后续Stage的输入
 * 2. ResultStage：最终Stage，直接通过在RDD上运行函数来计算Spark action（如count()、save()等）
 *
 * 对于ShuffleMapStage，还会跟踪每个输出分区所在的节点。
 *
 * 每个Stage还有一个firstJobId，标识首次提交该Stage的作业。
 * 在FIFO调度模式下，这允许较早作业的Stage优先计算或在故障时更快恢复。
 *
 * 由于故障恢复，单个Stage可能需要多次重试执行（称为"attempt"）。
 * Stage对象会跟踪多个StageInfo对象，最新的可通过latestInfo访问。
 *
 * @param id Stage唯一ID
 * @param rdd 此Stage运行的RDD：ShuffleMapStage是执行map任务的RDD，ResultStage是执行action的目标RDD
 * @param numTasks Stage中的总任务数；ResultStage可能不需要计算所有分区（如first()、lookup()、take()）
 * @param parents 此Stage依赖的父Stage列表（通过Shuffle依赖）
 * @param firstJobId 此Stage所属的第一个作业ID，用于FIFO调度
 * @param callSite 与此Stage关联的用户程序位置：ShuffleMapStage为目标RDD的创建位置，ResultStage为action调用位置
 */
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite,
    val resourceProfileId: Int)
  extends Logging {

  /** RDD的分区总数 */
  val numPartitions = rdd.partitions.length

  /** 此Stage所属的作业ID集合 */
  val jobIds = new HashSet[Int]

  /** 下一次尝试的ID */
  private var nextAttemptId: Int = 0
  private[scheduler] def getNextAttemptId: Int = nextAttemptId

  /**
   * 是否在不同的Stage尝试之间检测到校验和不匹配。
   * 校验和不匹配通常表示不同的Stage尝试产生了不同的数据（非确定性计算）。
   */
  private[scheduler] var isChecksumMismatched: Boolean = false

  /**
   * 检测到校验和不匹配的最大任务尝试ID。
   */
  private[scheduler] var maxChecksumMismatchedId: Int = nextAttemptId

  /**
   * 应忽略此Stage结果的最大尝试ID。
   * 当祖先Stage被检测到校验和不匹配时设置。该Stage可能也是非确定性的，
   * 因此需要忽略之前尝试的任务输出（可能消费了不一致的数据），
   * 以避免用错误的结果完成Stage和作业。
   */
  private[scheduler] var maxAttemptIdToIgnore: Option[Int] = None

  /** Stage名称，取自调用位置的简短形式 */
  val name: String = callSite.shortForm
  /** Stage详情，取自调用位置的长形式 */
  val details: String = callSite.longForm

  /**
   * 指向最近一次尝试的 [[StageInfo]] 对象的指针。
   * 需要在此处初始化（在任何尝试实际创建之前），因为DAGScheduler使用此StageInfo
   * 在作业开始时通知SparkListener（这发生在任何Stage尝试创建之前）。
   */
  private var _latestInfo: StageInfo =
    StageInfo.fromStage(this, nextAttemptId, resourceProfileId = resourceProfileId)

  /**
   * 失败的Stage尝试ID集合。跟踪这些失败以避免Stage持续失败时无限重试。
   * 跟踪每个失败的尝试ID以避免同一Stage尝试中多个任务失败时记录重复失败（SPARK-5945）。
   */
  val failedAttemptIds = new HashSet[Int]

  /** 清除失败记录 */
  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }

  /** 将最新尝试标记为回滚状态 */
  private[scheduler] def markAsRollingBack(): Unit = {
    // 仅当Stage已提交过时才执行
    if (getNextAttemptId > 0) {
      maxAttemptIdToIgnore = Some(latestInfo.attemptNumber())
    }
  }

  /** 通过创建带有新尝试ID的StageInfo来为此Stage创建新的尝试 */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences,
      resourceProfileId = resourceProfileId)
    nextAttemptId += 1
  }

  /** 如果Stage被跳过且首次访问，则递增nextAttemptId */
  def increaseAttemptIdOnFirstSkip(): Unit = {
    if (nextAttemptId == 0) {
      nextAttemptId = 1
    }
  }

  /** 返回此Stage最近一次尝试的StageInfo */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** 返回缺失的（即需要计算的）分区ID序列 */
  def findMissingPartitions(): Seq[Int]
}

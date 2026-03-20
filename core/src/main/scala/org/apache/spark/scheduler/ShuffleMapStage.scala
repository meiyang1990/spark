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

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency}
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStage是执行DAG中的中间Stage，为Shuffle操作产生数据。
 * 它们出现在每个Shuffle操作之前，可能包含多个流水线化的操作（如map和filter）。
 * 执行时，它们会保存Map输出文件，供后续的Reduce任务获取。
 *
 * `shuffleDep`字段描述了每个Stage所属的Shuffle，`outputLocs`和`numAvailableOutputs`等
 * 变量跟踪有多少Map输出已就绪。
 *
 * ShuffleMapStage也可以通过 DAGScheduler.submitMapStage 作为独立作业提交。
 * 对于这种Stage，提交它们的ActiveJob在`mapStageJobs`中跟踪。
 * 注意：可能有多个ActiveJob尝试计算同一个ShuffleMapStage。
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],
    mapOutputTrackerMaster: MapOutputTrackerMaster,
    resourceProfileId: Int)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite, resourceProfileId) {

  /** 独立提交此ShuffleMapStage的map-stage作业列表 */
  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * 尚未计算的分区，或者在计算后所在Executor已丢失需要重新计算的分区。
   * DAGScheduler使用此变量来判断Stage是否完成。
   * 活跃尝试和之前尝试中的任务成功都可能导致分区ID从pendingPartitions中移除。
   * 因此，此变量可能与活跃尝试的TaskSetManager中的待处理任务不一致
   * （此处存储的分区始终是TaskSetManager认为待处理分区的子集）。
   */
  val pendingPartitions = new HashSet[Int]

  override def toString: String = "ShuffleMapStage " + id

  /**
   * 返回活跃作业列表，即独立提交以执行此Stage的map-stage作业（如果有）。
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** 将作业添加到活跃作业列表 */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** 从活跃作业列表中移除作业 */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * 拥有Shuffle输出的分区数。当此值达到 [[numPartitions]] 时，此Map Stage就绪。
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)

  /**
   * 判断Map Stage是否就绪，即所有分区都已有Shuffle输出。
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** 返回缺失的（即需要计算的）分区ID序列 */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }

  /**
   * 根据RDD的outputDeterministicLevel属性，判断此Stage在静态声明上是否为非确定性的。
   * 此信息在RDD创建时就已确定。
   */
  def isStaticallyIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }

  /**
   * 在运行时通过校验和不匹配检测到此Stage是否为非确定性的。
   * 这意味着不同的Stage尝试为同一分区产生了不同的数据。
   */
  def isRuntimeIndeterminate: Boolean = {
    !rdd.isReliablyCheckpointed && isChecksumMismatched
  }
}

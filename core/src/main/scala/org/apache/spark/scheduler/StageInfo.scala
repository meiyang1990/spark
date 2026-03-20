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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * 存储Stage的信息，用于从调度器传递给SparkListener。
 * 包含Stage的ID、名称、任务数、RDD信息、父Stage、提交/完成时间等。
 */
@DeveloperApi
class StageInfo(
    val stageId: Int,
    private val attemptId: Int,
    val name: String,
    val numTasks: Int,
    val rddInfos: Seq[RDDInfo],
    val parentIds: Seq[Int],
    val details: String,
    val taskMetrics: TaskMetrics = null,
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
    private[spark] val shuffleDepId: Option[Int] = None,
    val resourceProfileId: Int,
    private[spark] var isShufflePushEnabled: Boolean = false,
    private[spark] var shuffleMergerCount: Int = 0) {
  /** Stage从DAGScheduler提交到TaskScheduler的时间 */
  var submissionTime: Option[Long] = None
  /** Stage完成或被取消的时间 */
  var completionTime: Option[Long] = None
  /** 如果Stage失败，记录失败原因 */
  var failureReason: Option[String] = None

  /**
   * 此Stage期间更新的累加器最终值，包括所有用户定义的累加器。
   */
  val accumulables = HashMap[Long, AccumulableInfo]()

  def stageFailed(reason: String): Unit = {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  // This would just be the second constructor arg, except we need to maintain this method
  // with parentheses for compatibility
  def attemptNumber(): Int = attemptId

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }

  private[spark] def setShuffleMergerCount(mergers: Int): Unit = {
    shuffleMergerCount = mergers
  }

  private[spark] def setPushBasedShuffleEnabled(pushBasedShuffleEnabled: Boolean): Unit = {
    isShufflePushEnabled = pushBasedShuffleEnabled
  }
}

private[spark] object StageInfo {
  /**
   * 从Stage构造StageInfo。
   *
   * 每个Stage关联一个或多个RDD，Stage的边界由Shuffle依赖标记。
   * 因此，通过窄依赖链与此Stage的RDD相关的所有祖先RDD也应关联到此Stage。
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
      resourceProfileId: Int
    ): StageInfo = {
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    val shuffleDepId = stage match {
      case sms: ShuffleMapStage => Option(sms.shuffleDep).map(_.shuffleId)
      case _ => None
    }
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences,
      shuffleDepId,
      resourceProfileId,
      false,
      0)
  }
}

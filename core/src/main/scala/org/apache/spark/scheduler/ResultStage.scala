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
// 这个文件已经全部加上中文注释

package org.apache.spark.scheduler

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ResultStage（结果Stage）在RDD的某些分区上应用函数来计算action的结果。
 * 此类捕获要执行的函数`func`（应用于每个分区）和分区ID集合`partitions`。
 * 对于first()和lookup()等action，某些Stage可能不会在RDD的所有分区上运行。
 */
private[spark] class ResultStage(
    id: Int,
    rdd: RDD[_],
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    resourceProfileId: Int)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite, resourceProfileId) {

  /**
   * 此ResultStage的活跃作业。如果作业已完成（例如被取消），则为空。
   */
  private[this] var _activeJob: Option[ActiveJob] = None

  def activeJob: Option[ActiveJob] = _activeJob

  /** 设置活跃作业 */
  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  /** 移除活跃作业 */
  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
   * 返回缺失的（即需要计算的）分区ID序列。
   * 仅在有活跃作业时才能调用。
   */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage " + id
}

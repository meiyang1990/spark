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

package org.apache.spark.deploy.history

import scala.collection.mutable

import org.apache.spark.deploy.history.EventFilter.FilterStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId

/**
 * 文件概要：事件历史过滤构建模块，提供基础事件过滤器的构建能力，用于在历史事件处理中过滤已完成作业和已下线Executor的事件
 */

/**
 * 追踪活跃作业和活跃Executor状态，为BasicEventFilter构建过滤条件，用于过滤已完成作业和下线Executor相关事件
 * 
 * 监听Spark事件流，维护当前所有活跃的作业、阶段、任务、RDD和Executor列表，最终生成过滤器供事件过滤使用
 * 实现了SparkListener接口接收事件，实现了EventFilterBuilder接口生成最终过滤器
 */
private[spark] class BasicEventFilterBuilder extends SparkListener with EventFilterBuilder {
  // 活跃作业到对应阶段ID列表的映射
  private val liveJobToStages = new mutable.HashMap[Int, Set[Int]]
  // 活跃阶段到对应任务ID列表的映射
  private val stageToTasks = new mutable.HashMap[Int, mutable.Set[Long]]
  // 活跃阶段到对应RDD ID列表的映射
  private val stageToRDDs = new mutable.HashMap[Int, Set[Int]]
  // 当前所有活跃的Executor ID集合
  private val _liveExecutors = new mutable.HashSet[String]

  private var totalJobs: Long = 0L
  private var totalStages: Long = 0L
  private var totalTasks: Long = 0L

  // 获取所有活跃作业ID集合
  private[history] def liveJobs: Set[Int] = liveJobToStages.keySet.toSet
  // 获取所有活跃阶段ID集合
  private[history] def liveStages: Set[Int] = stageToRDDs.keySet.toSet
  // 获取所有活跃任务ID集合
  private[history] def liveTasks: Set[Long] = stageToTasks.values.flatten.toSet
  // 获取所有活跃RDD ID集合
  private[history] def liveRDDs: Set[Int] = stageToRDDs.values.flatten.toSet
  // 获取所有活跃Executor ID集合
  private[history] def liveExecutors: Set[String] = _liveExecutors.toSet

  /**
   * 处理作业启动事件，更新活跃作业统计和状态
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    totalJobs += 1
    totalStages += jobStart.stageIds.length
    liveJobToStages += jobStart.jobId -> jobStart.stageIds.toSet
  }

  /**
   * 处理作业结束事件，从活跃集合中移除作业及其关联的阶段、RDD
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val stages = liveJobToStages.getOrElse(jobEnd.jobId, Seq.empty[Int])
    liveJobToStages -= jobEnd.jobId
    stageToTasks --= stages
    stageToRDDs --= stages
  }

  /**
   * 处理阶段提交事件，记录阶段关联的RDD信息
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageId = stageSubmitted.stageInfo.stageId
    stageToRDDs.put(stageId, stageSubmitted.stageInfo.rddInfos.map(_.id).toSet)
    stageToTasks.getOrElseUpdate(stageId, new mutable.HashSet[Long]())
  }

  /**
   * 处理任务启动事件，更新总任务计数并将任务加入活跃集合
   */
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    totalTasks += 1
    stageToTasks.get(taskStart.stageId).foreach { tasks =>
      tasks += taskStart.taskInfo.taskId
    }
  }

  /**
   * 处理Executor添加事件，将新Executor加入活跃集合
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    _liveExecutors += executorAdded.executorId
  }

  /**
   * 处理Executor移除事件，将下线Executor从活跃集合移除
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    _liveExecutors -= executorRemoved.executorId
  }

  /**
   * 根据当前收集的活跃状态，生成最终事件过滤器
   * @return 配置完成的BasicEventFilter实例
   */
  override def createFilter(): EventFilter = {
    val stats = FilterStatistics(totalJobs, liveJobs.size, totalStages,
      liveStages.size, totalTasks, liveTasks.size)

    new BasicEventFilter(stats, liveJobs, liveStages, liveTasks, liveRDDs, liveExecutors)
  }
}

/**
 * 作业相关事件过滤抽象基类，基于活跃作业/阶段/任务/RDD集合过滤已完成作业的相关事件
 * 
 * 提供作业相关事件的接受判断逻辑，返回true表示保留事件，false表示过滤事件
 * 子类扩展支持Executor相关事件的过滤
 */
private[spark] abstract class JobEventFilter(
    stats: Option[FilterStatistics],
    liveJobs: Set[Int],
    liveStages: Set[Int],
    liveTasks: Set[Long],
    liveRDDs: Set[Int]) extends EventFilter with Logging {

  logDebug(s"jobs : $liveJobs")
  logDebug(s"stages : $liveStages")
  logDebug(s"tasks : $liveTasks")
  logDebug(s"RDDs : $liveRDDs")

  override def statistics(): Option[FilterStatistics] = stats

  // 作业相关事件的接受判断偏函数，根据事件类型匹配判断是否保留
  protected val acceptFnForJobEvents: PartialFunction[SparkListenerEvent, Boolean] = {
    case e: SparkListenerStageCompleted =>
      liveStages.contains(e.stageInfo.stageId)
    case e: SparkListenerStageSubmitted =>
      liveStages.contains(e.stageInfo.stageId)
    case e: SparkListenerTaskStart =>
      liveTasks.contains(e.taskInfo.taskId)
    case e: SparkListenerTaskGettingResult =>
      liveTasks.contains(e.taskInfo.taskId)
    case e: SparkListenerTaskEnd =>
      liveTasks.contains(e.taskInfo.taskId)
    case e: SparkListenerJobStart =>
      liveJobs.contains(e.jobId)
    case e: SparkListenerJobEnd =>
      liveJobs.contains(e.jobId)
    case e: SparkListenerUnpersistRDD =>
      liveRDDs.contains(e.rddId)
    case e: SparkListenerExecutorMetricsUpdate =>
      e.accumUpdates.exists { case (taskId, stageId, _, _) =>
        liveTasks.contains(taskId) || liveStages.contains(stageId)
      }
    case e: SparkListenerSpeculativeTaskSubmitted =>
      liveStages.contains(e.stageId)
  }
}

/**
 * 完整基础事件过滤器，同时过滤已完成作业和已下线Executor相关事件
 * 
 * 仅保留活跃作业、活跃Executor关联的事件，不关联作业/Executor的事件默认保留（不做过滤）
 * 继承JobEventFilter的作业过滤逻辑，扩展增加Executor相关事件的过滤能力
 */
private[spark] class BasicEventFilter(
    stats: FilterStatistics,
    liveJobs: Set[Int],
    liveStages: Set[Int],
    liveTasks: Set[Long],
    liveRDDs: Set[Int],
    liveExecutors: Set[String])
  extends JobEventFilter(
    Some(stats),
    liveJobs,
    liveStages,
    liveTasks,
    liveRDDs) with Logging {

  logDebug(s"live executors : $liveExecutors")

  // Executor和BlockManager相关事件的接受判断偏函数
  private val _acceptFn: PartialFunction[SparkListenerEvent, Boolean] = {
    case e: SparkListenerExecutorAdded => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorRemoved => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorBlacklisted => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorUnblacklisted => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorExcluded => liveExecutors.contains(e.executorId)
    case e: SparkListenerExecutorUnexcluded => liveExecutors.contains(e.executorId)
    case e: SparkListenerStageExecutorMetrics => liveExecutors.contains(e.execId)
    case e: SparkListenerBlockManagerAdded => acceptBlockManagerEvent(e.blockManagerId)
    case e: SparkListenerBlockManagerRemoved => acceptBlockManagerEvent(e.blockManagerId)
    case e: SparkListenerBlockUpdated => acceptBlockManagerEvent(e.blockUpdatedInfo.blockManagerId)
  }

  /**
   * 判断BlockManager事件是否保留：Driver的事件总是保留，Executor的事件仅当Executor活跃时保留
   * @param blockManagerId BlockManager标识
   * @return true表示保留事件，false表示过滤
   */
  private def acceptBlockManagerEvent(blockManagerId: BlockManagerId): Boolean = {
    blockManagerId.isDriver || liveExecutors.contains(blockManagerId.executorId)
  }

  override def acceptFn(): PartialFunction[SparkListenerEvent, Boolean] = {
    _acceptFn.orElse(acceptFnForJobEvents)
  }
}
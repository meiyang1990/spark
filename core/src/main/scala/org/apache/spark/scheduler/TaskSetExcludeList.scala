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

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.util.Clock

/**
 * 处理任务集(TaskSet)内部的 Executor 和节点排除。
 * 包括排除特定的 (任务, Executor) / (任务, 节点) 对，
 * 以及完全排除整个任务集的 Executor 和节点。
 *
 * 它还必须存储任务失败的足够信息以供应用级别排除使用，
 * 后者由 [[HealthTracker]] 处理。注意 HealthTracker 在任务集成功完成之前
 * 不知道任何任务失败信息。
 *
 * 如果 isDryRun 为 true，则此类仅用于存储应用级别排除的信息，
 * 不会实际在任务/Stage 级别排除任何任务。
 *
 * 线程安全：此类是 [[TaskSetManager]] 的辅助类；与 [[TaskSetManager]] 中的方法一样，
 * 此类设计为只能从持有 TaskScheduler 锁的代码中调用，不应从其他线程调用。
 */
private[scheduler] class TaskSetExcludelist(
    private val listenerBus: LiveListenerBus,
    val conf: SparkConf,
    val stageId: Int,
    val stageAttemptId: Int,
    val clock: Clock,
    val isDryRun: Boolean = false) extends Logging {

  /** 每个 Executor 上允许的最大任务尝试次数 */
  private val MAX_TASK_ATTEMPTS_PER_EXECUTOR = conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
  /** 每个节点上允许的最大任务尝试次数 */
  private val MAX_TASK_ATTEMPTS_PER_NODE = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
  /** 每个 Executor 在此 Stage 中允许的最大失败次数 */
  private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
  /** 每个节点在此 Stage 中允许的最大失败 Executor 数 */
  private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)

  /**
   * 每个 Executor 到其上任务失败记录的映射。
   * 用于此任务集内的排除，同时在任务集成功完成后传递给 [[HealthTracker]] 进行应用级别排除。
   */
  val execToFailures = new HashMap[String, ExecutorFailuresInTaskSet]()

  /**
   * 节点到该节点上所有有失败记录的 Executor 的映射。
   * 需要此映射是因为即使 Executor 已死亡也需要知道节点上的 Executor 信息。
   * （在没有失败的正常情况下不追踪节点->Executor 映射以减少开销）
   */
  private val nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()
  /** 节点到该节点上被排除的任务索引集合 */
  private val nodeToExcludedTaskIndexes = new HashMap[String, HashSet[Int]]()
  /** 完全被排除的 Executor 集合 */
  private val excludedExecs = new HashSet[String]()
  /** 完全被排除的节点集合 */
  private val excludedNodes = new HashSet[String]()

  private var latestFailureReason: String = null

  /**
   * 获取此 TaskSet 最近的失败原因。
   */
  def getLatestFailureReason: String = {
    latestFailureReason
  }

  /**
   * Return true if this executor is excluded for the given task.  This does *not*
   * need to return true if the executor is excluded for the entire stage, or excluded
   * for the entire application.  That is to keep this method as fast as possible in the inner-loop
   * of the scheduler, where those filters will have already been applied.
   */
  def isExecutorExcludedForTask(executorId: String, index: Int): Boolean = {
    !isDryRun && execToFailures.get(executorId).exists { execFailures =>
      execFailures.getNumTaskFailures(index) >= MAX_TASK_ATTEMPTS_PER_EXECUTOR
    }
  }

  def isNodeExcludedForTask(node: String, index: Int): Boolean = {
    !isDryRun && nodeToExcludedTaskIndexes.get(node).exists(_.contains(index))
  }

  /**
   * Return true if this executor is excluded for the given stage.  Completely ignores whether
   * the executor is excluded for the entire application (or anything to do with the node the
   * executor is on).  That is to keep this method as fast as possible in the inner-loop of the
   * scheduler, where those filters will already have been applied.
   */
  def isExecutorExcludedForTaskSet(executorId: String): Boolean = {
    !isDryRun && excludedExecs.contains(executorId)
  }

  def isNodeExcludedForTaskSet(node: String): Boolean = {
    !isDryRun && excludedNodes.contains(node)
  }

  private[scheduler] def updateExcludedForFailedTask(
      host: String,
      exec: String,
      index: Int,
      failureReason: String): Unit = {
    latestFailureReason = failureReason
    val execFailures = execToFailures.getOrElseUpdate(exec, new ExecutorFailuresInTaskSet(host))
    execFailures.updateWithFailure(index, clock.getTimeMillis())

    // check if this task has also failed on other executors on the same host -- if its gone
    // over the limit, exclude this task from the entire host.
    val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(host, new HashSet())
    execsWithFailuresOnNode += exec
    val failuresOnHost = execsWithFailuresOnNode.iterator.flatMap { exec =>
      execToFailures.get(exec).map { failures =>
        // We count task attempts here, not the number of unique executors with failures.  This is
        // because jobs are aborted based on the number task attempts; if we counted unique
        // executors, it would be hard to config to ensure that you try another
        // node before hitting the max number of task failures.
        failures.getNumTaskFailures(index)
      }
    }.sum
    if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
      nodeToExcludedTaskIndexes.getOrElseUpdate(host, new HashSet()) += index
    }

    // Check if enough tasks have failed on the executor to exclude it for the entire stage.
    val numFailures = execFailures.numUniqueTasksWithFailures
    if (numFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
      if (excludedExecs.add(exec)) {
        logInfo(log"Excluding executor ${MDC(LogKeys.EXECUTOR_ID, exec)} for stage " +
          log"${MDC(LogKeys.STAGE_ID, stageId)}")
        // This executor has been excluded for this stage.  Let's check if it
        // the whole node should be excluded.
        val excludedExecutorsOnNode =
          execsWithFailuresOnNode.intersect(excludedExecs)
        val now = clock.getTimeMillis()
        // SparkListenerExecutorBlacklistedForStage is deprecated but post both events
        // to keep backward compatibility
        listenerBus.post(
          SparkListenerExecutorBlacklistedForStage(now, exec, numFailures, stageId, stageAttemptId))
        listenerBus.post(
          SparkListenerExecutorExcludedForStage(now, exec, numFailures, stageId, stageAttemptId))
        val numFailExec = excludedExecutorsOnNode.size
        if (numFailExec >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
          if (excludedNodes.add(host)) {
            logInfo(log"Excluding ${MDC(LogKeys.HOST, host)} for " +
              log"stage ${MDC(LogKeys.STAGE_ID, stageId)}")
            // SparkListenerNodeBlacklistedForStage is deprecated but post both events
            // to keep backward compatibility
            listenerBus.post(
              SparkListenerNodeBlacklistedForStage(now, host, numFailExec, stageId, stageAttemptId))
            listenerBus.post(
              SparkListenerNodeExcludedForStage(now, host, numFailExec, stageId, stageAttemptId))
          }
        }
      }
    }
  }
}

private[scheduler] object TaskSetExcludelist {

  /**
   * Returns true if the excludeOnFailure is enabled on the task/stage level,
   * based on checking the configuration in the following order:
   * 1. Is taskset level exclusion specifically enabled or disabled?
   * 2. Is overall exclusion feature enabled or disabled?
   * 3. Default is off
   */
  def isExcludeOnFailureEnabled(conf: SparkConf): Boolean = {
    conf.get(config.EXCLUDE_ON_FAILURE_ENABLED_TASK_AND_STAGE)
      .orElse(conf.get(config.EXCLUDE_ON_FAILURE_ENABLED)).getOrElse(false)
  }
}

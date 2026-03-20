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

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * HealthTracker 用于跟踪有问题的 Executor 和节点。它支持在整个应用程序范围内
 * 排除(exclude) Executor 和节点（带周期性过期机制）。TaskSetManager 会添加
 * 额外的逻辑来针对单个任务和 Stage 排除 Executor 和节点，与此处的逻辑协同工作。
 *
 * 追踪器需要处理多种工作负载场景，例如：
 *  * 错误的用户代码 -- 可能导致大量任务失败，但不应该归咎于单个 Executor
 *  * 大量小 Stage -- 可能阻止一个坏 Executor 在单个 Stage 内积累大量失败，
 *      但在整个应用中仍然有很多失败
 *  * "不稳定"的 Executor -- 不是每个任务都失败，但仍然足够有问题需要被排除
 *  * 缺失的 Shuffle 文件 -- 可能在健康的 Executor 上触发 Fetch 失败
 *
 * 参见 SPARK-8425 的设计文档了解更深入的讨论。注意 SPARK-32037 重命名了该功能。
 *
 * 线程安全：与 TaskSchedulerImpl 的大多数辅助类一样，此类不是线程安全的。
 * 虽然它被多个线程调用，但调用者必须已经持有 TaskSchedulerImpl 的锁。
 * 唯一的例外是 [[excludedNodeList()]]，可以在不持有锁的情况下调用。
 */
private[scheduler] class HealthTracker (
    private val listenerBus: LiveListenerBus,
    conf: SparkConf,
    allocationClient: Option[ExecutorAllocationClient],
    clock: Clock = new SystemClock()) extends Logging {

  def this(sc: SparkContext, allocationClient: Option[ExecutorAllocationClient]) = {
    this(sc.listenerBus, sc.conf, allocationClient)
  }

  HealthTracker.validateExcludeOnFailureConfs(conf)
  /** 每个 Executor 允许的最大失败次数 */
  private val MAX_FAILURES_PER_EXEC = conf.get(config.MAX_FAILURES_PER_EXEC)
  /** 每个节点允许的最大失败 Executor 数 */
  private val MAX_FAILED_EXEC_PER_NODE = conf.get(config.MAX_FAILED_EXEC_PER_NODE)
  /** 排除超时时间（毫秒），超时后排除状态自动解除 */
  val EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS = HealthTracker.getExcludeOnFailureTimeout(conf)
  /** 是否因 Fetch 失败而排除 */
  private val EXCLUDE_FETCH_FAILURE_ENABLED =
    conf.get(config.EXCLUDE_ON_FAILURE_FETCH_FAILURE_ENABLED)
  /** 是否使用退役（decommission）代替直接杀死排除的 Executor */
  private val EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED =
    conf.get(config.EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED)

  /**
   * Executor ID 到任务失败列表的映射。跟踪每个任务失败的时间，
   * 以避免因时间间隔很长的失败而排除 Executor。
   * 不会在超时时主动清理，以避免清理的时间开销。但不会增长太大，
   * 因为一旦 Executor 达到失败上限，就会被排除并移除此处的条目。
   */
  private val executorIdToFailureList = new HashMap[String, ExecutorFailureList]()
  /** 已排除的 Executor 及其状态 */
  val executorIdToExcludedStatus = new HashMap[String, ExcludedExecutor]()
  /** 已排除的节点及其过期时间 */
  val nodeIdToExcludedExpiryTime = new HashMap[String, Long]()
  /**
   * 当前被排除节点集合的不可变副本。保存在 AtomicReference 中以确保
   * [[excludedNodeList()]] 的线程安全性。
   */
  private val _excludedNodeList = new AtomicReference[Set[String]](Set())
  /**
   * 下一个被排除节点的过期时间。用作快捷方式，
   * 避免在没有过期条目时遍历 excludedNodeList 的所有条目。
   */
  var nextExpiryTime: Long = Long.MaxValue
  /**
   * 节点到该节点上所有被排除 Executor 的映射。
   * 当 Executor 从 Spark 中移除时不会从此映射中移除，以便跟踪一个节点上
   * 连续多个 Executor 被排除的情况。不会增长太大，因为一个节点上不会有
   * 太多被排除的 Executor，超时机制也会定期清理。
   */
  val nodeToExcludedExecs = new HashMap[String, HashSet[String]]()

  /**
   * 将已被排除至少 EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS 的 Executor 和节点
   * 恢复为可用状态（解除排除）。
   */
  def applyExcludeOnFailureTimeout(): Unit = {
    val now = clock.getTimeMillis()
    // quickly check if we've got anything to expire that is excluded -- if not,
    // avoid doing any work
    if (now > nextExpiryTime) {
      // Apply the timeout to excluded nodes and executors
      val execsToInclude = executorIdToExcludedStatus.filter(_._2.expiryTime < now).keys
      if (execsToInclude.nonEmpty) {
        // Include any executors that have been excluded longer than the excludeOnFailure timeout.
        logInfo(log"Removing executors ${MDC(EXECUTOR_IDS, execsToInclude)} from " +
          log"exclude list because the executors have reached the timed out")
        execsToInclude.foreach { exec =>
          val status = executorIdToExcludedStatus.remove(exec).get
          val failedExecsOnNode = nodeToExcludedExecs(status.node)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerExecutorUnblacklisted(now, exec))
          listenerBus.post(SparkListenerExecutorUnexcluded(now, exec))
          failedExecsOnNode.remove(exec)
          if (failedExecsOnNode.isEmpty) {
            nodeToExcludedExecs.remove(status.node)
          }
        }
      }
      val nodesToInclude = nodeIdToExcludedExpiryTime.filter(_._2 < now).keys
      if (nodesToInclude.nonEmpty) {
        // Include any nodes that have been excluded longer than the excludeOnFailure timeout.
        logInfo(log"Removing nodes ${MDC(NODES, nodesToInclude)} from exclude list because the " +
          log"nodes have reached has timed out")
        nodesToInclude.foreach { node =>
          nodeIdToExcludedExpiryTime.remove(node)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerNodeUnblacklisted(now, node))
          listenerBus.post(SparkListenerNodeUnexcluded(now, node))
        }
        _excludedNodeList.set(nodeIdToExcludedExpiryTime.keySet.toSet)
      }
      updateNextExpiryTime()
    }
  }

  /** 更新下一个过期时间，取 Executor 和节点中最早的过期时间 */
  private def updateNextExpiryTime(): Unit = {
    val execMinExpiry = if (executorIdToExcludedStatus.nonEmpty) {
      executorIdToExcludedStatus.map{_._2.expiryTime}.min
    } else {
      Long.MaxValue
    }
    val nodeMinExpiry = if (nodeIdToExcludedExpiryTime.nonEmpty) {
      nodeIdToExcludedExpiryTime.values.min
    } else {
      Long.MaxValue
    }
    nextExpiryTime = math.min(execMinExpiry, nodeMinExpiry)
  }

  /** 杀死或退役指定 Executor，根据配置决定使用杀死还是退役方式 */
  private def killExecutor(exec: String, msg: String): Unit = {
    val fullMsg = if (EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED) {
      s"${msg} (actually decommissioning)"
    } else {
      msg
    }
    allocationClient match {
      case Some(a) =>
        logInfo(fullMsg)
        if (EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED) {
          a.decommissionExecutor(exec, ExecutorDecommissionInfo(fullMsg),
            adjustTargetNumExecutors = false)
        } else {
          a.killExecutors(Seq(exec), adjustTargetNumExecutors = false, countFailures = false,
            force = true)
        }
      case None =>
        logInfo(log"Not attempting to kill excluded executor id ${MDC(EXECUTOR_ID, exec)}" +
          log" since allocation client is not defined.")
    }
  }

  private def killExcludedExecutor(exec: String): Unit = {
    if (conf.get(config.EXCLUDE_ON_FAILURE_KILL_ENABLED)) {
      killExecutor(exec, s"Killing excluded executor id $exec since " +
        s"${config.EXCLUDE_ON_FAILURE_KILL_ENABLED.key} is set.")
    }
  }

  private[scheduler] def killExcludedIdleExecutor(exec: String): Unit = {
    killExecutor(exec,
      s"Killing excluded idle executor id $exec because of task unschedulability and trying " +
        "to acquire a new executor.")
  }

  private def killExecutorsOnExcludedNode(node: String): Unit = {
    if (conf.get(config.EXCLUDE_ON_FAILURE_KILL_ENABLED)) {
      allocationClient match {
        case Some(a) =>
          if (EXCLUDE_ON_FAILURE_DECOMMISSION_ENABLED) {
            logInfo(log"Decommissioning all executors on excluded host ${MDC(HOST, node)} " +
              log"since ${MDC(CONFIG, config.EXCLUDE_ON_FAILURE_KILL_ENABLED.key)} " +
              log"is set.")
            if (!a.decommissionExecutorsOnHost(node)) {
              logError(log"Decommissioning executors on ${MDC(HOST, node)} failed.")
            }
          } else {
            logInfo(log"Killing all executors on excluded host ${MDC(HOST, node)} " +
              log"since ${MDC(CONFIG, config.EXCLUDE_ON_FAILURE_KILL_ENABLED.key)} is set.")
            if (!a.killExecutorsOnHost(node)) {
              logError(log"Killing executors on node ${MDC(HOST, node)} failed.")
            }
          }
        case None =>
          logWarning(
            log"Not attempting to kill executors on excluded host ${MDC(HOST_PORT, node)} " +
              log"since allocation client is not defined.")
      }
    }
  }

  /**
   * 因 Fetch 失败更新排除状态。
   * 如果启用了外部 Shuffle 服务，排除整个节点（因为该节点上所有 Executor 都受影响）；
   * 否则仅排除发生失败的 Executor。
   */
  def updateExcludedForFetchFailure(host: String, exec: String): Unit = {
    if (EXCLUDE_FETCH_FAILURE_ENABLED) {
      // If we exclude on fetch failures, we are implicitly saying that we believe the failure is
      // non-transient, and can't be recovered from (even if this is the first fetch failure,
      // stage is retried after just one failure, so we don't always get a chance to collect
      // multiple fetch failures).
      // If the external shuffle-service is on, then every other executor on this node would
      // be suffering from the same issue, so we should exclude (and potentially kill) all
      // of them immediately.

      val now = clock.getTimeMillis()
      val expiryTimeForNewExcludes = now + EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS

      if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
        if (!nodeIdToExcludedExpiryTime.contains(host)) {
          logInfo(log"excluding node ${MDC(HOST, host)} due to fetch failure of " +
            log"external shuffle service")

          nodeIdToExcludedExpiryTime.put(host, expiryTimeForNewExcludes)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerNodeBlacklisted(now, host, 1))
          listenerBus.post(SparkListenerNodeExcluded(now, host, 1))
          _excludedNodeList.set(nodeIdToExcludedExpiryTime.keySet.toSet)
          killExecutorsOnExcludedNode(host)
          updateNextExpiryTime()
        }
      } else if (!executorIdToExcludedStatus.contains(exec)) {
        logInfo(log"Excluding executor ${MDC(EXECUTOR_ID, exec)} due to fetch failure")

        executorIdToExcludedStatus.put(exec, ExcludedExecutor(host, expiryTimeForNewExcludes))
        // We hardcoded number of failure tasks to 1 for fetch failure, because there's no
        // reattempt for such failure.
        // post both to keep backwards compatibility
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, 1))
        listenerBus.post(SparkListenerExecutorExcluded(now, exec, 1))
        updateNextExpiryTime()
        killExcludedExecutor(exec)

        val excludedExecsOnNode = nodeToExcludedExecs.getOrElseUpdate(host, HashSet[String]())
        excludedExecsOnNode += exec
      }
    }
  }

  /**
   * 当 TaskSet 成功完成时，统计各 Executor 的失败次数。
   * 如果某 Executor 的累计失败超过阈值，将其排除；
   * 如果某节点上被排除的 Executor 数超过阈值，排除整个节点。
   */
  def updateExcludedForSuccessfulTaskSet(
      stageId: Int,
      stageAttemptId: Int,
      failuresByExec: HashMap[String, ExecutorFailuresInTaskSet]): Unit = {
    // if any tasks failed, we count them towards the overall failure count for the executor at
    // this point.
    val now = clock.getTimeMillis()
    failuresByExec.foreach { case (exec, failuresInTaskSet) =>
      val appFailuresOnExecutor =
        executorIdToFailureList.getOrElseUpdate(exec, new ExecutorFailureList)
      appFailuresOnExecutor.addFailures(stageId, stageAttemptId, failuresInTaskSet)
      appFailuresOnExecutor.dropFailuresWithTimeoutBefore(now)
      val newTotal = appFailuresOnExecutor.numUniqueTaskFailures

      val expiryTimeForNewExcludes = now + EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS
      // If this pushes the total number of failures over the threshold, exclude the executor.
      // If its already excluded, we avoid "re-excluding" (which can happen if there were
      // other tasks already running in another taskset when it got excluded), because it makes
      // some of the logic around expiry times a little more confusing.  But it also wouldn't be a
      // problem to re-exclude, with a later expiry time.
      if (newTotal >= MAX_FAILURES_PER_EXEC && !executorIdToExcludedStatus.contains(exec)) {
        logInfo(log"Excluding executor id: ${MDC(EXECUTOR_ID, exec)} because it has " +
          log"${MDC(TOTAL, newTotal)} task failures in successful task sets")
        val node = failuresInTaskSet.node
        executorIdToExcludedStatus.put(exec, ExcludedExecutor(node, expiryTimeForNewExcludes))
        // post both to keep backwards compatibility
        listenerBus.post(SparkListenerExecutorBlacklisted(now, exec, newTotal))
        listenerBus.post(SparkListenerExecutorExcluded(now, exec, newTotal))
        executorIdToFailureList.remove(exec)
        updateNextExpiryTime()
        killExcludedExecutor(exec)

        // In addition to excluding the executor, we also update the data for failures on the
        // node, and potentially exclude the entire node as well.
        val excludedExecsOnNode = nodeToExcludedExecs.getOrElseUpdate(node, HashSet[String]())
        excludedExecsOnNode += exec
        // If the node is already excluded, we avoid adding it again with a later expiry
        // time.
        if (excludedExecsOnNode.size >= MAX_FAILED_EXEC_PER_NODE &&
            !nodeIdToExcludedExpiryTime.contains(node)) {
          logInfo(log"Excluding node ${MDC(HOST, node)} because it has " +
            log"${MDC(NUM_EXECUTORS, excludedExecsOnNode.size)} executors " +
            log"excluded: ${MDC(EXECUTOR_IDS, excludedExecsOnNode)}")
          nodeIdToExcludedExpiryTime.put(node, expiryTimeForNewExcludes)
          // post both to keep backwards compatibility
          listenerBus.post(SparkListenerNodeBlacklisted(now, node, excludedExecsOnNode.size))
          listenerBus.post(SparkListenerNodeExcluded(now, node, excludedExecsOnNode.size))
          _excludedNodeList.set(nodeIdToExcludedExpiryTime.keySet.toSet)
          killExecutorsOnExcludedNode(node)
        }
      }
    }
  }

  /** 判断指定 Executor 是否被排除 */
  def isExecutorExcluded(executorId: String): Boolean = {
    executorIdToExcludedStatus.contains(executorId)
  }

  /**
   * 获取所有被排除节点的完整集合。与此类中的其他方法不同，
   * 此方法是线程安全的——不需要持有 taskScheduler 的锁。
   */
  def excludedNodeList(): Set[String] = {
    _excludedNodeList.get()
  }

  /** 判断指定节点是否被排除 */
  def isNodeExcluded(node: String): Boolean = {
    nodeIdToExcludedExpiryTime.contains(node)
  }

  /** 处理 Executor 被移除事件：清理失败列表，但保留排除状态以支持节点级排除判断 */
  def handleRemovedExecutor(executorId: String): Unit = {
    // We intentionally do not clean up executors that are already excluded in
    // nodeToExcludedExecs, so that if another executor on the same node gets excluded, we can
    // exclude the entire node. We also can't clean up executorIdToExcludedStatus, so we can
    // eventually remove the executor after the timeout. Despite not clearing those structures
    // here, we don't expect they will grow too big since you won't get too many executors on one
    // node, and the timeout will clear it up periodically in any case.
    executorIdToFailureList -= executorId
  }

  /**
   * 跟踪单个 Executor 上所有未超时的任务失败。
   *
   * 通常预期此列表非常小，因为在 Executor 被排除前不会包含超过
   * 最大任务失败次数（默认为2次）的记录。
   */
  private[scheduler] final class ExecutorFailureList extends Logging {

    private case class TaskId(stage: Int, stageAttempt: Int, taskIndex: Int)

    /**
     * All failures on this executor in successful task sets.
     */
    private var failuresAndExpiryTimes = ArrayBuffer[(TaskId, Long)]()
    /**
     * As an optimization, we track the min expiry time over all entries in failuresAndExpiryTimes
     * so its quick to tell if there are any failures with expiry before the current time.
     */
    private var minExpiryTime = Long.MaxValue

    def addFailures(
        stage: Int,
        stageAttempt: Int,
        failuresInTaskSet: ExecutorFailuresInTaskSet): Unit = {
      failuresInTaskSet.taskToFailureCountAndFailureTime.foreach {
        case (taskIdx, (_, failureTime)) =>
          val expiryTime = failureTime + EXCLUDE_ON_FAILURE_TIMEOUT_MILLIS
          failuresAndExpiryTimes += ((TaskId(stage, stageAttempt, taskIdx), expiryTime))
          if (expiryTime < minExpiryTime) {
            minExpiryTime = expiryTime
          }
      }
    }

    /**
     * The number of unique tasks that failed on this executor.  Only counts failures within the
     * timeout, and in successful tasksets.
     */
    def numUniqueTaskFailures: Int = failuresAndExpiryTimes.size

    def isEmpty: Boolean = failuresAndExpiryTimes.isEmpty

    /**
     * Apply the timeout to individual tasks.  This is to prevent one-off failures that are very
     * spread out in time (and likely have nothing to do with problems on the executor) from
     * triggering exclusion.  However, note that we do *not* remove executors and nodes from
     * being excluded as we expire individual task failures -- each have their own timeout.  E.g.,
     * suppose:
     *  * timeout = 10, maxFailuresPerExec = 2
     *  * Task 1 fails on exec 1 at time 0
     *  * Task 2 fails on exec 1 at time 5
     * -->  exec 1 is excluded from time 5 - 15.
     * This is to simplify the implementation, as well as keep the behavior easier to understand
     * for the end user.
     */
    def dropFailuresWithTimeoutBefore(dropBefore: Long): Unit = {
      if (minExpiryTime < dropBefore) {
        var newMinExpiry = Long.MaxValue
        val newFailures = new ArrayBuffer[(TaskId, Long)]
        failuresAndExpiryTimes.foreach { case (task, expiryTime) =>
          if (expiryTime >= dropBefore) {
            newFailures += ((task, expiryTime))
            if (expiryTime < newMinExpiry) {
              newMinExpiry = expiryTime
            }
          }
        }
        failuresAndExpiryTimes = newFailures
        minExpiryTime = newMinExpiry
      }
    }

    override def toString(): String = {
      s"failures = $failuresAndExpiryTimes"
    }
  }

}

private[spark] object HealthTracker extends Logging {

  private val DEFAULT_TIMEOUT = "1h"

  /**
   * 判断应用级别的 excludeOnFailure 是否启用。
   * 按以下顺序检查配置：
   * 1. 应用级别的排除是否被明确启用或禁用？
   * 2. 总体排除功能是否被启用或禁用？
   * 3. 是否通过遗留超时配置启用？
   * 4. 默认关闭
   */
  def isExcludeOnFailureEnabled(conf: SparkConf): Boolean = {
    conf.get(config.EXCLUDE_ON_FAILURE_ENABLED_APPLICATION)
      .orElse(conf.get(config.EXCLUDE_ON_FAILURE_ENABLED)) match {
      case Some(enabled) =>
        enabled
      case None =>
        // if they've got a non-zero setting for the legacy conf, always enable it,
        // otherwise, use the default.
        val legacyKey = config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF.key
        conf.get(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF).exists { legacyTimeout =>
          if (legacyTimeout == 0) {
            logWarning(log"Turning off excludeOnFailure due to legacy configuration: " +
              log"${MDC(CONFIG, legacyKey)} == 0")
            false
          } else {
            logWarning(log"Turning on excludeOnFailure due to legacy configuration: " +
              log"${MDC(CONFIG, legacyKey)} > 0")
            true
          }
        }
    }
  }

  def getExcludeOnFailureTimeout(conf: SparkConf): Long = {
    conf.get(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF).getOrElse {
      conf.get(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF).getOrElse {
        Utils.timeStringAsMs(DEFAULT_TIMEOUT)
      }
    }
  }

  /**
   * 验证排除相关配置的一致性；如果不一致则抛出异常。
   * 仅在 excludeOnFailure 启用时调用。
   *
   * 配置需要遵循几个不变量。默认值自然遵循这些规则，
   * 但用户可能无意中修改了某个配置而没有做相应调整。
   * 此方法确保在配置不一致时快速失败。
   */
  def validateExcludeOnFailureConfs(conf: SparkConf): Unit = {

    def mustBePos(k: String, v: String): Unit = {
      throw new IllegalArgumentException(s"$k was $v, but must be > 0.")
    }

    Seq(
      config.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      config.MAX_TASK_ATTEMPTS_PER_NODE,
      config.MAX_FAILURES_PER_EXEC_STAGE,
      config.MAX_FAILED_EXEC_PER_NODE_STAGE,
      config.MAX_FAILURES_PER_EXEC,
      config.MAX_FAILED_EXEC_PER_NODE
    ).foreach { config =>
      val v = conf.get(config)
      if (v <= 0) {
        mustBePos(config.key, v.toString)
      }
    }

    val timeout = getExcludeOnFailureTimeout(conf)
    if (timeout <= 0) {
      // first, figure out where the timeout came from, to include the right conf in the message.
      conf.get(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF) match {
        case Some(t) =>
          mustBePos(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF.key, timeout.toString)
        case None =>
          mustBePos(config.EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF.key, timeout.toString)
      }
    }

    val maxTaskFailures = conf.get(config.TASK_MAX_FAILURES)
    val maxNodeAttempts = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)

    if (maxNodeAttempts >= maxTaskFailures) {
      throw new IllegalArgumentException(s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key} " +
        s"( = ${maxNodeAttempts}) was >= ${config.TASK_MAX_FAILURES.key} " +
        s"( = ${maxTaskFailures} ). Though excludeOnFailure is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node. Decrease " +
        s"${config.MAX_TASK_ATTEMPTS_PER_NODE.key}, increase ${config.TASK_MAX_FAILURES.key}, " +
        s"or disable excludeOnFailure with ${config.EXCLUDE_ON_FAILURE_ENABLED.key}")
    }
  }
}

private final case class ExcludedExecutor(node: String, expiryTime: Long)

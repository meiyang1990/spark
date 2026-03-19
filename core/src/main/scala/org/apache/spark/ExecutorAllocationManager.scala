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

package org.apache.spark

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.Tests.TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID
import org.apache.spark.resource.ResourceProfileManager
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.dynalloc.ExecutorMonitor
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * 基于工作负载动态分配和移除 Executor 的代理。
 *
 * 为每个 ResourceProfile 维护一个目标 Executor 数量，并周期性地与集群管理器同步。
 * 初始值由配置决定，随着待处理和正在运行的任务数量变化动态调整。
 *
 * 【缩减策略】当前目标超过处理当前负载所需时缩减。
 *
 * 【扩展策略】当有积压任务等待调度时扩展。若调度器队列在 M 秒内未清空则添加新 Executor，
 * 若积压持续 N 秒则继续添加，每轮增量按指数增长直到达到上限。
 * 指数增长的原因：(1) 初期慢速添加以避免过度分配 (2) 后期快速增长以应对大规模负载。
 *
 * 【移除策略】若某 ResourceProfile 的 Executor 空闲 K 秒且数量超过需要，则移除。
 * 缓存数据的 Executor 空闲超过 L 秒也会被移除。
 *
 * 关键配置项（按 ResourceProfile 独立生效）：
 *   spark.dynamicAllocation.enabled / minExecutors / maxExecutors / initialExecutors
 *   spark.dynamicAllocation.executorAllocationRatio - 降低并行度以节省小任务资源
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) - 积压触发添加的超时
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) - 持续积压触发添加的超时
 *   spark.dynamicAllocation.executorIdleTimeout (K) - 无缓存 Executor 空闲移除超时
 *   spark.dynamicAllocation.cachedExecutorIdleTimeout (L) - 有缓存 Executor 空闲移除超时
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf,
    cleaner: Option[ContextCleaner] = None,
    clock: Clock = new SystemClock(),
    resourceProfileManager: ResourceProfileManager,
    reliableShuffleStorage: Boolean)
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Executor 数量的上下限配置
  private val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
  private val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
  private val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)

  // 积压任务多久后触发添加 Executor（秒）
  private val schedulerBacklogTimeoutS = conf.get(DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT)

  // 首次超时后，持续积压多久继续添加（秒）
  private val sustainedSchedulerBacklogTimeoutS =
    conf.get(DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT)

  // 测试模式下 mock 掉实际的 kill/add 操作
  private val testing = conf.get(DYN_ALLOCATION_TESTING)

  // Executor 分配比率，用于小任务场景降低并行度以节省资源
  private val executorAllocationRatio =
    conf.get(DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO)

  private val decommissionEnabled = conf.get(DECOMMISSION_ENABLED)

  private val defaultProfileId = resourceProfileManager.defaultResourceProfile.id

  validateSettings()

  // 每个 ResourceProfile 下一轮要添加的 Executor 数量（指数增长）
  private[spark] val numExecutorsToAddPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsToAddPerResourceProfileId(defaultProfileId) = 1

  // 每个 ResourceProfile 当前的目标 Executor 数量
  private[spark] val numExecutorsTargetPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsTargetPerResourceProfileId(defaultProfileId) = initialNumExecutors

  // 下次触发添加 Executor 的时间戳（纳秒），NOT_SET 表示未设置
  private var addTime: Long = NOT_SET

  // 调度轮询间隔（毫秒）
  private val intervalMillis: Long = 100

  // 监听 Spark 事件以驱动分配策略
  val listener = new ExecutorAllocationListener

  // 执行调度任务的单线程调度器
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // 暴露内部状态到 MetricsSystem 的指标源
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource(this)

  // Executor 监控器：追踪 Executor 空闲状态、退役和资源使用
  val executorMonitor =
    new ExecutorMonitor(conf, client, listenerBus, clock, executorAllocationManagerSource)

  // 是否仍在等待初始 Executor 分配。为 true 时不取消未完成的请求。
  // 在 (1) Stage 提交 或 (2) Executor 空闲超时后设为 false。
  @volatile private var initializing: Boolean = true

  // 每个 ResourceProfile 的数据本地性感知任务数，用于 Executor 放置
  private var numLocalityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]
  numLocalityAwareTasksPerResourceProfileId(defaultProfileId) = 0

  // ResourceProfile id 到 Host 到本地任务数的映射，用于 Executor 放置决策
  private var rpIdToHostToLocalTaskCount: Map[Int, Map[String, Int]] = Map.empty

  // 校验动态分配相关的配置项是否合法，不合法则抛出异常
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException(
        s"${DYN_ALLOCATION_MIN_EXECUTORS.key} and ${DYN_ALLOCATION_MAX_EXECUTORS.key} must be " +
          "positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException(s"${DYN_ALLOCATION_MAX_EXECUTORS.key} cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"${DYN_ALLOCATION_MIN_EXECUTORS.key} ($minNumExecutors) must " +
        s"be less than or equal to ${DYN_ALLOCATION_MAX_EXECUTORS.key} ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException(s"${DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT.key} must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        s"s${DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT.key} must be > 0!")
    }
    val shuffleTrackingEnabled = conf.get(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED)
    val shuffleDecommissionEnabled = decommissionEnabled &&
      conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)
    if (!conf.get(config.SHUFFLE_SERVICE_ENABLED) && !reliableShuffleStorage) {
      if (shuffleTrackingEnabled) {
        logInfo("Dynamic allocation is enabled without a shuffle service.")
      } else if (shuffleDecommissionEnabled) {
        logInfo("Shuffle data decommission is enabled without a shuffle service.")
      } else if (!testing) {
        throw new SparkException("Dynamic allocation of executors requires one of the " +
          "following conditions: 1) enabling external shuffle service through " +
          s"${config.SHUFFLE_SERVICE_ENABLED.key}. 2) enabling shuffle tracking through " +
          s"${DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED.key}. 3) enabling shuffle blocks " +
          s"decommission through ${DECOMMISSION_ENABLED.key} and " +
          s"${STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key}. 4) (Experimental) " +
          s"configuring ${SHUFFLE_IO_PLUGIN_CLASS.key} to use a custom ShuffleDataIO who's " +
          "ShuffleDriverComponents supports reliable storage.")
      }
    }

    if (shuffleTrackingEnabled && (shuffleDecommissionEnabled || reliableShuffleStorage)) {
      logWarning("You are enabling both shuffle tracking and other DA supported mechanism, " +
        "which will cause idle executors not to be released in a timely, " +
        "please check the configurations.")
    }

    if (executorAllocationRatio > 1.0 || executorAllocationRatio <= 0.0) {
      throw new SparkException(
        s"${DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO.key} must be > 0 and <= 1.0")
    }
  }

  // 注册调度器回调、启动定时调度任务并向集群管理器请求初始 Executor
  def start(): Unit = {
    listenerBus.addToManagementQueue(listener)
    listenerBus.addToManagementQueue(executorMonitor)
    cleaner.foreach(_.attachListener(executorMonitor))

    val scheduleTask = new Runnable() {
      override def run(): Unit = Utils.tryLog(schedule())
    }

    if (!testing || conf.get(TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED)) {
      executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
    }

    // copy the maps inside synchronize to ensure not being modified
    val (numExecutorsTarget, numLocalityAware) = synchronized {
      val numTarget = numExecutorsTargetPerResourceProfileId.toMap
      val numLocality = numLocalityAwareTasksPerResourceProfileId.toMap
      (numTarget, numLocality)
    }

    client.requestTotalExecutors(numExecutorsTarget, numLocalityAware, rpIdToHostToLocalTaskCount)
  }

  // 停止动态分配管理器
  def stop(): Unit = {
    ThreadUtils.shutdown(executor, FiniteDuration(10, TimeUnit.SECONDS))
  }

  /**
   * 当集群管理器丢失 Driver 状态时重置分配管理器（目前仅用于 YARN client 模式 AM 重启）。
   * 忘记所有已有 Executor 的状态，强制调度器在下次运行时重新评估。
   */
  def reset(): Unit = synchronized {
    addTime = 0L
    numExecutorsTargetPerResourceProfileId.keys.foreach { rpId =>
      numExecutorsTargetPerResourceProfileId(rpId) = initialNumExecutors
    }
    numExecutorsToAddPerResourceProfileId.keys.foreach { rpId =>
      numExecutorsToAddPerResourceProfileId(rpId) = 1
    }
    executorMonitor.reset()
  }

  // 计算指定 ResourceProfile 当前负载下最多需要的 Executor 数量（向上取整）
  private[spark] def maxNumExecutorsNeededPerResourceProfile(rpId: Int): Int = {
    val pendingTask = listener.pendingTasksPerResourceProfile(rpId)
    val pendingSpeculative = listener.pendingSpeculativeTasksPerResourceProfile(rpId)
    val unschedulableTaskSets = listener.pendingUnschedulableTaskSetsPerResourceProfile(rpId)
    val running = listener.totalRunningTasksPerResourceProfile(rpId)
    val numRunningOrPendingTasks = pendingTask + pendingSpeculative + running
    val rp = resourceProfileManager.resourceProfileFromId(rpId)
    val tasksPerExecutor = rp.maxTasksPerExecutor(conf)
    logDebug(s"max needed for rpId: $rpId numpending: $numRunningOrPendingTasks," +
      s" tasksperexecutor: $tasksPerExecutor")
    val maxNeeded = math.ceil(numRunningOrPendingTasks * executorAllocationRatio /
      tasksPerExecutor).toInt

    val maxNeededWithSpeculationLocalityOffset =
      if (tasksPerExecutor > 1 && maxNeeded == 1 && pendingSpeculative > 0) {
      // 有待处理的推测任务且只需 1 个 Executor 时，多分配 1 个以满足推测执行的本地性需求
      maxNeeded + 1
    } else {
      maxNeeded
    }

    if (unschedulableTaskSets > 0) {
      // 有因 Executor/节点被排除而无法调度的任务集时，额外请求 Executor
      val maxNeededForUnschedulables = math.ceil(unschedulableTaskSets * executorAllocationRatio /
        tasksPerExecutor).toInt
      math.max(maxNeededWithSpeculationLocalityOffset,
        executorMonitor.executorCountWithResourceProfile(rpId) + maxNeededForUnschedulables)
    } else {
      maxNeededWithSpeculationLocalityOffset
    }
  }

  /**
   * 定时调度入口，按固定间隔调用，调节 Executor 请求数和运行数。
   *
   * 先根据添加时间和当前需求调整请求的 Executor 数，
   * 然后移除已超时空闲的 Executor。
   *
   * 独立为方法便于测试。
   */
  private def schedule(): Unit = synchronized {
    // 获取空闲超时的 Executor 列表
    val executorIdsToBeRemoved = executorMonitor.timedOutExecutors()
    if (executorIdsToBeRemoved.nonEmpty) {
      // 有超时 Executor 说明系统已稳定运行，退出初始化状态
      initializing = false
    }

    // 初始化标志清除后才更新目标 Executor 数量
    updateAndSyncNumExecutorsTarget(clock.nanoTime())
    if (executorIdsToBeRemoved.nonEmpty) {
      removeExecutors(executorIdsToBeRemoved)
    }
  }

  /**
   * 更新每个 ResourceProfile 的目标 Executor 数量并同步到集群管理器。
   *
   * 如果当前分配和已发出的请求超过实际需求，则缩减目标数并通知集群管理器取消多余的待处理请求。
   * 如果未超出且添加时间已到期，则请求新的 Executor 并刷新添加时间。
   *
   * @return 目标 Executor 数量的变化量
   */
  private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    if (initializing) {
      // 初始化阶段不调整目标数，避免第一个作业不必要的预热
      0
    } else {
      val updatesNeeded = new mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]

      // 遍历所有 ResourceProfile，统一更新目标后一次性请求集群管理器
      numExecutorsTargetPerResourceProfileId.foreach { case (rpId, targetExecs) =>
        val maxNeeded = maxNumExecutorsNeededPerResourceProfile(rpId)
        if (maxNeeded < targetExecs) {
          // 目标数超过实际需求，缩减目标（但不主动 kill，kill 由空闲超时机制单独控制）
          // 缩减目标的好处：当 Executor 意外丢失时不会再申请不需要的补充
          decrementExecutorsFromTarget(maxNeeded, rpId, updatesNeeded)
        } else if (addTime != NOT_SET && now >= addTime) {
          // 添加定时器已触发，尝试增加 Executor
          addExecutorsToTarget(maxNeeded, rpId, updatesNeeded)
        }
      }
      doUpdateRequest(updatesNeeded.toMap, now)
    }
  }

  // 委托 updateTargetExecs 执行增加 Executor 目标数的操作
  private def addExecutorsToTarget(
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    updateTargetExecs(addExecutors, maxNeeded, rpId, updatesNeeded)
  }

  // 委托 updateTargetExecs 执行缩减 Executor 目标数的操作
  private def decrementExecutorsFromTarget(
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    updateTargetExecs(decrementExecutors, maxNeeded, rpId, updatesNeeded)
  }

  /**
   * 通用的目标更新方法：调用传入的更新函数（增加或缩减），记录变化量到 updatesNeeded。
   */
  private def updateTargetExecs(
      updateTargetFn: (Int, Int) => Int,
      maxNeeded: Int,
      rpId: Int,
      updatesNeeded: mutable.HashMap[Int, ExecutorAllocationManager.TargetNumUpdates]): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    // 执行更新（增加或缩减）
    val delta = updateTargetFn(maxNeeded, rpId)
    if (delta != 0) {
      updatesNeeded(rpId) = ExecutorAllocationManager.TargetNumUpdates(delta, oldNumExecutorsTarget)
    }
    delta
  }

  /**
   * 将目标 Executor 数量变更发送到集群管理器。
   * 仅在目标确实发生变化时才向集群管理器发送请求。
   */
  private def doUpdateRequest(
      updates: Map[Int, ExecutorAllocationManager.TargetNumUpdates],
      now: Long): Int = {
    if (updates.size > 0) {
      val requestAcknowledged = try {
        logDebug("requesting updates: " + updates)
        // 向集群管理器发送总量请求（包含所有 ResourceProfile 的目标数和本地性信息）
        testing ||
          client.requestTotalExecutors(
            numExecutorsTargetPerResourceProfileId.toMap,
            numLocalityAwareTasksPerResourceProfileId.toMap,
            rpIdToHostToLocalTaskCount)
      } catch {
        case NonFatal(e) =>
          // 用 INFO 级别记录，避免 shell 中默认显示；YARN AM 重启是可恢复问题
          logInfo("Error reaching cluster manager.", e)
          false
      }
      if (requestAcknowledged) {
        var totalDelta = 0
        updates.foreach { case (rpId, targetNum) =>
          val delta = targetNum.delta
          totalDelta += delta
          if (delta > 0) {
            val executorsString = log" new executor" + { if (delta > 1) log"s" else log"" }
            logInfo(log"Requesting ${MDC(TARGET_NUM_EXECUTOR_DELTA, delta)}" +
              executorsString + log" because tasks are backlogged " +
              log"(new desired total will be" +
              log" ${MDC(TARGET_NUM_EXECUTOR, numExecutorsTargetPerResourceProfileId(rpId))} " +
              log"for resource profile id: ${MDC(RESOURCE_PROFILE_ID, rpId)})")
            // 指数增长策略：如果本轮实际增加数等于计划增加数，则下一轮翻倍；否则重置为1
            numExecutorsToAddPerResourceProfileId(rpId) =
              if (delta == numExecutorsToAddPerResourceProfileId(rpId)) {
                numExecutorsToAddPerResourceProfileId(rpId) * 2
              } else {
                1
              }
            // 设置下一次增加的定时器
            logDebug(s"Starting timer to add more executors (to " +
              s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
            addTime = now + TimeUnit.SECONDS.toNanos(sustainedSchedulerBacklogTimeoutS)
          } else {
            logDebug(s"Lowering target number of executors to" +
              s" ${numExecutorsTargetPerResourceProfileId(rpId)} (previously " +
              s"${targetNum.oldNumExecutorsTarget} for resource profile id: ${rpId}) " +
              "because not all requested executors " +
              "are actually needed")
          }
        }
        totalDelta
      } else {
        // 请求未被确认，回滚所有 ResourceProfile 的目标数到旧值
        updates.foreach { case (rpId, targetNum) =>
          logWarning("Unable to reach the cluster manager to request more executors!")
          numExecutorsTargetPerResourceProfileId(rpId) = targetNum.oldNumExecutorsTarget
        }
        0
      }
    } else {
      logDebug("No change in number of executors")
      0
    }
  }

  // 缩减目标 Executor 数到 max(maxNeeded, minNumExecutors)，重置指数增长步长为 1
  private def decrementExecutors(maxNeeded: Int, rpId: Int): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    numExecutorsTargetPerResourceProfileId(rpId) = math.max(maxNeeded, minNumExecutors)
    numExecutorsToAddPerResourceProfileId(rpId) = 1
    numExecutorsTargetPerResourceProfileId(rpId) - oldNumExecutorsTarget
  }

  /**
   * 增加目标 Executor 数量并计算实际新增数。
   * 采用指数增长策略（1, 2, 4, 8...），达到上限则重置步长。
   *
   * @param maxNumExecutorsNeeded 当前所有运行中/待处理任务能填满的最大 Executor 数
   * @param rpId ResourceProfile 的 ID
   * @return 实际请求的额外 Executor 数
   */
  private def addExecutors(maxNumExecutorsNeeded: Int, rpId: Int): Int = {
    val oldNumExecutorsTarget = numExecutorsTargetPerResourceProfileId(rpId)
    // 已达上限则不再请求，重置步长
    if (oldNumExecutorsTarget >= maxNumExecutors) {
      logDebug("Not adding executors because our current target total " +
        s"is already ${oldNumExecutorsTarget} (limit $maxNumExecutors)")
      numExecutorsToAddPerResourceProfileId(rpId) = 1
      return 0
    }
    // 确保目标不低于当前实际拥有的 Executor 数，避免无意义的爬升过程
    var numExecutorsTarget = math.max(numExecutorsTargetPerResourceProfileId(rpId),
        executorMonitor.executorCountWithResourceProfile(rpId))
    // 按指数步长增加
    numExecutorsTarget += numExecutorsToAddPerResourceProfileId(rpId)
    // 不超过当前实际需求
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // 确保在配置的上下限范围内
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)
    val delta = numExecutorsTarget - oldNumExecutorsTarget
    numExecutorsTargetPerResourceProfileId(rpId) = numExecutorsTarget

    // 目标未变化时重置指数增长步长
    if (delta == 0) {
      numExecutorsToAddPerResourceProfileId(rpId) = 1
    }
    delta
  }

  /**
   * 请求集群管理器移除指定的 Executor 列表。
   * 会检查每个 Executor 的移除是否会违反最小数量限制或低于目标数，不满足条件则跳过。
   * @return 实际被移除的 Executor ID 列表
   */
  private def removeExecutors(executors: Seq[(String, Int)]): Seq[String] = synchronized {
    val executorIdsToBeRemoved = new ArrayBuffer[String]
    logDebug(s"Request to remove executorIds: ${executors.mkString(", ")}")
    val numExecutorsTotalPerRpId = mutable.Map[Int, Int]()
    executors.foreach { case (executorIdToBeRemoved, rpId) =>
      if (rpId == UNKNOWN_RESOURCE_PROFILE_ID) {
        if (testing) {
          throw new SparkException("ResourceProfile Id was UNKNOWN, this is not expected")
        }
        logWarning(log"Not removing executor ${MDC(EXECUTOR_IDS, executorIdToBeRemoved)} " +
          log"because the ResourceProfile was UNKNOWN!")
      } else {
        // 初始化该 ResourceProfile 的可用 Executor 总数（排除待移除和正在退役的）
        val newExecutorTotal = numExecutorsTotalPerRpId.getOrElseUpdate(rpId,
          (executorMonitor.executorCountWithResourceProfile(rpId) -
            executorMonitor.pendingRemovalCountPerResourceProfileId(rpId) -
            executorMonitor.decommissioningPerResourceProfileId(rpId)
          ))
        if (newExecutorTotal - 1 < minNumExecutors) {
          // 移除后会低于最小 Executor 数限制，跳过
          logDebug(s"Not removing idle executor $executorIdToBeRemoved because there " +
            s"are only $newExecutorTotal executor(s) left (minimum number of executor limit " +
            s"$minNumExecutors)")
        } else if (newExecutorTotal - 1 < numExecutorsTargetPerResourceProfileId(rpId)) {
          // 移除后会低于目标 Executor 数，跳过
          logDebug(s"Not removing idle executor $executorIdToBeRemoved because there " +
            s"are only $newExecutorTotal executor(s) left (number of executor " +
            s"target ${numExecutorsTargetPerResourceProfileId(rpId)})")
        } else {
          executorIdsToBeRemoved += executorIdToBeRemoved
          numExecutorsTotalPerRpId(rpId) -= 1
        }
      }
    }

    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }

    // 向后端发送 kill/decommission 请求
    val executorsRemoved = if (testing) {
      executorIdsToBeRemoved
    } else {
      // 不调整目标数（已在任务积压减少时调整过了）
      if (decommissionEnabled) {
        // 优雅退役模式：发送退役请求而非直接 kill
        val executorIdsWithoutHostLoss = executorIdsToBeRemoved.map(
          id => (id, ExecutorDecommissionInfo("spark scale down"))).toArray
        client.decommissionExecutors(
          executorIdsWithoutHostLoss,
          adjustTargetNumExecutors = false,
          triggeredByExecutor = false)
      } else {
        client.killExecutors(executorIdsToBeRemoved.toSeq, adjustTargetNumExecutors = false,
          countFailures = false, force = false)
      }
    }

    // [SPARK-21834] killExecutors API 会减少目标数，需要重新同步为期望值
    client.requestTotalExecutors(
      numExecutorsTargetPerResourceProfileId.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap,
      rpIdToHostToLocalTaskCount)

    if (testing || executorsRemoved.nonEmpty) {
      // 通知 executorMonitor 更新内部状态（退役或已 kill）
      if (decommissionEnabled) {
        executorMonitor.executorsDecommissioned(executorsRemoved.toSeq)
      } else {
        executorMonitor.executorsKilled(executorsRemoved.toSeq)
      }
      logInfo(log"Executors ${MDC(EXECUTOR_IDS, executorsRemoved.mkString(","))} " +
        log"removed due to idle timeout.")
      executorsRemoved.toSeq
    } else {
      logWarning(log"Unable to reach the cluster manager to kill executor/s " +
        log"${MDC(EXECUTOR_IDS, executorIdsToBeRemoved.mkString(","))} " +
        log"or no executor eligible to kill!")
      Seq.empty[String]
    }
  }

  /**
   * 调度器收到新的待处理任务时触发。
   * 如果添加定时器尚未设置，则设置一个未来的时间点用于决定何时增加 Executor。
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.nanoTime() + TimeUnit.SECONDS.toNanos(schedulerBacklogTimeoutS)
    }
  }

  /**
   * 调度器队列清空时触发。
   * 重置所有与增加 Executor 相关的状态变量。
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    // 所有 ResourceProfile 的指数增长步长重置为 1
    numExecutorsToAddPerResourceProfileId.mapValuesInPlace { case (_, _) => 1 }
  }

  // 封装 Stage 尝试标识（stageId + attemptId）
  private case class StageAttempt(stageId: Int, stageAttemptId: Int) {
    override def toString: String = s"Stage $stageId (Attempt $stageAttemptId)"
  }

  /**
   * SparkListener 实现，监听调度事件并通知 ExecutorAllocationManager 何时增减 Executor。
   *
   * 对事件的相对顺序和一致性采取保守假设，不依赖特定事件到达顺序。
   */
  private[spark] class ExecutorAllocationListener extends SparkListener {

    // Stage 尝试 → 该 Stage 的总任务数
    private val stageAttemptToNumTasks = new mutable.HashMap[StageAttempt, Int]
    // Stage 尝试 → 正在运行的任务数（含推测任务），无活跃 Stage 时应为 0
    private val stageAttemptToNumRunningTask = new mutable.HashMap[StageAttempt, Int]
    // Stage 尝试 → 已启动的普通任务索引集合
    private val stageAttemptToTaskIndices = new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]
    // Stage 尝试 → 正在运行的推测任务索引集合
    private val stageAttemptToSpeculativeTaskIndices =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]()
    // Stage 尝试 → 待执行的推测任务索引集合
    private val stageAttemptToPendingSpeculativeTasks =
      new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]

    // ResourceProfile ID → 该 Profile 下的活跃 Stage 集合
    private val resourceProfileIdToStageAttempt =
      new mutable.HashMap[Int, mutable.Set[StageAttempt]]

    // 因任务失败过多导致 Executor/节点被排除而无法调度的 TaskSet 集合
    // 每个 TaskSet 只记录最后一个不可调度任务以避免调度中的高成本循环
    private val unschedulableTaskSets = new mutable.HashSet[StageAttempt]

    // Stage 尝试 → (有本地性偏好的任务数, 节点→本地性任务数的映射, ResourceProfile ID)
    // 用于为资源框架提供 Executor 放置提示
    private val stageAttemptToExecutorPlacementHints =
      new mutable.HashMap[StageAttempt, (Int, Map[String, Int], Int)]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val stageAttemptId = stageSubmitted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageAttemptToNumTasks(stageAttempt) = numTasks
        allocationManager.onSchedulerBacklogged()
        // need to keep stage task requirements to ask for the right containers
        val profId = stageSubmitted.stageInfo.resourceProfileId
        logDebug(s"Stage resource profile id is: $profId with numTasks: $numTasks")
        resourceProfileIdToStageAttempt.getOrElseUpdate(
          profId, new mutable.HashSet[StageAttempt]) += stageAttempt
        numExecutorsToAddPerResourceProfileId.getOrElseUpdate(profId, 1)

        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }
        stageAttemptToExecutorPlacementHints.put(stageAttempt,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap, profId))
        // Update the executor placement hints
        updateExecutorPlacementHints()

        if (!numExecutorsTargetPerResourceProfileId.contains(profId)) {
          numExecutorsTargetPerResourceProfileId.put(profId, initialNumExecutors)
          if (initialNumExecutors > 0) {
            logDebug(s"requesting executors, rpId: $profId, initial number is $initialNumExecutors")
            // we need to trigger a schedule since we add an initial number here.
            client.requestTotalExecutors(
              numExecutorsTargetPerResourceProfileId.toMap,
              numLocalityAwareTasksPerResourceProfileId.toMap,
              rpIdToHostToLocalTaskCount)
          }
        }
      }
    }

    // Stage 完成时触发：清理该 Stage 相关的各项追踪数据
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      val stageAttemptId = stageCompleted.stageInfo.attemptNumber()
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        // 不从 stageAttemptToNumRunningTask 中移除，因为该尝试可能仍有运行中的任务
        stageAttemptToNumTasks -= stageAttempt
        stageAttemptToPendingSpeculativeTasks -= stageAttempt
        stageAttemptToTaskIndices -= stageAttempt
        stageAttemptToSpeculativeTaskIndices -= stageAttempt
        stageAttemptToExecutorPlacementHints -= stageAttempt
        removeStageFromResourceProfileIfUnused(stageAttempt)

        updateExecutorPlacementHints()

        // 如果所有 Stage 的待处理任务都已清空，标记调度器队列为空
        if (stageAttemptToNumTasks.isEmpty
          && stageAttemptToPendingSpeculativeTasks.isEmpty
          && stageAttemptToSpeculativeTaskIndices.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }

    // 任务启动时触发：更新运行中任务计数，区分普通任务和推测任务
    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val stageAttemptId = taskStart.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = taskStart.taskInfo.index
      allocationManager.synchronized {
        stageAttemptToNumRunningTask(stageAttempt) =
          stageAttemptToNumRunningTask.getOrElse(stageAttempt, 0) + 1
        if (taskStart.taskInfo.speculative) {
          // 推测任务启动：加入运行中推测集合，从待执行推测集合中移除
          stageAttemptToSpeculativeTaskIndices.getOrElseUpdate(stageAttempt,
            new mutable.HashSet[Int]) += taskIndex
          stageAttemptToPendingSpeculativeTasks
            .get(stageAttempt).foreach(_.remove(taskIndex))
        } else {
          stageAttemptToTaskIndices.getOrElseUpdate(stageAttempt,
            new mutable.HashSet[Int]) += taskIndex
        }
        // 如果没有更多待处理任务，标记队列为空
        if (!hasPendingTasks) {
          allocationManager.onSchedulerQueueEmpty()
        }
      }
    }

    // 任务结束时触发：递减运行中任务计数，根据结束原因决定是否需要标记积压
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val stageId = taskEnd.stageId
      val stageAttemptId = taskEnd.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = taskEnd.taskInfo.index
      allocationManager.synchronized {
        if (stageAttemptToNumRunningTask.contains(stageAttempt)) {
          stageAttemptToNumRunningTask(stageAttempt) -= 1
          if (stageAttemptToNumRunningTask(stageAttempt) == 0) {
            stageAttemptToNumRunningTask -= stageAttempt
            removeStageFromResourceProfileIfUnused(stageAttempt)
          }
        }
        if (taskEnd.taskInfo.speculative) {
          stageAttemptToSpeculativeTaskIndices.get(stageAttempt).foreach {_.remove{taskIndex}}
        }

        taskEnd.reason match {
          case Success =>
            // 普通任务成功完成时，移除同索引的待执行推测任务（推测任务已无必要）
            stageAttemptToPendingSpeculativeTasks.get(stageAttempt).foreach(_.remove(taskIndex))
          case _: TaskKilled =>
            // 被主动 kill 的任务不做额外处理
          case _ =>
            // 任务失败（非主动 kill），预期会被重新提交
            // 如果队列为空，需重新标记为积压以确保有足够资源运行重试任务（SPARK-8366）
            if (!hasPendingTasks) {
              allocationManager.onSchedulerBacklogged()
            }
            if (!taskEnd.taskInfo.speculative) {
              // 非推测任务失败需从已完成索引集中移除，使其重新变为待处理状态
              // 推测任务被 kill 说明对应的推测任务已成功，不应移除索引（SPARK-30511）
              stageAttemptToTaskIndices.get(stageAttempt).foreach {_.remove(taskIndex)}
            }
        }
      }
    }

    // 推测任务被提交时触发：加入待执行推测集合，标记调度器有积压
    override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted)
      : Unit = {
      val stageId = speculativeTask.stageId
      val stageAttemptId = speculativeTask.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      val taskIndex = speculativeTask.taskIndex
      allocationManager.synchronized {
        stageAttemptToPendingSpeculativeTasks.getOrElseUpdate(stageAttempt,
          new mutable.HashSet[Int]).add(taskIndex)
        allocationManager.onSchedulerBacklogged()
      }
    }

    // 出现不可调度的 TaskSet 时触发（因 Executor/节点排除）：记录并标记积压
    override def onUnschedulableTaskSetAdded(
        unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = {
      val stageId = unschedulableTaskSetAdded.stageId
      val stageAttemptId = unschedulableTaskSetAdded.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        unschedulableTaskSets.add(stageAttempt)
        allocationManager.onSchedulerBacklogged()
      }
    }

    // 不可调度状态解除时触发：从集合中移除
    override def onUnschedulableTaskSetRemoved(
        unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = {
      val stageId = unschedulableTaskSetRemoved.stageId
      val stageAttemptId = unschedulableTaskSetRemoved.stageAttemptId
      val stageAttempt = StageAttempt(stageId, stageAttemptId)
      allocationManager.synchronized {
        unschedulableTaskSets.remove(stageAttempt)
        removeStageFromResourceProfileIfUnused(stageAttempt)
      }
    }

    // 当 Stage 确认无任何运行中/待处理任务时，从 ResourceProfile→Stage 映射中移除
    def removeStageFromResourceProfileIfUnused(stageAttempt: StageAttempt): Unit = {
      if (!stageAttemptToNumRunningTask.contains(stageAttempt) &&
          !stageAttemptToNumTasks.contains(stageAttempt) &&
          !stageAttemptToPendingSpeculativeTasks.contains(stageAttempt) &&
          !stageAttemptToTaskIndices.contains(stageAttempt) &&
          !stageAttemptToSpeculativeTaskIndices.contains(stageAttempt)
      ) {
        val rpForStage = resourceProfileIdToStageAttempt.filter { case (k, v) =>
          v.contains(stageAttempt)
        }.keys
        if (rpForStage.size == 1) {
          resourceProfileIdToStageAttempt(rpForStage.head) -= stageAttempt
        } else {
          // 一个 Stage 应恰好关联一个 ResourceProfile，否则属于异常情况
          logWarning(log"Should have exactly one resource profile for stage " +
            log"${MDC(STAGE_ATTEMPT, stageAttempt)}, but have " +
            log"${MDC(RESOURCE_PROFILE_IDS, rpForStage)}")
        }
      }
    }

    /**
     * 估算指定 ResourceProfile 下所有活跃 Stage 的剩余待处理任务数。
     * 不考虑可能已失败并被重新提交的任务。
     * 注意：调用方需持有 allocationManager 锁。
     */
    def pendingTasksPerResourceProfile(rpId: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rpId, Set.empty).toSeq
      attempts.map(attempt => getPendingTaskSum(attempt)).sum
    }

    // 是否存在待处理的普通任务
    def hasPendingRegularTasks: Boolean = {
      val attemptSets = resourceProfileIdToStageAttempt.values
      attemptSets.exists(attempts => attempts.exists(getPendingTaskSum(_) > 0))
    }

    // 计算指定 Stage 尝试的待处理普通任务数 = 总任务数 - 已启动任务数
    private def getPendingTaskSum(attempt: StageAttempt): Int = {
      val numTotalTasks = stageAttemptToNumTasks.getOrElse(attempt, 0)
      val numRunning = stageAttemptToTaskIndices.get(attempt).map(_.size).getOrElse(0)
      numTotalTasks - numRunning
    }

    // 估算指定 ResourceProfile 下待执行的推测任务数
    def pendingSpeculativeTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.map(attempt => getPendingSpeculativeTaskSum(attempt)).sum
    }

    // 是否存在待处理的推测任务
    def hasPendingSpeculativeTasks: Boolean = {
      val attemptSets = resourceProfileIdToStageAttempt.values
      attemptSets.exists { attempts =>
        attempts.exists(getPendingSpeculativeTaskSum(_) > 0)
      }
    }

    private def getPendingSpeculativeTaskSum(attempt: StageAttempt): Int = {
      stageAttemptToPendingSpeculativeTasks.get(attempt).map(_.size).getOrElse(0)
    }

    /**
     * 统计指定 ResourceProfile 下不可调度的 TaskSet 数量。
     * 由于无法精确得知不可调度任务的数量，以 TaskSet 数作为启发式指标。
     */
    def pendingUnschedulableTaskSetsPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.count(attempt => unschedulableTaskSets.contains(attempt))
    }

    // 是否存在任何待处理任务（普通 + 推测）
    def hasPendingTasks: Boolean = {
      hasPendingSpeculativeTasks || hasPendingRegularTasks
    }

    // 统计指定 ResourceProfile 下所有活跃 Stage 的运行中任务总数
    def totalRunningTasksPerResourceProfile(rp: Int): Int = {
      val attempts = resourceProfileIdToStageAttempt.getOrElse(rp, Set.empty).toSeq
      attempts.map { attempt =>
        stageAttemptToNumRunningTask.getOrElse(attempt, 0)
      }.sum
    }

    /**
     * 汇总所有活跃 Stage 的 Executor 放置提示信息。
     * 按 ResourceProfile 分组统计有本地性偏好的任务数和各节点的任务数，
     * 供集群管理器在分配 Executor 时参考以优化数据本地性。
     */
    def updateExecutorPlacementHints(): Unit = {
      val localityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]

      // ResourceProfile id => map[host, count]
      val rplocalityToCount = new mutable.HashMap[Int, mutable.HashMap[String, Int]]()
      stageAttemptToExecutorPlacementHints.values.foreach {
        case (numTasksPending, localities, rpId) =>
          val rpNumPending =
            localityAwareTasksPerResourceProfileId.getOrElse(rpId, 0)
          localityAwareTasksPerResourceProfileId(rpId) = rpNumPending + numTasksPending
          localities.foreach { case (hostname, count) =>
            val rpBasedHostToCount =
              rplocalityToCount.getOrElseUpdate(rpId, new mutable.HashMap[String, Int])
            val newUpdated = rpBasedHostToCount.getOrElse(hostname, 0) + count
            rpBasedHostToCount(hostname) = newUpdated
          }
      }

      allocationManager.numLocalityAwareTasksPerResourceProfileId =
        localityAwareTasksPerResourceProfileId
      allocationManager.rpIdToHostToLocalTaskCount =
        rplocalityToCount.map { case (k, v) => (k, v.toMap)}.toMap
    }
  }
}

/**
 * ExecutorAllocationManager 的指标数据源，向 Spark MetricsSystem 暴露内部 Executor 分配状态。
 * 注意：这些指标深度依赖 ExecutorAllocationManager 的内部实现，跨 Spark 版本可能不稳定。
 */
private[spark] class ExecutorAllocationManagerSource(
    executorAllocationManager: ExecutorAllocationManager) extends Source {
  val sourceName = "ExecutorAllocationManager"
  val metricRegistry = new MetricRegistry()

  // 注册 Gauge 指标的通用方法，带空值保护
  private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
    metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
      override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
    })
  }

  private def getCounter(name: String): Counter = {
    metricRegistry.counter(MetricRegistry.name("executors", name))
  }

  // 计数器：优雅退役、未完成退役、被 Driver kill、意外退出的 Executor 数量
  val gracefullyDecommissioned: Counter = getCounter("numberExecutorsGracefullyDecommissioned")
  val decommissionUnfinished: Counter = getCounter("numberExecutorsDecommissionUnfinished")
  val driverKilled: Counter = getCounter("numberExecutorsKilledByDriver")
  val exitedUnexpectedly: Counter = getCounter("numberExecutorsExitedUnexpectedly")

  // 以下 Gauge 指标返回所有 ResourceProfile 的汇总值
  registerGauge("numberExecutorsToAdd",
    executorAllocationManager.numExecutorsToAddPerResourceProfileId.values.sum, 0)
  registerGauge("numberExecutorsPendingToRemove",
    executorAllocationManager.executorMonitor.pendingRemovalCount, 0)
  registerGauge("numberAllExecutors",
    executorAllocationManager.executorMonitor.executorCount, 0)
  registerGauge("numberTargetExecutors",
    executorAllocationManager.numExecutorsTargetPerResourceProfileId.values.sum, 0)
  registerGauge("numberMaxNeededExecutors",
    executorAllocationManager.numExecutorsTargetPerResourceProfileId.keys
      .map(executorAllocationManager.maxNumExecutorsNeededPerResourceProfile(_)).sum, 0)
  registerGauge("numberDecommissioningExecutors",
    executorAllocationManager.executorMonitor.decommissioningCount, 0)
}

// ExecutorAllocationManager 伴生对象，存放常量和辅助类
private object ExecutorAllocationManager {
  // 表示"未设置"的哨兵值，用于 addTime
  val NOT_SET = Long.MaxValue

  // 辅助 case class：记录目标 Executor 数量的变化量和变化前的旧值，便于测试和回滚
  private[spark] case class TargetNumUpdates(delta: Int, oldNumExecutorsTarget: Int)

}

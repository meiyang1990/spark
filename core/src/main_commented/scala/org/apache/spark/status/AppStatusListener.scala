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

package org.apache.spark.status

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.CPUS_PER_TASK
import org.apache.spark.internal.config.Status._
import org.apache.spark.resource.ResourceProfile.CPUS
import org.apache.spark.scheduler._
import org.apache.spark.status.api.v1
import org.apache.spark.storage._
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.scope._

/**
 * 文件级注释：应用状态监听器，负责将Spark应用运行时事件写入KV存储，为UI和REST API提供状态数据支持
 * 存储的数据类型基于公共REST API定义，支持实时运行应用和历史日志回放两种场景
 * 
 * @param kvstore 用于存储应用状态数据的KV存储实例
 * @param conf Spark配置对象
 * @param live 是否为实时运行的应用
 * @param appStatusSource 应用状态指标源，用于统计状态相关指标
 * @param lastUpdateTime 回放日志时，日志的最后更新时间，用于更准确计算未完成任务的持续时间
 */
/**
 * A Spark listener that writes application information to a data store. The types written to the
 * store are defined in the `storeTypes.scala` file and are based on the public REST API.
 *
 * @param lastUpdateTime When replaying logs, the log's last update time, so that the duration of
 *                       unfinished tasks can be more accurately calculated (see SPARK-21922).
 */
private[spark] class AppStatusListener(
    kvstore: ElementTrackingStore,
    conf: SparkConf,
    live: Boolean,
    appStatusSource: Option[AppStatusSource] = None,
    lastUpdateTime: Option[Long] = None) extends SparkListener with Logging {

  private var sparkVersion = SPARK_VERSION
  private var appInfo: v1.ApplicationInfo = null
  private var appSummary = new AppSummary(0, 0)
  private var defaultCpusPerTask: Int = 1

  // 实时实体更新间隔，-1表示回放时不进行增量更新，只在最后写入
  private val liveUpdatePeriodNs = if (live) conf.get(LIVE_ENTITY_UPDATE_PERIOD) else -1L

  /**
   * 最小刷新间隔，避免任务事件不频繁时UI数据长时间不更新
   */
  private val liveUpdateMinFlushPeriod = conf.get(LIVE_ENTITY_UPDATE_MIN_FLUSH_PERIOD)

  private val maxTasksPerStage = conf.get(MAX_RETAINED_TASKS_PER_STAGE)
  private val maxGraphRootNodes = conf.get(MAX_RETAINED_ROOT_NODES)

  // 缓存所有活跃实体，避免频繁写入底层存储，优化性能
  private val liveStages = new ConcurrentHashMap[(Int, Int), LiveStage]()
  private val liveJobs = new HashMap[Int, LiveJob]()
  private[spark] val liveExecutors = new HashMap[String, LiveExecutor]()
  private[spark] val deadExecutors = new HashMap[String, LiveExecutor]()
  private val liveTasks = new HashMap[Long, LiveTask]()
  private val liveRDDs = new HashMap[Int, LiveRDD]()
  private val pools = new HashMap[String, SchedulerPool]()
  private val liveResourceProfiles = new HashMap[Int, LiveResourceProfile]()
  private[spark] val liveMiscellaneousProcess = new HashMap[String, LiveMiscellaneousProcess]()

  private val SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"
  // 单独维护活跃执行器数量，避免对liveExecutors的同步操作
  @volatile private var activeExecutorCount = 0

  /** 上次刷新活跃实体的时间，用于控制刷新频率，避免过于频繁 */
  private var lastFlushTimeNs = System.nanoTime()

  // 注册存储触发器，当超出最大保留数量时自动清理过期执行器数据
  kvstore.addTrigger(classOf[ExecutorSummaryWrapper], conf.get(MAX_RETAINED_DEAD_EXECUTORS))
    { count => cleanupExecutors(count) }

  // 注册存储触发器，当超出最大保留数量时自动清理过期Job数据
  kvstore.addTrigger(classOf[JobDataWrapper], conf.get(MAX_RETAINED_JOBS)) { count =>
    cleanupJobs(count)
  }

  // 注册存储触发器，当超出最大保留数量时自动清理过期Stage数据
  kvstore.addTrigger(classOf[StageDataWrapper], conf.get(MAX_RETAINED_STAGES)) { count =>
    cleanupStages(count)
  }

  // 注册存储刷新回调，回放日志时在刷新前批量更新所有实体
  kvstore.onFlush {
    if (!live) {
      val now = System.nanoTime()
      flush(update(_, now))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case SparkListenerLogStart(version) => sparkVersion = version
    case processInfoEvent: SparkListenerMiscellaneousProcessAdded =>
      onMiscellaneousProcessAdded(processInfoEvent)
    case _ =>
  }

  /** 处理应用启动事件，初始化应用基本信息并写入存储 */
  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    assert(event.appId.isDefined, "Application without IDs are not supported.")

    val attempt = v1.ApplicationAttemptInfo(
      event.appAttemptId,
      new Date(event.time),
      new Date(-1),
      new Date(event.time),
      -1L,
      event.sparkUser,
      false,
      sparkVersion)

    appInfo = v1.ApplicationInfo(
      event.appId.get,
      event.appName,
      None,
      None,
      None,
      None,
      Seq(attempt))

    kvstore.write(new ApplicationInfoWrapper(appInfo))
    kvstore.write(appSummary)

    // 更新Driver执行器的日志信息，Driver在SparkContext初始化时已注册
    event.driverLogs.foreach { logs =>
      val driver = liveExecutors.get(SparkContext.DRIVER_IDENTIFIER)
      driver.foreach { d =>
        d.executorLogs = logs.toMap
        d.attributes = event.driverAttributes.getOrElse(Map.empty).toMap
        update(d, System.nanoTime())
      }
    }
  }

  /** 处理资源配置添加事件，将新资源配置写入存储 */
  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {
    val maxTasks = if (event.resourceProfile.isCoresLimitKnown) {
      Some(event.resourceProfile.maxTasksPerExecutor(conf))
    } else {
      None
    }
    val liveRP = new LiveResourceProfile(event.resourceProfile.id,
      event.resourceProfile.executorResources, event.resourceProfile.taskResources, maxTasks)
    liveResourceProfiles(event.resourceProfile.id) = liveRP
    val rpInfo = new v1.ResourceProfileInfo(liveRP.resourceProfileId,
      liveRP.executorResources, liveRP.taskResources)
    kvstore.write(new ResourceProfileWrapper(rpInfo))
  }

  /** 处理环境更新事件，将应用环境信息写入存储 */
  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    val details = event.environmentDetails

    val jvmInfo = details("JVM Information").toMap
    val runtime = new v1.RuntimeInfo(
      jvmInfo.get("Java Version").orNull,
      jvmInfo.get("Java Home").orNull,
      jvmInfo.get("Scala Version").orNull)

    val envInfo = new v1.ApplicationEnvironmentInfo(
      runtime,
      details.getOrElse("Spark Properties", Nil),
      details.getOrElse("Hadoop Properties", Nil),
      details.getOrElse("System Properties", Nil),
      details.getOrElse("Metrics Properties", Nil),
      details.getOrElse("Classpath Entries", Nil),
      Nil)

    // 从配置中获取默认每个任务的CPU核数
    defaultCpusPerTask = envInfo.sparkProperties.toMap.get(CPUS_PER_TASK.key).map(_.toInt)
      .getOrElse(defaultCpusPerTask)

    kvstore.write(new ApplicationEnvironmentInfoWrapper(envInfo))
  }

  /** 处理应用结束事件，更新应用完成时间和状态 */
  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    val old = appInfo.attempts.head
    val attempt = v1.ApplicationAttemptInfo(
      old.attemptId,
      old.startTime,
      new Date(event.time),
      new Date(event.time),
      event.time - old.startTime.getTime(),
      old.sparkUser,
      true,
      old.appSparkVersion)

    appInfo = v1.ApplicationInfo(
      appInfo.id,
      appInfo.name,
      None,
      None,
      None,
      None,
      Seq(attempt))
    kvstore.write(new ApplicationInfoWrapper(appInfo))
  }

  /** 处理执行器添加事件，创建或更新执行器信息并更新存储 */
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    // 支持执行器重新注册，所以使用更新语义
    val exec = getOrCreateExecutor(event.executorId, event.time)
    exec.host = event.executorInfo.executorHost
    exec.isActive = true
    exec.totalCores = event.executorInfo.totalCores
    val rpId = event.executorInfo.resourceProfileId
    val liveRP = liveResourceProfiles.get(rpId)
    val cpusPerTask = liveRP.flatMap(_.taskResources.get(CPUS))
      .map(_.amount.toInt).getOrElse(defaultCpusPerTask)
    val maxTasksPerExec = liveRP.flatMap(_.maxTasksPerExecutor)
    exec.maxTasks = maxTasksPerExec.getOrElse(event.executorInfo.totalCores / cpusPerTask)
    exec.executorLogs = event.executorInfo.logUrlMap
    exec.resources = event.executorInfo.resourcesInfo
    exec.attributes = event.executorInfo.attributes
    exec.resourceProfileId = rpId
    liveUpdate(exec, System.nanoTime())
  }

  /** 处理执行器移除事件，更新执行器状态并清理相关数据 */
  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    liveExecutors.remove(event.executorId).foreach { exec =>
      val now = System.nanoTime()
      activeExecutorCount = math.max(0, activeExecutorCount - 1)
      exec.isActive = false
      exec.removeTime = new Date(event.time)
      exec.removeReason = event.reason
      update(exec, now, last = true)

      // 移除所有引用该执行器的RDD分布信息
      liveRDDs.values.foreach { rdd =>
        if (rdd.removeDistribution(exec)) {
          update(rdd, now)
        }
      }
      // 移除所有引用该执行器的RDD分区信息
      liveRDDs.values.foreach { rdd =>
        rdd.getPartitions().values
          .filter(_.executors.contains(event.executorId))
          .foreach { partition =>
            if (partition.executors.length == 1) {
              rdd.removePartition(partition.blockName)
              rdd.memoryUsed = addDeltaToValue(rdd.memoryUsed, partition.memoryUsed * -1)
              rdd.diskUsed = addDeltaToValue(rdd.diskUsed, partition.diskUsed * -1)
            } else {
              rdd.memoryUsed = addDeltaToValue(rdd.memoryUsed,
                (partition.memoryUsed / partition.executors.length) * -1)
              rdd.diskUsed = addDeltaToValue(rdd.diskUsed,
                (partition.diskUsed / partition.executors.length) * -1)
              partition.update(
                partition.executors.filter(!_.equals(event.executorId)),
                addDeltaToValue(partition.memoryUsed,
                  (partition.memoryUsed / partition.executors.length) * -1),
                addDeltaToValue(partition.diskUsed,
                  (partition.diskUsed / partition.executors.length) * -1))
            }
          }
        update(rdd, now)
      }
      // 如果该执行器在活跃Stage运行过，先保留在deadExecutors，待Stage完成后再清理
      if (isExecutorActiveForLiveStages(exec)) {
        deadExecutors.put(event.executorId, exec)
      }
    }
  }

  /** 检查执行器是否在当前活跃Stage中有活动记录 */
  private def isExecutorActiveForLiveStages(exec: LiveExecutor): Boolean = {
    liveStages.values.asScala.exists { stage =>
      stage.info.submissionTime.getOrElse(0L) < exec.removeTime.getTime
    }
  }

  // 保留旧方法用于向后兼容，支持历史服务器读取旧版本事件日志
  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    updateExecExclusionStatus(event.executorId, true)
  }

  override def onExecutorExcluded(event: SparkListenerExecutorExcluded): Unit = {
    updateExecExclusionStatus(event.executorId, true)
  }

  override def onExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Unit = {
    updateExclusionStatusForStage(event.stageId, event.stageAttemptId, event.executorId)
  }

  override def onExecutorExcludedForStage(
      event: SparkListenerExecutorExcludedForStage): Unit = {
    updateExclusionStatusForStage(event.stageId, event.stageAttemptId, event.executorId)
  }

  override def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit = {
    updateNodeExclusionStatusForStage(event.stageId, event.stageAttemptId, event.hostId)
  }

  override def onNodeExcludedForStage(event: SparkListenerNodeExcludedForStage): Unit = {
    updateNodeExclusionStatusForStage(event.stageId, event.stageAttemptId, event.hostId)
  }

  private def addExcludedStageTo(exec: LiveExecutor, stageId: Int, now: Long): Unit = {
    exec.excludedInStages += stageId
    liveUpdate(exec, now)
  }

  private def setStageExcludedStatus(stage: LiveStage, now: Long, executorIds: String*): Unit = {
    executorIds.foreach { executorId =>
      val executorStageSummary = stage.executorSummary(executorId)
      executorStageSummary.isExcluded = true
      maybeUpdate(executorStageSummary, now)
    }
    stage.excludedExecutors ++= executorIds
    maybeUpdate(stage, now)
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    updateExecExclusionStatus(event.executorId, false)
  }

  override def onExecutorUnexcluded(event: SparkListenerExecutorUnexcluded): Unit = {
    updateExecExclusionStatus(event.executorId, false)
  }

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    updateNodeExclusion(event.hostId, true)
  }

  override def onNodeExcluded(event: SparkListenerNodeExcluded): Unit = {
    updateNodeExclusion(event.hostId, true)
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    updateNodeExclusion(event.hostId, false)
  }

  override def onNodeUnexcluded(event: SparkListenerNodeUnexcluded): Unit = {
    updateNodeExclusion(event.hostId, false)
  }

  private def updateNodeExclusionStatusForStage(stageId: Int, stageAttemptId: Int,
      hostId: String): Unit = {
    val now = System.nanoTime()

    // 隐式排除该节点上所有可用执行器
    Option(liveStages.get((stageId, stageAttemptId))).foreach { stage =>
      val executorIds = liveExecutors.values.filter(exec => exec.host == hostId
        && exec.executorId != SparkContext.DRIVER_IDENTIFIER).map(_.executorId).toSeq
      setStageExcludedStatus(stage, now, executorIds: _*)
    }
    liveExecutors.values.filter(exec => exec.hostname == hostId
      && exec.executorId != SparkContext.DRIVER_IDENTIFIER).foreach { exec =>
      addExcludedStageTo(exec, stageId, now)
    }
  }

  private def updateExclusionStatusForStage(stageId: Int, stageAttemptId: Int,
      execId: String): Unit = {
    val now = System.nanoTime()

    Option(liveStages.get((stageId, stageAttemptId))).foreach { stage =>
      setStage
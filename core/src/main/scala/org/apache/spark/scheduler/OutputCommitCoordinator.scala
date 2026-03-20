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

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/** 输出提交协调的内部消息 */
private sealed trait OutputCommitCoordinationMessage extends Serializable

/** 停止协调器消息 */
private case object StopCoordinator extends OutputCommitCoordinationMessage
/** 请求提交输出的消息 */
private case class AskPermissionToCommitOutput(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attemptNumber: Int)

/**
 * 决定任务是否可以向 HDFS 提交输出的权威组件。采用"先到先得"策略。
 *
 * OutputCommitCoordinator 在 Driver 和 Executor 中都会实例化。在 Executor 上，
 * 它配置了指向 Driver 端 OutputCommitCoordinatorEndpoint 的引用，
 * 因此提交输出的请求会被转发到 Driver 端的 OutputCommitCoordinator。
 *
 * 此类在 SPARK-4879 中引入；详见该 JIRA issue（及相关的 Pull Request）了解设计讨论。
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // 由 SparkEnv 初始化
  var coordinatorRef: Option[RpcEndpointRef] = None

  // 用于标识提交者的类。提交者的任务ID由处理的分区隐式定义，
  // 但协调器需要同时跟踪 Stage 尝试和任务尝试，因为某些情况下
  // 同一任务可能在同一 Stage 的两个不同尝试中并发运行。
  private case class TaskIdentifier(stageAttempt: Int, taskAttempt: Int)

  /** Stage 状态：跟踪每个分区的授权提交者和已知的失败尝试 */
  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[TaskIdentifier](numPartitions)(null)
    val failures = mutable.Map[Int, mutable.Set[TaskIdentifier]]()
  }

  /**
   * 活跃 Stage ID 到其授权任务尝试的映射，每个分区持有提交任务输出的排他锁，
   * 以及该 Stage 中所有已知的失败尝试。
   *
   * Stage 开始时添加条目，结束时（无论成功或失败）移除条目。
   *
   * 对此映射的访问应通过对 OutputCommitCoordinator 实例进行同步来保护。
   */
  private val stageStates = mutable.Map[Int, StageState]()

  /**
   * 返回 OutputCommitCoordinator 的内部数据结构是否全部为空。
   */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
   * 由任务调用，询问它们是否可以向 HDFS 提交输出。
   *
   * 如果一个任务尝试已被授权提交，则提交同一任务的所有其他尝试将被拒绝。
   * 如果已授权的任务尝试失败（例如 Executor 丢失），则后续的任务尝试可能被授权提交输出。
   *
   * @param stage Stage 编号
   * @param partition 分区编号
   * @param attemptNumber 此任务被尝试的次数（参见 [[TaskContext.attemptNumber()]]）
   * @return 如果此任务被授权提交则返回 true，否则返回 false
   */
  def canCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * 由 DAGScheduler 在 Stage 启动时调用。初始化 Stage 的状态（如果尚未初始化）。
   *
   * @param stage Stage ID
   * @param maxPartitionId 此 Stage 任务中可能出现的最大分区ID
   *                       （即 context.partitionId 的最大可能值）
   */
  private[scheduler] def stageStart(stage: Int, maxPartitionId: Int): Unit = synchronized {
    stageStates.get(stage) match {
      case Some(state) =>
        require(state.authorizedCommitters.length == maxPartitionId + 1)
        logInfo(log"Reusing state from previous attempt of stage ${MDC(LogKeys.STAGE_ID, stage)}")

      case _ =>
        stageStates(stage) = new StageState(maxPartitionId + 1)
    }
  }

  // 由 DAGScheduler 在 Stage 结束时调用，清理该 Stage 的状态
  private[scheduler] def stageEnd(stage: Int): Unit = synchronized {
    stageStates.remove(stage)
  }

  // 由 DAGScheduler 在任务完成时调用，更新提交授权状态
  private[scheduler] def taskCompleted(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int,
      reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case _: TaskCommitDenied =>
        logInfo(log"Task was denied committing, stage: ${MDC(LogKeys.STAGE_ID, stage)}." +
          log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)}, " +
          log"partition: ${MDC(LogKeys.PARTITION_ID, partition)}, " +
          log"attempt: ${MDC(LogKeys.NUM_ATTEMPT, attemptNumber)}")
      case _ =>
        // Mark the attempt as failed to exclude from future commit protocol
        val taskId = TaskIdentifier(stageAttempt, attemptNumber)
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += taskId
        if (stageState.authorizedCommitters(partition) == taskId) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          stageState.authorizedCommitters(partition) = null
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      stageStates.clear()
    }
  }

  // 标记为 private[scheduler] 而非 private，以便测试中可以 mock
  /** 处理提交许可请求：检查是否已失败、是否已有授权提交者 */
  private[scheduler] def handleAskPermissionToCommit(
      stage: Int,
      stageAttempt: Int,
      partition: Int,
      attemptNumber: Int): Boolean = synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, stageAttempt, partition, attemptNumber) =>
        logInfo(log"Commit denied for stage=${MDC(LogKeys.STAGE_ID, stage)}." +
          log"${MDC(LogKeys.STAGE_ATTEMPT_ID, stageAttempt)}, partition=" +
          log"${MDC(LogKeys.PARTITION_ID, partition)}: task attempt " +
          log"${MDC(LogKeys.NUM_ATTEMPT, attemptNumber)} already marked as failed.")
        false
      case Some(state) =>
        val existing = state.authorizedCommitters(partition)
        if (existing == null) {
          logDebug(s"Commit allowed for stage=$stage.$stageAttempt, partition=$partition, " +
            s"task attempt $attemptNumber")
          state.authorizedCommitters(partition) = TaskIdentifier(stageAttempt, attemptNumber)
          true
        } else {
          logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
            s"already committed by $existing")
          false
        }
      case None =>
        logDebug(s"Commit denied for stage=$stage.$stageAttempt, partition=$partition: " +
          "stage already marked as completed.")
        false
    }
  }

  /** 检查指定的任务尝试是否已被标记为失败 */
  private def attemptFailed(
      stageState: StageState,
      stageAttempt: Int,
      partition: Int,
      attempt: Int): Boolean = synchronized {
    val failInfo = TaskIdentifier(stageAttempt, attempt)
    stageState.failures.get(partition).exists(_.contains(failInfo))
  }
}

private[spark] object OutputCommitCoordinator {

  // 此 RPC 端点仅用于远程通信，Executor 通过它向 Driver 请求提交许可
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, stageAttempt, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, stageAttempt, partition,
            attemptNumber))
    }
  }
}

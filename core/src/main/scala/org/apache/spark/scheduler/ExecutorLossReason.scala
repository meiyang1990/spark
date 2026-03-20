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

import org.apache.spark.executor.ExecutorExitCode

/**
 * 表示Executor或整个进程失败/退出原因的基类。
 */
private[spark]
class ExecutorLossReason(val message: String) extends Serializable {
  override def toString: String = message
}

/**
 * Executor以指定退出码退出。
 * @param exitCode 进程退出码
 * @param exitCausedByApp 退出是否由应用程序引起
 * @param reason 退出原因的可读描述
 */
private[spark]
case class ExecutorExited(exitCode: Int, exitCausedByApp: Boolean, reason: String)
  extends ExecutorLossReason(reason)

private[spark] object ExecutorExited {
  /** 根据退出码自动生成退出原因描述 */
  def apply(exitCode: Int, exitCausedByApp: Boolean): ExecutorExited = {
    ExecutorExited(
      exitCode,
      exitCausedByApp,
      ExecutorExitCode.explainExitCode(exitCode))
  }
}

/** Executor丢失的消息常量 */
private[spark] object ExecutorLossMessage {
  val decommissionFinished = "Finished decommissioning"
}

/** Executor被Driver主动杀死 */
private[spark] object ExecutorKilled extends ExecutorLossReason("Executor killed by driver.")

/**
 * 表示尚不知道Executor退出原因的丢失原因。
 *
 * 任务调度器用此来移除与Executor关联的状态，但在真正的丢失原因确定之前，
 * 不会立即将该Executor上正在运行的任务标记为失败。
 */
private [spark] object LossReasonPending extends ExecutorLossReason("Pending loss reason.")

/**
 * Executor进程丢失。
 * @param _message 人类可读的丢失原因
 * @param workerHost 如果定义了，表示主机也确认丢失（包括Shuffle服务）
 * @param causedByApp 丢失是否由运行中的应用引起（默认为true，除非明确知道不是）
 */
private[spark]
case class ExecutorProcessLost(
    _message: String = "Executor Process Lost",
    workerHost: Option[String] = None,
    causedByApp: Boolean = true)
  extends ExecutorLossReason(_message)

/**
 * 表示Executor被标记为下线（decommission）的丢失原因。
 *
 * 任务调度器用此来移除与Executor关联的状态，但在Executor"完全"丢失之前，
 * 不会立即将其上正在运行的任务标记为失败。
 * 如果修改此代码，请确保重新运行K8s集成测试。
 *
 * @param workerHost 如果定义了，表示Worker也被下线
 * @param reason 详细的下线原因消息
 */
private [spark] case class ExecutorDecommission(
    workerHost: Option[String] = None,
    reason: String = "")
  extends ExecutorLossReason(ExecutorDecommission.msgPrefix + reason)

private[spark] object ExecutorDecommission {
  val msgPrefix = "Executor decommission: "
}

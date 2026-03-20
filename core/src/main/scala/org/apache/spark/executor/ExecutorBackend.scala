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

package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState

/**
 * A pluggable interface used by the Executor to send updates to the cluster scheduler.
 *
 * ExecutorBackend 是一个可插拔的接口，Executor 通过该接口向集群调度器发送状态更新。
 * 该接口允许不同的后端实现（如 YARN、Kubernetes、Standalone）与 Executor 协作。
 */
private[spark] trait ExecutorBackend {
  /**
   * 向集群调度器汇报任务的状态更新
   * @param taskId 任务唯一标识符
   * @param state 任务的当前状态（如 RUNNING、FINISHED、FAILED 等）
   * @param data 序列化后的任务结果或失败原因
   */
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit
}


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

package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState

/**
 * 文件概述：Executor后端通信接口定义，为不同集群资源管理器提供Executor状态上报的统一抽象
 * 本文件定义了可插拔的Executor后端接口，隔离Executor核心逻辑与不同资源管理器的实现差异
 */

/**
 * 可插拔的Executor后端通信接口，负责Executor向集群调度器发送任务状态更新
 * 
 * 设计目的：为不同集群资源管理器（YARN、Kubernetes、Standalone等）提供统一的状态上报抽象
 * 核心职责：解耦Executor核心执行逻辑与集群调度器的通信细节，支持不同后端的灵活扩展
 * 协作对象：配合Executor使用，由Executor调用该接口完成任务状态上报
 */
private[spark] trait ExecutorBackend {
  /**
   * 向集群调度器汇报指定任务的最新状态
   * @param taskId 任务的全局唯一标识符
   * @param state 任务当前的执行状态（RUNNING、FINISHED、FAILED、KILLED等）
   * @param data 序列化后的状态数据，任务成功时为任务结果，失败时为异常信息
   */
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit
}
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

/**
 * Executor被下线（decommission）时提供详细信息的消息。
 * @param message 人类可读的下线原因说明
 * @param workerHost 如果定义了workerHost，表示该主机（即其他地方所说的`node`或`worker`）
 *                   也被下线了。用于判断即使启用了外部Shuffle服务，Shuffle数据是否可能丢失。
 */
private[spark]
case class ExecutorDecommissionInfo(message: String, workerHost: Option[String] = None)

/**
 * TaskSchedulerImpl维护的与Executor下线相关的状态。
 * 此状态从上面的信息消息派生，但保持独立以便状态可以独立于消息进行演化。
 *
 * @param startTime 按Driver时钟记录的下线开始时间戳，
 *                  用于在配置了EXECUTOR_DECOMMISSION_KILL_INTERVAL时估算Executor最终丢失的时间
 * @param workerHost 被下线的Worker主机名（如果主机也被下线）
 */
private[scheduler] case class ExecutorDecommissionState(
    startTime: Long,
    workerHost: Option[String] = None)

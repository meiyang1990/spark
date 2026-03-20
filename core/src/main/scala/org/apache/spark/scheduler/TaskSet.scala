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

import java.util.Properties

import org.apache.spark.internal.LogKeys.{STAGE_ATTEMPT_ID, STAGE_ID}
import org.apache.spark.internal.MessageWithContext

/**
 * 一组一起提交给底层TaskScheduler的任务集合，
 * 通常表示某个Stage中缺失的（需要计算的）分区。
 *
 * @param tasks 任务数组
 * @param stageId Stage ID
 * @param stageAttemptId Stage尝试ID
 * @param priority 优先级（通常为作业ID，数值越小优先级越高）
 * @param properties 调度属性
 * @param resourceProfileId 资源配置文件ID
 * @param shuffleId 关联的Shuffle ID（如果有）
 */
private[spark] class TaskSet(
    val tasks: Array[Task[_]],
    val stageId: Int,
    val stageAttemptId: Int,
    val priority: Int,
    val properties: Properties,
    val resourceProfileId: Int,
    val shuffleId: Option[Int]) {
  val id: String = s"$stageId.$stageAttemptId"

  override def toString: String = "TaskSet " + id

  // Identifier used in the structured logging framework.
  lazy val logId: MessageWithContext = {
    val hashMap = new java.util.HashMap[String, String]()
    hashMap.put(STAGE_ID.name, stageId.toString)
    hashMap.put(STAGE_ATTEMPT_ID.name, stageAttemptId.toString)
    MessageWithContext(id, hashMap)
  }
}

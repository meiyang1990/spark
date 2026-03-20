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

package org.apache.spark

/**
 * 任务状态枚举：LAUNCHING(启动中)、RUNNING(运行中)、FINISHED(完成)、
 * FAILED(失败)、KILLED(被杀)、LOST(丢失)
 */
private[spark] object TaskState extends Enumeration {

  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  // 终态集合（不可再恢复的状态）
  private val FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)

  type TaskState = Value

  /** 判断任务是否失败（LOST 或 FAILED） */
  def isFailed(state: TaskState): Boolean = (LOST == state) || (FAILED == state)

  /** 判断任务是否已结束（处于任何终态） */
  def isFinished(state: TaskState): Boolean = FINISHED_STATES.contains(state)
}

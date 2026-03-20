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

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * 任务本地性级别枚举，从高到低排列：
 * - PROCESS_LOCAL：进程本地，数据在同一JVM中（目前仅在TaskSetManager内部使用）
 * - NODE_LOCAL：节点本地，数据在同一台物理机上
 * - NO_PREF：无偏好，任务对位置没有特殊要求
 * - RACK_LOCAL：机架本地，数据在同一机架的不同节点上
 * - ANY：任意位置，可以在集群中的任何节点上运行
 */
@DeveloperApi
object TaskLocality extends Enumeration {
  // PROCESS_LOCAL 目前仅在 TaskSetManager 内部使用
  val PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY = Value

  type TaskLocality = Value

  /**
   * 判断在给定约束条件下，某个本地性级别是否被允许。
   * 即condition的级别不低于constraint（枚举值越小优先级越高）。
   */
  def isAllowed(constraint: TaskLocality, condition: TaskLocality): Boolean = {
    condition <= constraint
  }
}

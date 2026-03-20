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

import org.apache.spark.SparkContext

/**
 * 外部集群管理器接口，用于插件化扩展调度器。
 * 第三方集群管理器（如Mesos、Kubernetes等）通过实现此接口接入Spark的调度系统。
 */
private[spark] trait ExternalClusterManager {

  /**
   * 检查此集群管理器实例是否能为指定的master URL创建调度组件。
   * @param masterURL master URL
   * @return 如果能创建调度后端，返回true
   */
  def canCreate(masterURL: String): Boolean

  /**
   * 为给定的SparkContext创建任务调度器实例。
   * @param sc SparkContext
   * @param masterURL master URL
   * @return 负责任务处理的TaskScheduler
   */
  def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler

  /**
   * 为给定的SparkContext和调度器创建调度后端。
   * 此方法在通过 `ExternalClusterManager.createTaskScheduler()` 创建任务调度器之后调用。
   * @param sc SparkContext
   * @param masterURL master URL
   * @param scheduler 将与调度后端配合使用的TaskScheduler
   * @return 与TaskScheduler协同工作的SchedulerBackend
   */
  def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend

  /**
   * 初始化任务调度器和后端调度器。在调度组件创建完成后调用。
   * @param scheduler 负责任务处理的TaskScheduler
   * @param backend 与TaskScheduler协同工作的SchedulerBackend
   */
  def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit
}

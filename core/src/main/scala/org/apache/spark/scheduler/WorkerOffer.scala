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

import org.apache.spark.resource.ResourceProfile

/**
 * 表示Executor上可用的空闲资源。TaskScheduler在资源调度时使用此类来描述每个Executor的可用资源。
 *
 * @param executorId Executor的唯一标识
 * @param host Executor所在主机名
 * @param cores 可用CPU核心数
 * @param address 可选的hostPort字符串，当同一主机上启动多个Executor时提供比host更详细的信息
 * @param resources Executor上的自定义资源（如GPU、FPGA等）及其可用量
 * @param resourceProfileId 资源配置文件ID
 */
private[spark]
case class WorkerOffer(
    executorId: String,
    host: String,
    cores: Int,
    address: Option[String] = None,
    resources: ExecutorResourcesAmounts = ExecutorResourcesAmounts.empty,
    resourceProfileId: Int = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

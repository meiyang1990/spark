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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.ExecutorResourceInfo

/**
 * CoarseGrainedSchedulerBackend 使用的 Executor 数据分组。
 *
 * @param executorEndpoint 表示此 Executor 的 RPC 端点引用
 * @param executorAddress 此 Executor 的网络地址
 * @param executorHost 此 Executor 运行所在的主机名
 * @param freeCores Executor 上当前可用的空闲核心数
 * @param totalCores Executor 可用的总核心数
 * @param resourcesInfo 当前 Executor 上可用资源的信息
 * @param resourceProfileId 此 Executor 使用的 ResourceProfile ID
 * @param registrationTs 此 Executor 的注册时间戳
 * @param requestTs 此 Executor 最可能的请求时间
 */
private[cluster] class ExecutorData(
    val executorEndpoint: RpcEndpointRef,
    val executorAddress: RpcAddress,
    override val executorHost: String,
    var freeCores: Int,
    override val totalCores: Int,
    override val logUrlMap: Map[String, String],
    override val attributes: Map[String, String],
    override val resourcesInfo: Map[String, ExecutorResourceInfo],
    override val resourceProfileId: Int,
    val registrationTs: Long,
    val requestTs: Option[Long]
) extends ExecutorInfo(executorHost, totalCores, logUrlMap, attributes,
  resourcesInfo, resourceProfileId, Some(registrationTs), requestTs)

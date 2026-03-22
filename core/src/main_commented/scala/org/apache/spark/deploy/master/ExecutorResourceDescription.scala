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

package org.apache.spark.deploy.master

import org.apache.spark.resource.ResourceRequirement

/**
 * 不同资源配置文件下的Executor资源需求描述，用于Master节点的Executor调度分配
 *
 * @param coresPerExecutor 单个Executor需要的CPU核心数
 * @param memoryMbPerExecutor 单个Executor需要的内存大小，单位为MB
 * @param customResourcesPerExecutor 单个Executor需要的自定义资源（如GPU、FPGA等）请求列表
 */
private[spark] case class ExecutorResourceDescription(
    coresPerExecutor: Option[Int],
    memoryMbPerExecutor: Int,
    customResourcesPerExecutor: Seq[ResourceRequirement] = Seq.empty)
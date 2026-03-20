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
 * Executor components used with various cluster managers.
 * See [[org.apache.spark.executor.Executor]].
 *
 * Executor 相关组件包，与各种集群管理器（如 YARN、Kubernetes、Standalone）配合使用。
 * Executor 是 Spark 在 Worker 节点上执行任务的进程，负责：
 * - 运行应用程序的 Task
 * - 将数据缓存到内存或磁盘
 * - 向 Driver 汇报任务执行状态和指标
 * 详见 [[org.apache.spark.executor.Executor]]
 */
package object executor

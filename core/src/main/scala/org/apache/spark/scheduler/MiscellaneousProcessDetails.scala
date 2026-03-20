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

import org.apache.spark.annotation.{DeveloperApi, Since}

/**
 * :: DeveloperApi ::
 * 存储杂项进程信息，用于从调度器传递给SparkListener。
 * 例如用于跟踪非Executor/Driver的辅助进程信息。
 *
 * @param hostPort 进程的主机和端口
 * @param cores 进程使用的CPU核心数
 * @param logUrlInfo 日志URL信息映射
 */

@DeveloperApi
@Since("3.2.0")
class MiscellaneousProcessDetails(
    val hostPort: String,
    val cores: Int,
    val logUrlInfo: Map[String, String]) extends Serializable

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

package org.apache.spark.deploy

import java.net.URI

import org.apache.spark.resource.{ResourceProfile, ResourceRequirement, ResourceUtils}

/**
 * Spark应用描述信息类，用于在集群部署模式下向集群管理器传递应用基本信息和资源需求
 * 封装了应用名称、资源配置、启动命令、事件日志配置等核心元数据
 */
private[spark] case class ApplicationDescription(
    name: String,
    maxCores: Option[Int],
    command: Command,
    appUiUrl: String,
    defaultProfile: ResourceProfile,
    eventLogDir: Option[URI] = None,
    // short name of compression codec used when writing event logs, if any (e.g. lzf)
    eventLogCodec: Option[String] = None,
    // number of executors this application wants to start with,
    // only used if dynamic allocation is enabled
    initialExecutorLimit: Option[Int] = None,
    user: String = System.getProperty("user.name", "<unknown>")) {

  /** 获取每个执行器需要的内存大小（单位MB），默认值为1024MB */
  def memoryPerExecutorMB: Int = defaultProfile.getExecutorMemory.map(_.toInt).getOrElse(1024)

  /** 获取每个执行器分配的CPU核数，未配置则返回空 */
  def coresPerExecutor: Option[Int] = defaultProfile.getExecutorCores

  /** 获取每个执行器的自定义资源需求，按资源名称排序后返回 */
  def resourceReqsPerExecutor: Seq[ResourceRequirement] =
    ResourceUtils.executorResourceRequestToRequirement(
      defaultProfile.getCustomExecutorResources().values.toSeq.sortBy(_.resourceName))

  override def toString: String = "ApplicationDescription(" + name + ")"
}
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

package org.apache.spark.resource

import org.apache.spark.annotation.{Evolving, Since}

/**
 * ExecutorResourceRequest - Executor 资源请求
 * 
 * 与 ResourceProfile 配合使用，以编程方式指定 RDD 在 Stage 级别所需的 Executor 资源。
 * 
 * 主要功能：
 * 1. 指定 Executor 的资源需求（GPU、FPGA 等）
 * 2. 定义如何发现这些资源的具体信息
 * 
 * 参数说明：
 * - amount：资源数量（如每个 Executor 需要多少个 GPU）
 * - discoveryScript：可选的资源发现脚本。某些集群管理器（如 YARN）不会告诉 Spark
 *   分配的资源地址，需要在 Executor 启动时运行脚本来发现可用资源地址
 * - vendor：可选的供应商信息，某些集群管理器（如 Kubernetes）需要此参数
 * 
 * 使用示例（YARN 上分配 GPU）：
 * - resourceName: "gpu"
 * - amount: 每个 Executor 需要的 GPU 数量
 * - discoveryScript: 发现 GPU 地址的脚本（YARN 不会主动告知）
 * - vendor: 留空（仅 Kubernetes 需要）
 * 
 * 建议使用 ExecutorResourceRequests 类作为便捷 API。
 *
 * @param resourceName 资源名称
 * @param amount 请求的数量
 * @param discoveryScript 可选的资源发现脚本
 * @param vendor 可选的供应商名称
 */
@Evolving
@Since("3.1.0")
class ExecutorResourceRequest(
    val resourceName: String,
    val amount: Long,
    val discoveryScript: String = "",
    val vendor: String = "") extends Serializable {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ExecutorResourceRequest =>
        that.getClass == this.getClass &&
          that.resourceName == resourceName && that.amount == amount &&
        that.discoveryScript == discoveryScript && that.vendor == vendor
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Seq(resourceName, amount, discoveryScript, vendor).hashCode()

  override def toString(): String = {
    s"name: $resourceName, amount: $amount, script: $discoveryScript, vendor: $vendor"
  }
}

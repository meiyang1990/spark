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

import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}
import org.apache.spark.resource.ResourceInformation

/**
 * Master节点中Executor的描述信息，保存分配给Worker节点的Executor完整元数据
 * 用于Master管理整个集群中所有已分配的Executor资源和状态
 * @param id Executor在应用内的唯一ID
 * @param application 该Executor所属的应用信息
 * @param worker 该Executor分配到的Worker节点信息
 * @param cores 分配给该Executor的CPU核心数
 * @param memory 分配给该Executor的内存大小（单位：MB）
 * @param resources 分配给该Executor的自定义资源（如GPU、FPGA等），key为资源名称，value为资源信息
 * @param rpId 资源配置文件ID，用于标识该Executor使用的资源配置
 */
private[master] class ExecutorDesc(
    val id: Int,
    val application: ApplicationInfo,
    val worker: WorkerInfo,
    val cores: Int,
    val memory: Int,
    // resources(e.f. gpu/fpga) allocated to this executor
    // map from resource name to ResourceInformation
    val resources: Map[String, ResourceInformation],
    val rpId: Int) {

  // 当前Executor的运行状态，初始为启动中
  var state = ExecutorState.LAUNCHING

  /** 从网络传输过来的ExecutorDescription更新当前对象的可变状态 */
  def copyState(execDesc: ExecutorDescription): Unit = {
    state = execDesc.state
  }

  /** 获取Executor的全局唯一ID，格式为 应用ID/ExecutorID */
  def fullId: String = application.id + "/" + id

  override def equals(other: Any): Boolean = {
    other match {
      case info: ExecutorDesc =>
        // 通过全局唯一ID、WorkerID、资源分配信息判断两个Executor是否相等
        fullId == info.fullId &&
        worker.id == info.worker.id &&
        cores == info.cores &&
        memory == info.memory
      case _ => false
    }
  }

  override def toString: String = fullId

  override def hashCode: Int = toString.hashCode()
}
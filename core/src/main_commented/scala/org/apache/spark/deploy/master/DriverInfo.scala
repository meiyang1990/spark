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

import java.util.Date

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.Utils

/**
 * Spark Standalone集群Master中Driver信息的容器，存储Driver的基本描述、运行状态、分配资源和运行位置等信息
 */
private[deploy] class DriverInfo(
    val startTime: Long,
    val id: String,
    val desc: DriverDescription,
    val submitDate: Date)
  extends Serializable {

  // Driver当前运行状态，初始为已提交
  @transient var state: DriverState.Value = DriverState.SUBMITTED
  // 启动失败时存储异常信息
  @transient var exception: Option[Exception] = None
  // 当前分配运行该Driver的Worker节点
  @transient var worker: Option[WorkerInfo] = None
  // 分配给该Driver的计算资源(如GPU/FPGA等)，key为资源名称，value为资源信息
  private var _resources: Map[String, ResourceInformation] = Map.empty

  // 初始化瞬态字段
  init()

  // Java反序列化时重新初始化瞬态字段
  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  /**
   * 初始化Driver状态相关瞬态字段
   */
  private def init(): Unit = {
    state = DriverState.SUBMITTED
    worker = None
    exception = None
  }

  /**
   * 设置分配给Driver的计算资源
   * @param r 资源映射表，key为资源名称，value为资源信息
   */
  def withResources(r: Map[String, ResourceInformation]): Unit = _resources = r

  /**
   * 获取分配给Driver的所有计算资源
   * @return 资源映射表
   */
  def resources: Map[String, ResourceInformation] = _resources
}
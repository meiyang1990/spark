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

/**
 * 执行器描述类，用于Worker向Master传输执行器的状态信息，支持Master故障恢复时重建内部数据结构
 * 
 * 该类封装了执行器的核心元信息和当前状态，可序列化在网络中传输，在Master故障切换时，
 * Worker上报的该描述信息足够Master重建所有执行器的内部数据结构，完成故障恢复。
 * 
 * @param appId 所属应用的ID
 * @param execId 执行器ID
 * @param rpId 资源配置文件ID
 * @param cores 分配给执行器的CPU核数
 * @param memoryMb 分配给执行器的内存大小，单位MB
 * @param state 执行器当前状态
 */
private[deploy] class ExecutorDescription(
    val appId: String,
    val execId: Int,
    val rpId: Int,
    val cores: Int,
    val memoryMb: Int,
    val state: ExecutorState.Value)
  extends Serializable {

  override def toString: String =
    "ExecutorState(appId=%s, execId=%d, rpId=%d, cores=%d, memoryMb=%d state=%s)"
      .format(appId, execId, rpId, cores, memoryMb, state)
}
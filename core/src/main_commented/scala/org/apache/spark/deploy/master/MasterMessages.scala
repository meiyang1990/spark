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

/**
 * 文件级注释：Spark Standalone 集群 Master 节点内部通信消息定义
 * 本文件定义了 Master 节点与内部组件之间、选举代理与 Master 之间通信的所有消息类型
 * 用于 Master 节点状态管理、领导者选举、故障恢复和端口查询等核心流程
 */

/** 所有Master内部消息的根特质，标记可序列化 */
sealed trait MasterMessages extends Serializable

/** 集合了所有仅Master及其关联实体可见的内部通信消息 */
private[master] object MasterMessages {

  // LeaderElectionAgent 发送给 Master 的消息

  /** 消息：当前节点已被选举为集群领导者（Active Master） */
  case object ElectedLeader

  /** 消息：当前节点的领导者身份被撤销 */
  case object RevokedLeadership

  // Master 发送给自己的内部消息

  /** 定时检查消息：检查超时失联的Worker并清理 */
  case object CheckForWorkerTimeOut

  /**
   * 消息：开始故障恢复流程，携带持久化保存的应用和Worker信息
   * @param storedApps 持久化存储的应用信息列表
   * @param storedWorkers 持久化存储的Worker节点信息列表
   */
  case class BeginRecovery(storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo])

  /** 消息：故障恢复流程已完成 */
  case object CompleteRecovery

  /** 请求消息：查询Master各服务绑定的端口信息 */
  case object BoundPortsRequest

  /**
   * 响应消息：返回Master各服务绑定的端口信息
   * @param rpcEndpointPort RPC端点服务绑定端口
   * @param webUIPort Web UI绑定端口
   * @param restPort REST API绑定端口，可选
   */
  case class BoundPortsResponse(rpcEndpointPort: Int, webUIPort: Int, restPort: Option[Int])
}
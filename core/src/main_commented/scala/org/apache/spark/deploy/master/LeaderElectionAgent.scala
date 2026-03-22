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

import org.apache.spark.annotation.DeveloperApi

/**
 * 文件说明: Spark Standalone 集群主节点领导者选举接口定义，提供统一的选举代理抽象，支持不同实现方案
 * 核心功能: 定义领导者选举Agent的通用接口，支持ZooKeeper等分布式选举实现，以及单节点本地模式实现
 */

/**
 * :: DeveloperApi ::
 * 领导者选举代理抽象Trait，定义所有选举实现的统一接口，负责跟踪当前集群主节点状态
 */
@DeveloperApi
trait LeaderElectionAgent {
  /** 被选举的可领导节点实例，选举结果回调该实例 */
  val masterInstance: LeaderElectable
  /** 停止选举代理，默认空实现，子类可覆盖 */
  def stop(): Unit = {} // to avoid noops in implementations.
}

/**
 * :: DeveloperApi ::
 * 可被选举为主节点的Trait，定义领导者身份变更时的回调接口
 */
@DeveloperApi
trait LeaderElectable {
  /** 当前节点被选举为领导者时触发的回调 */
  def electedLeader(): Unit
  /** 当前节点领导者身份被撤销时触发的回调 */
  def revokedLeadership(): Unit
}

/**
 * 单节点模式领导者选举实现，适用于本地开发测试或单节点部署场景，本节点始终是领导者
 */
private[spark] class MonarchyLeaderAgent(val masterInstance: LeaderElectable)
  extends LeaderElectionAgent {
  // 初始化时直接将本节点标记为已当选领导者
  masterInstance.electedLeader()
}
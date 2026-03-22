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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Deploy.ZOOKEEPER_DIRECTORY

/**
 * 基于ZooKeeper实现的Standalone集群主节点选举代理
 * 核心职责：使用Curator框架的LeaderLatch实现高可用主节点选举，监听选举状态变更并通知Master节点
 */
private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
    conf: SparkConf) extends LeaderLatchListener with LeaderElectionAgent with Logging  {

  // ZooKeeper上选举节点的工作目录
  val workingDir = conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/spark") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  // 启动选举代理
  start()

  /**
   * 初始化ZooKeeper客户端和LeaderLatch，启动主节点选举流程
   */
  private def start(): Unit = {
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, workingDir)
    leaderLatch.addListener(this)
    leaderLatch.start()
  }

  /**
   * 停止选举代理，关闭LeaderLatch和ZooKeeper客户端连接
   */
  override def stop(): Unit = {
    leaderLatch.close()
    zk.close()
  }

  /**
   * 当选主节点后的回调处理
   */
  override def isLeader(): Unit = {
    synchronized {
      // 双重检查：防止竞态条件，确认当前仍然持有 leadership
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  /**
   * 失去主节点身份后的回调处理
   */
  override def notLeader(): Unit = {
    synchronized {
      // 双重检查：防止竞态条件，确认当前确实不再持有 leadership
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  /**
   * 更新主节点领导状态，状态发生变化时通知Master实例
   * @param isLeader 当前是否为主节点
   */
  private def updateLeadershipStatus(isLeader: Boolean): Unit = {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      masterInstance.revokedLeadership()
    }
  }

  /**
   * 领导状态枚举定义：标记当前节点是否是当选主节点
   */
  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }
}
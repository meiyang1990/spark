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

package org.apache.spark.deploy.client

import org.apache.spark.scheduler.ExecutorDecommissionInfo

/**
 * Standalone 模式下应用客户端的事件回调接口，定义了集群各类状态变化对应的回调方法。
 * 当前支持连接集群、断开连接、新增Executor、移除Executor、移除Worker五种核心事件。
 * 
 * 使用约定：回调方法实现中不应该进行阻塞操作，避免影响客户端事件处理流程。
 */
private[spark] trait StandaloneAppClientListener {
  /**
   * 客户端成功连接到Spark Standalone集群后的回调方法
   * @param appId 分配给当前应用的ID
   */
  def connected(appId: String): Unit

  /** 客户端与Master断开连接后的回调方法，断开可能是临时状态，后续会尝试故障切换到新Master */
  def disconnected(): Unit

  /**
   * 应用不可恢复故障的回调方法
   * @param reason 应用死亡的原因说明
   */
  def dead(reason: String): Unit

  /**
   * 集群新增Executor后的回调方法
   * @param fullId Executor完整ID
   * @param workerId Executor所在Worker节点ID
   * @param hostPort Worker节点的主机端口
   * @param cores 分配给该Executor的CPU核数
   * @param memory 分配给该Executor的内存大小
   */
  def executorAdded(
      fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int): Unit

  /**
   * Executor被移除后的回调方法（失败或被回收都会触发）
   * @param fullId 被移除Executor的完整ID
   * @param message 移除原因说明
   * @param exitStatus Executor退出状态码
   * @param workerHost Executor所在Worker节点的主机地址
   */
  def executorRemoved(
      fullId: String, message: String, exitStatus: Option[Int], workerHost: Option[String]): Unit

  /**
   * Executor被推荐停服下线后的回调方法
   * @param fullId 被停服Executor的完整ID
   * @param decommissionInfo Executor停服相关信息
   */
  def executorDecommissioned(fullId: String, decommissionInfo: ExecutorDecommissionInfo): Unit

  /**
   * Worker节点被移除后的回调方法
   * @param workerId 被移除Worker节点ID
   * @param host Worker节点主机地址
   * @param message 移除原因说明
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit
}
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

import scala.jdk.CollectionConverters._

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Deploy.ZOOKEEPER_URL

/**
 * Spark部署模块对接Apache Curator的工具类
 * 提供ZooKeeper客户端创建、节点创建、递归删除等通用操作，用于Spark高可用场景下的ZooKeeper操作
 */
private[spark] object SparkCuratorUtil extends Logging {

  // ZooKeeper连接超时时间，单位毫秒
  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  // ZooKeeper会话超时时间，单位毫秒
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  // 重试等待基础时间，单位毫秒
  private val RETRY_WAIT_MILLIS = 5000
  // 最大重连尝试次数
  private val MAX_RECONNECT_ATTEMPTS = 3

  /**
   * 根据Spark配置创建并启动一个新的Curator ZooKeeper客户端
   * @param conf Spark配置对象，包含ZooKeeper连接地址配置
   * @param zkUrlConf ZK连接地址对应的配置键名，默认使用部署模块的ZOOKEEPER_URL配置
   * @return 已启动的Curator ZooKeeper客户端实例
   */
  def newClient(
      conf: SparkConf,
      zkUrlConf: String = ZOOKEEPER_URL.key): CuratorFramework = {
    val ZK_URL = conf.get(zkUrlConf)
    val zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    zk
  }

  /**
   * 在ZooKeeper中创建指定路径的节点，自动创建所有不存在的父节点
   * 忽略节点已存在的异常，其他异常向上抛出
   * @param zk Curator ZooKeeper客户端实例
   * @param path 需要创建的ZooKeeper节点路径
   */
  def mkdir(zk: CuratorFramework, path: String): Unit = {
    if (zk.checkExists().forPath(path) == null) {
      try {
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
          // do nothing, ignore node existing exception.
        case e: Exception => throw e
      }
    }
  }

  /**
   * 递归删除ZooKeeper指定路径下的所有子节点和当前节点
   * 如果路径不存在则直接返回，不做任何操作
   * @param zk Curator ZooKeeper客户端实例
   * @param path 需要删除的ZooKeeper根路径
   */
  def deleteRecursive(zk: CuratorFramework, path: String): Unit = {
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path).asScala) {
        zk.delete().forPath(path + "/" + child)
      }
      zk.delete().forPath(path)
    }
  }
}
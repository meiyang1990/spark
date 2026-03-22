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

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.Deploy.{RECOVERY_COMPRESSION_CODEC, RECOVERY_DIRECTORY}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer

/**
 * 文件说明：Spark Standalone模式Master恢复模式工厂定义与实现，为不同恢复模式提供对应持久化引擎和领导选举代理实例
 */

/**
 * ::DeveloperApi::
 * 
 * Standalone恢复模式抽象工厂，为Spark Standalone集群主节点故障恢复定义恢复组件创建接口
 * 不同恢复模式（文件系统、ZooKeeper、RocksDB）都需要继承该类实现对应组件创建
 * 
 * @param conf Spark配置对象
 * @param serializer 序列化器，用于持久化状态序列化
 */
@DeveloperApi
abstract class StandaloneRecoveryModeFactory(conf: SparkConf, serializer: Serializer) {

  /**
   * 创建持久化引擎，用于将集群元数据（Worker、Driver等信息）持久化存储，供故障恢复使用
   * @return 持久化引擎实例
   */
  def createPersistenceEngine(): PersistenceEngine

  /**
   * 创建领导选举代理，负责主节点选举，决定哪个节点成为活动Master
   * @param master 可被选举的Master实例
   * @return 领导选举代理实例
   */
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent
}

/**
 * 基于文件系统的恢复模式工厂，单Master场景使用，不需要主节点选举，从本地文件系统恢复集群状态
 */
private[master] class FileSystemRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) with Logging {

  val recoveryDir = conf.get(RECOVERY_DIRECTORY)

  override def createPersistenceEngine(): PersistenceEngine = {
    logInfo(log"Persisting recovery state to directory: ${MDC(LogKeys.PATH, recoveryDir)}")
    val codec = conf.get(RECOVERY_COMPRESSION_CODEC).map(c => CompressionCodec.createCodec(conf, c))
    new FileSystemPersistenceEngine(recoveryDir, serializer, codec)
  }

  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }
}

/**
 * 基于RocksDB的恢复模式工厂，单Master场景使用，不需要主节点选举，从本地RocksDB恢复集群状态
 */
private[master] class RocksDBRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) with Logging {

  override def createPersistenceEngine(): PersistenceEngine = {
    val recoveryDir = conf.get(RECOVERY_DIRECTORY)
    logInfo(log"Persisting recovery state to directory: " +
      log"${MDC(LogKeys.PATH, recoveryDir)}")
    new RocksDBPersistenceEngine(recoveryDir, serializer)
  }

  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }
}

/**
 * 基于ZooKeeper的恢复模式工厂，多Master高可用场景使用，ZooKeeper负责主节点选举和集群状态持久化
 */
private[master] class ZooKeeperRecoveryModeFactory(conf: SparkConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  override def createPersistenceEngine(): PersistenceEngine = {
    new ZooKeeperPersistenceEngine(conf, serializer)
  }

  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new ZooKeeperLeaderElectionAgent(master, conf)
  }
}
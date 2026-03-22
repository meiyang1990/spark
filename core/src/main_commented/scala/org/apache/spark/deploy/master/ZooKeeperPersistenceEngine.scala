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

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.serializer.Serializer

/**
 * 基于ZooKeeper的持久化引擎，实现StandBy模式下Master状态的持久化存储
 * 在Spark HA模式下，将应用、worker等状态信息存储到ZooKeeper，供新当选的Active Master恢复使用
 * @param conf Spark配置对象
 * @param serializer 序列化器，用于对象序列化存储
 */
private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer)
  extends PersistenceEngine
  with Logging {

  // ZK中存储Master状态的工作目录
  private val workingDir = conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/spark") + "/master_status"
  // ZK客户端连接实例
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  // 创建工作目录，不存在时自动创建
  SparkCuratorUtil.mkdir(zk, workingDir)


  /**
   * 将对象持久化存储到ZooKeeper
   * @param name 存储节点名称
   * @param obj 要持久化的对象
   */
  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(workingDir + "/" + name, obj)
  }

  /**
   * 从ZooKeeper删除指定持久化对象
   * @param name 要删除的存储节点名称
   */
  override def unpersist(name: String): Unit = {
    zk.delete().forPath(workingDir + "/" + name)
  }

  /**
   * 从ZooKeeper读取指定前缀的所有持久化对象
   * @param prefix 对象名称前缀，用于过滤指定类型的对象
   * @tparam T 对象类型
   * @return 反序列化后的对象列表
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    zk.getChildren.forPath(workingDir).asScala
      .filter(_.startsWith(prefix)).flatMap(deserializeFromFile[T]).toSeq
  }

  /**
   * 关闭ZooKeeper客户端连接
   */
  override def close(): Unit = {
    zk.close()
  }

  /**
   * 将对象序列化后写入ZooKeeper指定路径
   * @param path ZK节点路径
   * @param value 要序列化的对象
   */
  private def serializeIntoFile(path: String, value: AnyRef): Unit = {
    // 序列化对象
    val serialized = serializer.newInstance().serialize(value)
    // 将序列化结果转换为字节数组
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    // 创建持久化ZK节点存储数据
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
  }

  /**
   * 从ZooKeeper指定文件反序列化读取对象
   * @param filename ZK节点名称
   * @param m 对象类型标记
   * @tparam T 对象类型
   * @return 反序列化后的对象Option，读取失败返回None
   */
  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    // 从ZK读取节点数据
    val fileData = zk.getData().forPath(workingDir + "/" + filename)
    try {
      // 反序列化对象并返回
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      // 读取异常，删除损坏节点并返回None
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(workingDir + "/" + filename)
        None
    }
  }
}
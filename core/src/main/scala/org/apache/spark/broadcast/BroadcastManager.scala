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

package org.apache.spark.broadcast

import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.commons.collections4.map.AbstractReferenceMap.ReferenceStrength
import org.apache.commons.collections4.map.ReferenceMap

import org.apache.spark.SparkConf
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.internal.Logging

/**
 * 广播变量管理器，负责创建和销毁广播变量。
 * 在 driver 和 executor 上都会创建一个实例来管理本地的广播变量。
 * @param isDriver 是否运行在 driver 端
 * @param conf Spark 配置对象
 */
private[spark] class BroadcastManager(
    val isDriver: Boolean, conf: SparkConf) extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  // 在构造时立即初始化工厂
  initialize()

  /**
   * 初始化广播工厂。由 SparkContext 或 Executor 在首次使用广播时调用
   */
  private def initialize(): Unit = {
    synchronized {
      if (!initialized) {
        // 创建 Torrent 广播工厂实例
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf)
        initialized = true
      }
    }
  }

  /** 停止广播管理器，释放相关资源 */
  def stop(): Unit = {
    broadcastFactory.stop()
  }

  // 下一个要创建的广播变量 ID（原子递增）
  private val nextBroadcastId = new AtomicLong(0)

  /**
   * 缓存已创建的广播变量值。用弱引用存储，允许垃圾回收释放空间。
   * 这是 broadcast 模块的内部缓存，用于提高本地访问性能。
   */
  private[broadcast] val cachedValues =
    Collections.synchronizedMap(
      new ReferenceMap(ReferenceStrength.HARD, ReferenceStrength.WEAK)
        .asInstanceOf[java.util.Map[Any, Any]]
    )

  /**
   * 创建新的广播变量。
   * @param value_ 要广播的值
   * @param isLocal 是否在本地模式（单 JVM）
   * @param serializedOnly 是否仅序列化（不在 driver 缓存原始值）
   * @return 新创建的 Broadcast 对象
   */
  def newBroadcast[T: ClassTag](
      value_ : T,
      isLocal: Boolean,
      serializedOnly: Boolean = false): Broadcast[T] = {
    // 分配唯一的广播变量 ID
    val bid = nextBroadcastId.getAndIncrement()
    // 特殊处理 Python 广播变量：将 ID 关联到 PythonBroadcast，
    // 以便其底层数据文件能根据该 ID 映射到 BroadcastBlockId（见 SPARK-28486）
    value_ match {
      case pb: PythonBroadcast =>
        pb.setBroadcastId(bid)
      case _ => // 其他类型无需特殊处理
    }
    // 委托给工厂创建实际的广播变量
    broadcastFactory.newBroadcast[T](value_, isLocal, bid, serializedOnly)
  }

  /**
   * 删除指定 ID 的广播变量在 executor 上的缓存
   * @param id 广播变量 ID
   * @param removeFromDriver 是否同时从 driver 删除
   * @param blocking 是否阻塞等待完成
   */
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}

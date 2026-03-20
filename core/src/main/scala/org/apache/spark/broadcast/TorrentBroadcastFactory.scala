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

import scala.reflect.ClassTag

import org.apache.spark.SparkConf

/**
 * BitTorrent 风格协议的广播实现工厂。通过分布式传输将广播数据高效地分发到各 Executor。
 * 详见 [[org.apache.spark.broadcast.TorrentBroadcast]]
 */
private[spark] class TorrentBroadcastFactory extends BroadcastFactory {

  // 初始化工厂（Torrent 广播无需特殊初始化）
  override def initialize(isDriver: Boolean, conf: SparkConf): Unit = { }

  /**
   * 创建新的 Torrent 广播变量实例
   */
  override def newBroadcast[T: ClassTag](
      value_ : T,
      isLocal: Boolean,
      id: Long,
      serializedOnly: Boolean = false): Broadcast[T] = {
    new TorrentBroadcast[T](value_, id, serializedOnly)
  }

  // 停止工厂（Torrent 广播无需特殊清理）
  override def stop(): Unit = { }

  /**
   * 删除指定 ID 对应的 Torrent 广播的所有持久化状态
   * @param id 广播变量 ID
   * @param removeFromDriver 是否从 driver 删除状态
   * @param blocking 是否阻塞等待完成
   */
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver, blocking)
  }
}

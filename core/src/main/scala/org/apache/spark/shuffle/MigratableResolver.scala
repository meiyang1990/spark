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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.BlockId

/**
 * :: Experimental ::
 * 实验性接口，允许 Spark 迁移 Shuffle 数据块。
 * 主要用于 Executor 下线时将 Shuffle 数据迁移到其他节点。
 */
@Experimental
@Since("3.1.0")
trait MigratableResolver {
  /**
   * 获取本地存储的 Shuffle 信息列表，用于数据块迁移。
   */
  def getStoredShuffles(): Seq[ShuffleBlockInfo]

  /**
   * 标记不应迁移的 Shuffle。
   */
  def addShuffleToSkip(shuffleId: Int): Unit = {}

  /**
   * 以流的方式写入 Shuffle 数据块，用于数据块迁移。
   * 实现者可支持 STORAGE_REMOTE_SHUFFLE_MAX_DISK 配置限制。
   */
  def putShuffleBlockAsStream(blockId: BlockId, serializerManager: SerializerManager):
      StreamCallbackWithID

  /**
   * 获取指定 Shuffle 和 Map 的数据块用于迁移。
   */
  def getMigrationBlocks(shuffleBlockInfo: ShuffleBlockInfo): List[(BlockId, ManagedBuffer)]
}

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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.storage.{BlockId, ShuffleMergedBlockId}

private[spark]
/**
 * Shuffle 数据块解析器接口。
 * 实现者知道如何根据逻辑 Shuffle 数据块标识符（map、reduce、shuffle）检索数据块数据。
 * 实现可以使用文件或文件片段来封装 Shuffle 数据。
 * BlockStore 使用此接口在检索 Shuffle 数据时抽象不同 Shuffle 实现。
 */
trait ShuffleBlockResolver {
  type ShuffleId = Int

  /**
   * 获取指定数据块的数据。
   *
   * @param dirs 如果为 None，使用 disk manager 的本地目录；否则从指定目录读取
   * @throws Exception 如果数据块数据不可用
   */
  def getBlockData(blockId: BlockId, dirs: Option[Array[String]] = None): ManagedBuffer

  /**
   * 获取指定 Shuffle Map 的 BlockId 列表。
   * 用于在关联的 Executor 被移除后从外部 Shuffle 服务删除 Shuffle 文件。
   */
  def getBlocksForShuffle(shuffleId: Int, mapId: Long): Seq[BlockId] = {
    Seq.empty
  }

  /**
   * 获取指定合并 Shuffle 数据块的数据，以多个 chunk 形式返回。
   */
  def getMergedBlockData(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): Seq[ManagedBuffer]

  /**
   * 获取指定合并 Shuffle 数据块的元数据。
   */
  def getMergedBlockMeta(
      blockId: ShuffleMergedBlockId,
      dirs: Option[Array[String]]): MergedBlockMeta

  def stop(): Unit
}

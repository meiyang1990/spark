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

package org.apache.spark.storage

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.BlockManagerMessages.UpdateBlockInfo

/**
 * :: DeveloperApi ::
 * 存储 BlockManager 中 Block 状态的信息。
 */
@DeveloperApi
case class BlockUpdatedInfo(
    blockManagerId: BlockManagerId,
    blockId: BlockId,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long)

private[spark] object BlockUpdatedInfo {

  // 从 UpdateBlockInfo 消息构造 BlockUpdatedInfo
  private[spark] def apply(updateBlockInfo: UpdateBlockInfo): BlockUpdatedInfo = {
    BlockUpdatedInfo(
      updateBlockInfo.blockManagerId,
      updateBlockInfo.blockId,
      updateBlockInfo.storageLevel,
      updateBlockInfo.memSize,
      updateBlockInfo.diskSize)
  }
}

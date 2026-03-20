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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerMessages.{BlockManagerHeartbeat, StopBlockManagerMaster}

/**
 * 将心跳处理从 BlockManagerMasterEndpoint 中分离出来，出于性能考虑。
 * 这个独立的端点专门处理 BlockManager 的心跳消息。
 */
private[spark] class BlockManagerMasterHeartbeatEndpoint(
    override val rpcEnv: RpcEnv,
    isLocal: Boolean,
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo])
  extends ThreadSafeRpcEndpoint with Logging {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case StopBlockManagerMaster =>
      stop()
      context.reply(true)

    case _ => // do nothing for unexpected events
  }

  /**
   * 收到心跳时返回 true，表示 driver 知道该 block manager。
   * 否则返回 false，表示 block manager 需要重新注册。
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }
}

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

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.network.buffer.ManagedBuffer

/**
 * 这个 [[ManagedBuffer]] 包装了从 [[BlockManager]] 检索的 [[BlockData]] 实例，
 * 以便在该 buffer 的引用释放后，相应 block 的读锁也能被释放。
 *
 * 如果 `dispose` 设置为 true，当 buffer 的引用计数降为零时，[[BlockData]] 会被销毁。
 *
 * 这实际上是一个包装器/桥接器，用于连接 BlockManager 的读锁概念和网络层的 retain/release 计数概念。
 */
private[storage] class BlockManagerManagedBuffer(
    blockInfoManager: BlockInfoManager,
    blockId: BlockId,
    data: BlockData,
    dispose: Boolean,
    unlockOnDeallocate: Boolean = true) extends ManagedBuffer {

  private val refCount = new AtomicInteger(1)

  override def size(): Long = data.size

  override def nioByteBuffer(): ByteBuffer = data.toByteBuffer()

  override def createInputStream(): InputStream = data.toInputStream()

  override def convertToNetty(): Object = data.toNetty()

  override def convertToNettyForSsl(): Object = data.toNettyForSsl()

  override def retain(): ManagedBuffer = {
    refCount.incrementAndGet()
    val locked = blockInfoManager.lockForReading(blockId, blocking = false)
    assert(locked.isDefined)
    this
 }

  override def release(): ManagedBuffer = {
    if (unlockOnDeallocate) {
      blockInfoManager.unlock(blockId)
    }
    if (refCount.decrementAndGet() == 0 && dispose) {
      data.dispose()
    }
    this
  }
}

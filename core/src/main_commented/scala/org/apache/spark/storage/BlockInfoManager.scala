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

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{Condition, Lock}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.google.common.collect.{ConcurrentHashMultiset, ImmutableMultiset}
import com.google.common.util.concurrent.Striped

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging


/**
 * 单个存储块的元数据跟踪类，保存块的存储级别、锁状态等信息
 * 
 * 本类实例本身不是线程安全的，所有并发保护由 BlockInfoManager 中的锁实现
 *
 * @param level 块请求存储级别，不是实际生效存储级别（例如 MEMORY_AND_DISK 不代表块实际已经存在内存中）
 * @param classTag 块的ClassTag，用于选择序列化器
 * @param tellMaster 块状态变更是否需要上报给Master，大部分块为true，广播块为false
 */
private[storage] class BlockInfo(
    val level: StorageLevel,
    val classTag: ClassTag[_],
    val tellMaster: Boolean) {

  /**
   * 块的大小，单位字节
   */
  def size: Long = _size
  def size_=(s: Long): Unit = {
    _size = s
    checkInvariants()
  }
  private[this] var _size: Long = 0

  /**
   * 当前该块被读锁定的次数
   */
  def readerCount: Int = _readerCount
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    checkInvariants()
  }
  private[this] var _readerCount: Int = 0

  /**
   * 当前持有该块写锁的任务尝试ID，如果写锁未被持有为 [[BlockInfo.NO_WRITER]]，
   * 如果写锁被非任务代码持有为 [[BlockInfo.NON_TASK_WRITER]]
   */
  def writerTask: Long = _writerTask
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    checkInvariants()
  }
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER

  private def checkInvariants(): Unit = {
    // 读锁计数必须非负
    assert(_readerCount >= 0)
    // 块不能同时被读锁定和写锁定，读计数和写锁互斥
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER)
  }

  checkInvariants()
}

/**
 * BlockInfo的包装类，为每个块关联对应的锁和条件变量，实现细粒度并发控制
 */
private class BlockInfoWrapper(
    val info: BlockInfo,
    private val lock: Lock,
    private val condition: Condition) {
  def this(info: BlockInfo, lock: Lock) = this(info, lock, lock.newCondition())

  /**
   * 在持有锁的情况下执行指定操作，执行完成后自动释放锁
   * @param f 需要执行的函数，参数为BlockInfo和条件变量
   * @return 函数执行结果
   */
  def withLock[T](f: (BlockInfo, Condition) => T): T = {
    lock.lock()
    try f(info, condition) finally {
      lock.unlock()
    }
  }

  /**
   * 尝试获取锁，如果获取成功则执行指定操作，执行完成后释放锁；获取失败直接返回
   * @param f 需要执行的函数，参数为BlockInfo和条件变量
   */
  def tryLock(f: (BlockInfo, Condition) => Unit): Unit = {
    if (lock.tryLock()) {
      try f(info, condition) finally {
        lock.unlock()
      }
    }
  }
}

private[storage] object BlockInfo {

  /**
   * 特殊常量，表示块没有被写锁定
   */
  val NO_WRITER: Long = -1

  /**
   * 特殊常量，表示写锁被非任务线程持有（例如Driver线程或单元测试代码）
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
 * BlockManager的核心组件，负责跟踪所有块的元数据并管理块的读写锁
 *
 * 本类实现的锁接口是读写锁，每次锁获取都会自动关联到当前运行任务，任务完成或失败时会自动释放所有锁
 *
 * 本类是线程安全的
 */
private[storage] class BlockInfoManager(trackingCacheVisibility: Boolean = false) extends Logging {

  private type TaskAttemptId = Long

  /**
   * 用于按BlockId查询块元数据。条目通过lockNewBlockForWriting()原子性地插入，不存在才创建，通过removeBlock()删除
   */
  private[this] val blockInfoWrappers = new ConcurrentHashMap[BlockId, BlockInfoWrapper]

  // 按类型缓存BlockId到分组的映射，避免删除时遍历全表的O(n)扫描
  private[this] val rddToBlockIds =
    new ConcurrentHashMap[Int, ConcurrentHashMap.KeySetView[BlockId, java.lang.Boolean]]()
  private[this] val broadcastToBlockIds =
    new ConcurrentHashMap[Long, ConcurrentHashMap.KeySetView[BlockId, java.lang.Boolean]]()
  private[this] val sessionToBlockIds =
    new ConcurrentHashMap[String, ConcurrentHashMap.KeySetView[BlockId, java.lang.Boolean]]()

  /**
   * 记录存储管理器中不可见的RDD块，当块被标记为可见或被删除时移除条目
   */
  private[this] val invisibleRDDBlocks = new mutable.HashSet[RDDBlockId]

  /**
   * 分条锁，用于控制对块元数据的多线程访问
   *
   * 不直接在BlockInfo对象上做同步是为了避免lockNewBlockForReading方法中的竞态条件。
   * 当该方法成功返回后，需要保证传入的BlockInfo已经被持久化到管理器，可以安全修改。
   * 只有为每个BlockId维护比BlockInfo生命周期更长的独立锁才能保证这一点。
   */
  private[this] val locks = Striped.lock(1024)

  /**
   * 按任务尝试ID跟踪该任务所有持有写锁的块
   */
  private[this] val writeLocksByTask = new ConcurrentHashMap[TaskAttemptId, util.Set[BlockId]]

  /**
   * 为指定任务尝试ID注册写锁，如果任务不存在则创建新条目
   * @param taskAttemptId 任务尝试ID
   * @param blockId 目标块ID
   */
  private def registerWriteLockForTask(taskAttemptId: TaskAttemptId, blockId: BlockId): Unit = {
    writeLocksByTask.compute(taskAttemptId, (_, blockIds) => {
      val newBlockIds = if (blockIds == null) {
        util.Collections.synchronizedSet(new util.HashSet[BlockId])
      } else {
        blockIds
      }
      newBlockIds.add(blockId)
      newBlockIds
    })
  }

  /**
   * 注销指定任务尝试ID对块的写锁，任务条目后续会被releaseAllLocksForTask清理
   * @param taskAttemptId 任务尝试ID
   * @param blockId 目标块ID
   */
  private def unregisterWriteLockForTask(taskAttemptId: TaskAttemptId, blockId: BlockId): Unit = {
    writeLocksByTask.computeIfPresent(taskAttemptId, (_, blockIds) => {
      blockIds.remove(blockId)
      blockIds
    })
  }

  /**
   * 按任务尝试ID跟踪该任务所有持有读锁的块，同时记录每个块被锁定的次数（因为读锁可重入）
   */
  private[this] val readLocksByTask =
    new ConcurrentHashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]

  // ----------------------------------------------------------------------------------------------

  // 初始化特殊任务尝试ID（非任务写者）
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  // 仅暴露给测试使用
  private[storage] def containsInvisibleRDDBlock(blockId: RDDBlockId): Boolean = {
    invisibleRDDBlocks.synchronized {
      invisibleRDDBlocks.contains(blockId)
    }
  }

  /**
   * 检查指定RDD块对查询是否可见
   * @param blockId RDD块ID
   * @return 是否可见
   */
  private[spark] def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    if (trackingCacheVisibility) {
      invisibleRDDBlocks.synchronized {
        blockInfoWrappers.containsKey(blockId) && !invisibleRDDBlocks.contains(blockId)
      }
    } else {
      // 关闭缓存可见性跟踪时，所有块默认可见
      true
    }
  }

  /**
   * 尝试将指定RDD块标记为可见
   * @param blockId RDD块ID
   */
  private[spark] def tryMarkBlockAsVisible(blockId: RDDBlockId): Unit = {
    if (trackingCacheVisibility) {
      invisibleRDDBlocks.synchronized {
        invisibleRDDBlocks.remove(blockId)
      }
    }
  }

  /**
   * 在任务启动时调用，向BlockInfoManager注册该任务，必须在调用本类其他方法之前调用
   * @param taskAttemptId 任务尝试ID
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = {
    writeLocksByTask.putIfAbsent(taskAttemptId, util.Collections.synchronizedSet(new util.HashSet))
    readLocksByTask.putIfAbsent(taskAttemptId, ConcurrentHashMultiset.create())
  }

  /**
   * 获取当前任务的任务尝试ID，如果调用方是非任务线程则返回BlockInfo.NON_TASK_WRITER
   * @return 当前任务尝试ID
   */
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * 锁获取的辅助方法，处理阻塞和非阻塞两种获取模式
   * @param blockId 目标块ID
   * @param blocking 是否阻塞等待锁
   * @param acquireCheck 检查是否可以获取锁的函数，返回true表示可以获取
   * @return 获取成功返回Some(BlockInfo)，失败返回None
   */
  private def acquireLock(
      blockId: BlockId,
      blocking: Boolean)(
      f: BlockInfo => Boolean): Option[BlockInfo] = {
    var done = false
    var result: Option[BlockInfo] = None
    while (!done) {
      val wrapper = blockInfoWrappers.get(blockId)
      if (wrapper == null) {
        done = true
      } else {
        wrapper.withLock { (info, condition) =>
          if (f(info)) {
            result = Some(info)
            done = true
          } else if (!blocking) {
            done = true
          } else {
            condition.await()
          }
        }
      }
    }
    result
  }

  /**
   * 在块对应BlockInfo和Condition上执行指定函数，执行过程持有块锁，块不存在则抛出异常
   * @param blockId 目标块ID
   * @param f 需要执行的函数
   * @return 函数执行结果
   */
  private def blockInfo[T](blockId: BlockId)(f: (BlockInfo, Condition) => T): T = {
    val wrapper = blockInfoWrappers.get(blockId)
    if (wrapper == null) {
      throw SparkCoreErrors.blockDoesNotExistError(blockId)
    }
    wrapper.withLock(f)
  }

  /**
   * 为块获取读锁并返回其元数据
   *
   * 如果块已经被其他任务读锁定，则立即授予当前任务读锁，并增加读计数
   *
   * 如果块已经被其他任务写锁定，阻塞模式下会阻塞等待写锁释放，非阻塞模式直接返回
   *
   * 单个任务可以多次对同一个块加读锁，每次加锁都需要单独释放
   *
   * @param blockId 目标块ID
   * @param blocking 是否阻塞等待锁，默认为true
   * @return None表示块不存在或已被删除（未持有锁），Some(BlockInfo)表示已成功获取读锁
   */
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId trying to acquire read lock for $blockId")
    acquireLock(blockId, blocking) { info =>
      val acquire = info.writerTask == BlockInfo.NO_WRITER
      if (acquire) {
        info.readerCount += 1
        readLocksByTask.get(taskAttemptId).add(blockId)
        logTrace(s"Task $taskAttemptId acquired read lock for $blockId")
      }
      acquire
    }
  }

  /**
   * 为块获取写锁并返回其元数据
   *
   * 如果块已经被其他任务读锁定或写锁定，阻塞模式下会阻塞等待锁释放，非阻塞模式直接返回
   *
   * @param blockId 目标块ID
   * @param blocking 是否阻塞等待锁，默认为true
   * @return None表示块不存在或已被删除（未持有锁），Some(BlockInfo)表示已成功获取写锁
   */
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId trying to acquire write lock for $blockId")
    acquireLock(blockId, blocking) { info =>
      val acquire = info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0
      if (acquire) {
        info.writerTask = taskAttemptId
        registerWriteLockForTask(taskAttemptId, blockId)
        logTrace(s"Task $taskAttemptId acquired write lock for $blockId")
      }
      acquire
    }
  }

  /**
   * 检查当前任务是否持有指定块的写锁，如果不持有则抛出异常，否则返回块元数据
   * @param blockId 目标块ID
   * @return 块元数据BlockInfo
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = {
    val taskAttemptId = currentTaskAttemptId
    blockInfo(blockId) { (info, _) =>
      if (info.writerTask != taskAttemptId) {
        throw SparkCoreErrors.taskHasNotLockedBlockError(currentTaskAttemptId, blockId)
      } else {
        info
      }
    }
  }

  /**
   * 获取块元数据，不获取任何锁。该方法仅暴露给BlockManager.getStatus()使用，不应该在类外其他代码调用
   * @param blockId 目标块ID
   * @return 块元数据Option
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = {
    val wrapper = blockInfoWrappers.get(blockId)
    if (wrapper != null) {
      Some(wrapper.info)
    } else {
      None
    }
  }

  /**
   * 将块的独占写锁降级为共享读锁
   * @param blockId 目标块ID
   */
  def downgradeLock(blockId: BlockId): Unit = {
    val taskAttemptId = currentTaskAttemptId
    logTrace(s"Task $taskAttemptId downgrading write lock for $blockId")
    blockInfo(blockId) { (info, _) =>
      require(info.writerTask == taskAttemptId,
        s"Task $taskAttemptId tried to downgrade a write lock that it does not hold on" +
          s" block $blockId")
      unlock(blockId)
      val lockOutcome = lockForReading(blockId, blocking = false)
      assert(lockOutcome.isDefined)
    }
  }

  /**
   * 释放指定块上的锁
   * 如果TaskContext没有正确传播到任务的所有子线程，将无法从TaskContext获取任务ID，因此需要显式传入任务ID
   * 参见SPARK-18406
   * @param blockId 目标块ID
   * @param taskAttemptIdOption 可选显式指定任务尝试ID
   */
  def unlock(blockId: BlockId, taskAttemptIdOption: Option[TaskAttemptId] = None): Unit = {
    val taskAttemptId = taskAttemptId
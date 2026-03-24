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

import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.Map
import scala.collection.mutable

import sun.misc.Unsafe

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.Utils

/**
 * 单个BlockManager的存储状态信息类
 *
 * 该类假设BlockId和BlockStatus是不可变的，消费者无法修改原始信息，访问操作非线程安全
 * 用于记录BlockManager上RDD块和非RDD块的存储使用情况，提供内存和磁盘使用量统计
 *
 * @param blockManagerId 对应的BlockManager标识
 * @param maxMemory 该BlockManager可使用的最大内存
 * @param maxOnHeapMem 堆内存最大限制，None表示未配置
 * @param maxOffHeapMem 堆外内存最大限制，None表示未配置
 */
private[spark] class StorageStatus(
    val blockManagerId: BlockManagerId,
    val maxMemory: Long,
    val maxOnHeapMem: Option[Long],
    val maxOffHeapMem: Option[Long]) {

  /**
   * 存储在此BlockManager上的块的内部表示
   */
  private val _rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]

  private case class RddStorageInfo(memoryUsage: Long, diskUsage: Long, level: StorageLevel)
  private val _rddStorageInfo = new mutable.HashMap[Int, RddStorageInfo]

  private case class NonRddStorageInfo(var onHeapUsage: Long, var offHeapUsage: Long,
      var diskUsage: Long)
  private val _nonRddStorageInfo = NonRddStorageInfo(0L, 0L, 0L)

  /**
   *  使用初始块集合创建存储状态，不修改原始块数据
   */
  def this(
      bmid: BlockManagerId,
      maxMemory: Long,
      maxOnHeapMem: Option[Long],
      maxOffHeapMem: Option[Long],
      initialBlocks: Map[BlockId, BlockStatus]) = {
    this(bmid, maxMemory, maxOnHeapMem, maxOffHeapMem)
    initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
  }

  /**
   * 获取该BlockManager存储的所有块
   *
   * @note 该操作开销较大，需要克隆底层映射并拼接，对于包含、获取、大小等常用操作有更高效的替代方案
   */
  def blocks: Map[BlockId, BlockStatus] = _nonRddBlocks ++ rddBlocks

  /**
   * 获取该BlockManager存储的所有RDD块
   *
   * @note 该操作开销较大，需要克隆底层映射并拼接，对于获取RDD占用内存、磁盘、堆外内存大小等常用操作有更高效的替代方案
   */
  def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }

  /**
   * 添加指定块到存储状态，若块已存在则覆盖原有信息
   */
  private[spark] def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    updateStorageInfo(blockId, blockStatus)
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
  }

  /**
   * O(1)时间复杂度获取指定块的状态
   *
   * @note 比`this.blocks.get`快很多，后者是O(blocks)时间复杂度
   */
  def getBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).flatMap(_.get(blockId))
      case _ =>
        _nonRddBlocks.get(blockId)
    }
  }

  /** 返回该BlockManager可以使用的最大内存 */
  def maxMem: Long = maxMemory

  /** 返回该BlockManager剩余可用内存 */
  def memRemaining: Long = maxMem - memUsed

  /** 返回该BlockManager已使用内存 */
  def memUsed: Long = onHeapMemUsed.getOrElse(0L) + offHeapMemUsed.getOrElse(0L)

  /** 返回该BlockManager剩余可用堆内存 */
  def onHeapMemRemaining: Option[Long] =
    for (m <- maxOnHeapMem; o <- onHeapMemUsed) yield m - o

  /** 返回该BlockManager剩余可用堆外内存 */
  def offHeapMemRemaining: Option[Long] =
    for (m <- maxOffHeapMem; o <- offHeapMemUsed) yield m - o

  /** 返回该BlockManager已使用堆内存 */
  def onHeapMemUsed: Option[Long] = onHeapCacheSize.map(_ + _nonRddStorageInfo.onHeapUsage)

  /** 返回该BlockManager已使用堆外内存 */
  def offHeapMemUsed: Option[Long] = offHeapCacheSize.map(_ + _nonRddStorageInfo.offHeapUsage)

  /** 返回RDD缓存使用的堆内存 */
  def onHeapCacheSize: Option[Long] = maxOnHeapMem.map { _ =>
    _rddStorageInfo.collect {
      case (_, storageInfo) if !storageInfo.level.useOffHeap => storageInfo.memoryUsage
    }.sum
  }

  /** 返回RDD缓存使用的堆外内存 */
  def offHeapCacheSize: Option[Long] = maxOffHeapMem.map { _ =>
    _rddStorageInfo.collect {
      case (_, storageInfo) if storageInfo.level.useOffHeap => storageInfo.memoryUsage
    }.sum
  }

  /** 返回该BlockManager已使用磁盘空间 */
  def diskUsed: Long = _nonRddStorageInfo.diskUsage + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum

  /** O(1)时间复杂度返回指定RDD在该BlockManager上使用的磁盘空间 */
  def diskUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_.diskUsage).getOrElse(0L)

  /**
   * 更新存储统计信息，考虑该块已有的状态
   */
  private def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit = {
    // 获取块原有状态，不存在则使用空状态
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    // 计算内存和磁盘使用量变化
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val level = newBlockStatus.storageLevel

    // 从原有信息计算当前使用量
    val (oldMem, oldDisk) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case RddStorageInfo(mem, disk, _) => (mem, disk) }
          .getOrElse((0L, 0L))
      case _ if !level.useOffHeap =>
        (_nonRddStorageInfo.onHeapUsage, _nonRddStorageInfo.diskUsage)
      case _ =>
        (_nonRddStorageInfo.offHeapUsage, _nonRddStorageInfo.diskUsage)
    }
    // 计算新的使用量，确保不小于0
    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)

    // 更新统计信息
    blockId match {
      case RDDBlockId(rddId, _) =>
        // 如果RDD不再持久化，移除其统计信息
        if (newMem + newDisk == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = RddStorageInfo(newMem, newDisk, level)
        }
      case _ =>
        if (!level.useOffHeap) {
          _nonRddStorageInfo.onHeapUsage = newMem
        } else {
          _nonRddStorageInfo.offHeapUsage = newMem
        }
        _nonRddStorageInfo.diskUsage = newDisk
    }
  }
}

/**
 * 存储相关对象的辅助工具类
 * 提供ByteBuffer资源清理、外部Shuffle服务端口获取等工具方法
 */
private[spark] object StorageUtils extends Logging {

  // 使用Unsafe API实现的ByteBuffer清理器，用于手动释放直接内存和内存映射文件内存
  private val bufferCleaner: ByteBuffer => Unit = {
    val unsafeField = classOf[Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    val unsafe = unsafeField.get(null).asInstanceOf[Unsafe]
    buffer: ByteBuffer => unsafe.invokeCleaner(buffer)
  }

  /**
   * 尝试清理直接内存或内存映射ByteBuffer
   * 该方法使用不安全的Sun API手动释放堆外内存，释放后再访问该buffer会引发错误
   * 直接缓冲区和内存映射缓冲区不会对GC造成压力，等待GC可能导致堆外内存耗尽或文件句柄泄漏，标准API没有手动释放能力，因此使用该方案
   *
   * @param buffer 需要清理的ByteBuffer对象
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Disposing of $buffer")
      bufferCleaner(buffer)
    }
  }

  /**
   * 获取外部Shuffle服务的端口号
   * 在YARN模式下，端口可能已经通过Hadoop配置设置，因为服务由YARN NodeManager启动
   *
   * @param conf Spark配置对象
   * @return 外部Shuffle服务端口号
   */
  def externalShuffleServicePort(conf: SparkConf): Int = {
    // 优先从Spark或YARN配置获取端口
    val tmpPort = Utils.getSparkOrYarnConfig(conf, config.SHUFFLE_SERVICE_PORT.key,
      config.SHUFFLE_SERVICE_PORT.defaultValueString).toInt
    // 测试场景下YARN配置会将端口设为0让YARN自动分配空闲端口，此时回退到Spark配置获取实际端口
    if (tmpPort == 0) {
      conf.get(config.SHUFFLE_SERVICE_PORT.key).toInt
    } else {
      tmpPort
    }
  }
}
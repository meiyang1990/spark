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

package org.apache.spark.deploy.history

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.History
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.History.HybridStoreDiskBackend.ROCKSDB
import org.apache.spark.status.KVUtils
import org.apache.spark.status.KVUtils._
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.util.kvstore.KVStore

/**
 * 文件级注释：Spark历史服务器磁盘空间管理器，负责管理已完成应用的状态存储磁盘使用量，
 * 当磁盘使用超过配置阈值时自动淘汰不活跃的应用存储以释放空间。
 * 不保证使用量永远不会超过阈值，但最终会回落到阈值以下。
 */

/**
 * 历史服务器磁盘空间管理器，跟踪SHS的应用状态存储磁盘使用，当使用超过阈值时自动淘汰不活跃应用释放空间
 * 
 * @param conf Spark配置
 * @param path 存储应用数据的根目录
 * @param listing 应用列表存储，用于持久化存储使用量信息
 * @param clock 时钟实例用于时间计算
 */
private class HistoryServerDiskManager(
    conf: SparkConf,
    path: File,
    listing: KVStore,
    clock: Clock) extends Logging {

  // 应用存储目录
  private val appStoreDir = new File(path, "apps")
  if (!appStoreDir.isDirectory() && !appStoreDir.mkdir()) {
    throw new IllegalArgumentException(s"Failed to create app directory ($appStoreDir).")
  }
  // 根据配置的磁盘后端选择对应文件扩展名
  private val extension =
    if (conf.get(History.HYBRID_STORE_DISK_BACKEND) == ROCKSDB.toString) ".rdb" else ".ldb"

  // 临时存储目录，用于存放正在加载过程中的应用状态
  private val tmpStoreDir = new File(path, "temp")
  if (!tmpStoreDir.isDirectory() && !tmpStoreDir.mkdir()) {
    throw new IllegalArgumentException(s"Failed to create temp directory ($tmpStoreDir).")
  }

  // 最大允许的磁盘使用量阈值，来自配置
  private val maxUsage = conf.get(MAX_LOCAL_DISK_USAGE)
  // 当前总磁盘使用量（包含已提交和正在租赁的空间）
  private val currentUsage = new AtomicLong(0L)
  // 已提交到最终存储的磁盘使用量
  private val committedUsage = new AtomicLong(0L)
  // 当前正在被使用的应用存储及其大小，key为应用ID-尝试ID对
  private val active = new HashMap[(String, Option[String]), Long]()

  /**
   * 初始化磁盘管理器，计算当前磁盘使用量，清理遗留临时文件，修复元数据
   */
  def initialize(): Unit = {
    updateUsage(sizeOf(appStoreDir), committed = true)

    // 启动时清理上次退出遗留的临时存储目录
    tmpStoreDir.listFiles().foreach(Utils.deleteQuietly)

    // 移除已经被外部删除的应用存储元数据
    val (existences, orphans) = KVUtils.viewToSeq(listing
      .view(classOf[ApplicationStoreInfo]))
      .partition { info =>
        new File(info.path).exists()
      }

    orphans.foreach { info =>
      listing.delete(info.getClass(), info.path)
    }

    // 重启后根据实际目录大小更新元数据，因为KVDir存储合并可能改变大小，保证统计准确
    existences.foreach { info =>
      val fileSize = sizeOf(new File(info.path))
      if (fileSize != info.size) {
        listing.write(info.copy(size = fileSize))
      }
    }

    logInfo(log"Initialized disk manager:" +
      log" current usage = ${MDC(NUM_BYTES_CURRENT, Utils.bytesToString(currentUsage.get()))}," +
      log" max usage = ${MDC(NUM_BYTES_MAX, Utils.bytesToString(maxUsage))}")
  }

  /**
   * 租赁空间，为新加载的应用状态预分配磁盘空间，如果空间不足会淘汰不活跃应用。
   * 租赁期间数据先写到临时目录，此时应用还未对查询可见。
   * 
   * @param eventLogSize 事件日志大小
   * @param isCompressed 事件日志是否压缩
   * @return 租赁对象，用于后续提交或回滚
   */
  def lease(eventLogSize: Long, isCompressed: Boolean = false): Lease = {
    val needed = approximateSize(eventLogSize, isCompressed)
    makeRoom(needed)

    val tmp = Utils.createTempDir(tmpStoreDir.getPath(), "appstore")
    Utils.chmod700(tmp)

    updateUsage(needed)
    val current = currentUsage.get()
    if (current > maxUsage) {
      logInfo(log"Lease of ${MDC(NUM_BYTES, Utils.bytesToString(needed))} may cause" +
        log" usage to exceed max (${MDC(NUM_BYTES_CURRENT, Utils.bytesToString(current))}" +
        log" > ${MDC(NUM_BYTES_MAX, Utils.bytesToString(maxUsage))})")
    }

    new Lease(tmp, needed)
  }

  /**
   * 获取已加载完成的应用存储，标记该应用为活跃状态，不会被淘汰。
   * 
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @return 应用存储目录，如果不存在返回None
   */
  def openStore(appId: String, attemptId: Option[String]): Option[File] = {
    var newSize: Long = 0
    val storePath = active.synchronized {
      val path = appStorePath(appId, attemptId)
      if (path.isDirectory()) {
        newSize = sizeOf(path)
        active(appId -> attemptId) = newSize
        Some(path)
      } else {
        None
      }
    }

    storePath.foreach { path =>
      updateApplicationStoreInfo(appId, attemptId, newSize)
    }

    storePath
  }

  /**
   * 释放应用存储，标记应用为不活跃，可以被淘汰，支持主动删除。
   * 
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @param delete 是否立即删除存储
   */
  def release(appId: String, attemptId: Option[String], delete: Boolean = false): Unit = {
    // 即使只读访问也可能修改存储文件结构，关闭时需要更新大小统计
    val oldSizeOpt = active.synchronized {
      active.remove(appId -> attemptId)
    }

    oldSizeOpt.foreach { oldSize =>
      val path = appStorePath(appId, attemptId)
      updateUsage(-oldSize, committed = true)
      if (path.isDirectory()) {
        if (delete) {
          deleteStore(path)
        } else {
          val newSize = sizeOf(path)
          val newInfo = listing.read(classOf[ApplicationStoreInfo], path.getAbsolutePath())
            .copy(size = newSize)
          listing.write(newInfo)
          updateUsage(newSize, committed = true)
        }
      }
    }
  }

  /**
   * 根据事件日志大小估算应用状态存储的大小，基于经验的近似计算。
   * 
   * @param eventLogSize 事件日志大小
   * @param isCompressed 事件日志是否压缩
   * @return 估算的存储大小
   */
  def approximateSize(eventLogSize: Long, isCompressed: Boolean): Long = {
    if (isCompressed) {
      // 压缩日志解压后存储，最终存储大小约为原始压缩日志的2倍
      eventLogSize * 2
    } else {
      // 非压缩日志，状态存储大小约为日志大小的一半
      eventLogSize / 2
    }
  }

  /**
   * 当前可用空闲磁盘空间
   * @return 空闲字节数，最小为0
   */
  def free(): Long = {
    math.max(maxUsage - currentUsage.get(), 0L)
  }

  /**
   * 当前已提交到最终存储的总使用量
   * @return 已使用字节数
   */
  def committed(): Long = committedUsage.get()

  // 删除应用存储并更新元数据
  private def deleteStore(path: File): Unit = {
    Utils.deleteRecursively(path)
    listing.delete(classOf[ApplicationStoreInfo], path.getAbsolutePath())
  }

  // 释放空间，淘汰不活跃应用直到满足所需空间需求
  private def makeRoom(size: Long): Unit = {
    if (free() < size) {
      logDebug(s"Not enough free space, looking at candidates for deletion...")
      val evicted = new ListBuffer[ApplicationStoreInfo]()
      // 按最后访问时间排序，优先淘汰最早访问的不活跃应用
      Utils.tryWithResource(
        listing.view(classOf[ApplicationStoreInfo]).index("lastAccess").closeableIterator()
      ) { iter =>
        var needed = size
        while (needed > 0 && iter.hasNext()) {
          val info = iter.next()
          val isActive = active.synchronized {
            active.contains(info.appId -> info.attemptId)
          }
          if (!isActive) {
            evicted += info
            needed -= info.size
          }
        }
      }

      if (evicted.nonEmpty) {
        val freed = evicted.map { info =>
          logInfo(log"Deleting store for" +
            log" ${MDC(APP_ID, info.appId)}/${MDC(APP_ATTEMPT_ID, info.attemptId)}.")
          deleteStore(new File(info.path))
          updateUsage(-info.size, committed = true)
          info.size
        }.sum

        logInfo(log"Deleted ${MDC(NUM_BYTES_EVICTED, evicted.size)} store(s)" +
          log" to free ${MDC(NUM_BYTES_TO_FREE, Utils.bytesToString(freed))}" +
          log" (target = ${MDC(NUM_BYTES, Utils.bytesToString(size))}).")
      } else {
        logWarning(log"Unable to free any space to make room for " +
          log"${MDC(NUM_BYTES, Utils.bytesToString(size))}.")
      }
    }
  }

  /**
   * 获取应用存储文件路径
   */
  private[history] def appStorePath(appId: String, attemptId: Option[String]): File = {
    val fileName = appId + attemptId.map("_" + _).getOrElse("") + extension
    new File(appStoreDir, fileName)
  }

  // 更新应用存储元数据信息
  private def updateApplicationStoreInfo(
      appId: String, attemptId: Option[String], newSize: Long): Unit = {
    val path = appStorePath(appId, attemptId)
    val info = ApplicationStoreInfo(path.getAbsolutePath(), clock.getTimeMillis(), appId,
      attemptId, newSize)
    listing.write(info)
  }

  // 更新磁盘使用量统计
  private def updateUsage(delta: Long, committed: Boolean = false): Unit = {
    val updated = currentUsage.addAndGet(delta)
    if (updated < 0) {
      throw new IllegalStateException(
        s"Disk usage tracker went negative (now = $updated, delta = $delta)")
    }
    if (committed) {
      val updatedCommitted = committedUsage.addAndGet(delta)
      if (updatedCommitted < 0) {
        throw new IllegalStateException(
          s"Disk usage tracker went negative (now = $updatedCommitted, delta = $delta)")
      }
    }
  }

  /**
   * 计算目录总大小，暴露用于测试
   */
  private[history] def sizeOf(path: File): Long = Utils.sizeOf(path)

  /**
   * 租赁对象，表示为一个加载中的应用预占的磁盘空间，支持提交和回滚操作
   * 
   * @param tmpPath 临时存储目录
   * @param leased 租赁的字节数
   */
  private[history] class Lease(val tmpPath: File, private val leased: Long) {

    /**
     * 提交租赁，将临时存储移动到最终位置，更新统计信息，标记应用为活跃状态
     * 
     * @param appId 应用ID
     * @param attemptId 尝试ID
     * @return 最终存储目录
     */
    def commit(appId: String, attemptId: Option[String]): File = {
      val dst = appStorePath(appId, attemptId)

      active.synchronized {
        require(!active.contains(appId -> attemptId),
          s"Cannot commit lease for active application $appId / $attemptId")

        if (dst.isDirectory()) {
          val size = sizeOf(dst)
          deleteStore(dst)
          updateUsage(-size, committed = true)
        }
      }

      updateUsage(-leased)

      val newSize = sizeOf(tmpPath)
      makeRoom(newSize)
      tmpPath.renameTo(dst)

      updateUsage(newSize, committed = true)
      if (committedUsage.get() > maxUsage) {
        val current = Utils.bytesToString(committedUsage.get())
        val max = Utils.bytesToString(maxUsage)
        logWarning(log"Commit of application ${MDC(APP_ID, appId)} / " +
          log"${MDC(APP_ATTEMPT_ID, attemptId)} causes maximum disk usage to be " +
          log"exceeded (${MDC(NUM_BYTES, current)} > ${MDC(NUM_BYTES_MAX, max)}")
      }

      updateApplicationStoreInfo(appId, attemptId, newSize)

      active.synchronized {
        active(appId -> attemptId) = newSize
      }
      dst
    }

    /**
     * 回滚租赁，删除临时目录，释放预占的空间
     */
    def rollback(): Unit = {
      updateUsage(-leased)
      Utils.deleteRecursively(tmpPath)
    }

  }

}

/**
 * 应用存储信息实体，存储在元数据中记录每个应用存储的路径、访问时间、大小等信息
 * 
 * @param path 存储目录路径
 * @param lastAccess 最后访问时间戳
 * @param appId 应用ID
 * @param attemptId 尝试ID
 * @param size 存储大小字节数
 */
private case class ApplicationStoreInfo(
    @KVIndexParam path: String,
    @KVIndexParam("lastAccess") lastAccess: Long,
    appId: String,
    attemptId: Option[String],
    size: Long)
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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.History._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.Utils

/**
 * Spark历史服务器（SHS）内存存储使用量追踪管理器
 * 负责管理混合存储模式下，应用事件日志加载到内存的内存占用分配与释放
 */
private class HistoryServerMemoryManager(
    conf: SparkConf) extends Logging {

  // 从配置读取内存存储最大使用量上限
  private val maxUsage = conf.get(MAX_IN_MEMORY_STORE_USAGE)
  // Visible for testing.
  // 当前已使用内存总量，原子变量保证并发安全
  private[history] val currentUsage = new AtomicLong(0L)
  // 所有活跃应用的内存占用记录，key为(应用ID, 尝试ID)，value为预分配内存大小
  private[history] val active = new HashMap[(String, Option[String]), Long]()

  /**
   * 初始化内存管理器，输出初始内存状态日志
   */
  def initialize(): Unit = {
    logInfo(log"Initialized memory manager: " +
      log"current usage = ${MDC(NUM_BYTES_CURRENT, Utils.bytesToString(currentUsage.get()))}, " +
      log"max usage = ${MDC(NUM_BYTES_MAX, Utils.bytesToString(maxUsage))}")
  }

  /**
   * 为指定应用尝试预分配内存，用于加载事件日志到内存存储
   * @param appId 应用ID
   * @param attemptId 应用尝试ID
   * @param eventLogSize 事件日志文件大小
   * @param codec 事件日志使用的压缩算法
   */
  def lease(
      appId: String,
      attemptId: Option[String],
      eventLogSize: Long,
      codec: Option[String]): Unit = {
    // 根据压缩算法估算加载到内存后的占用大小
    val memoryUsage = approximateMemoryUsage(eventLogSize, codec)
    // 检查预分配后是否超过内存上限，超过则抛出异常拒绝加载
    if (memoryUsage + currentUsage.get > maxUsage) {
      throw new RuntimeException("Not enough memory to create hybrid store " +
        s"for app $appId / $attemptId.")
    }
    // 同步更新活跃应用内存占用记录
    active.synchronized {
      active(appId -> attemptId) = memoryUsage
    }
    // 原子累加已使用内存总量
    currentUsage.addAndGet(memoryUsage)
    logInfo(log"Leasing ${MDC(NUM_BYTES, Utils.bytesToString(memoryUsage))} memory usage for " +
      log"app ${MDC(APP_ID, appId)} / ${MDC(APP_ATTEMPT_ID, attemptId)}")
  }

  /**
   * 释放指定应用尝试占用的内存
   * @param appId 应用ID
   * @param attemptId 应用尝试ID
   */
  def release(appId: String, attemptId: Option[String]): Unit = {
    // 同步从活跃记录中移除并获取占用大小
    val memoryUsage = active.synchronized { active.remove(appId -> attemptId) }

    memoryUsage match {
      case Some(m) =>
        // 原子减去已释放的内存大小
        currentUsage.addAndGet(-m)
        logInfo(log"Released ${MDC(NUM_BYTES, Utils.bytesToString(m))} memory usage for " +
          log"app ${MDC(APP_ID, appId)} / ${MDC(APP_ATTEMPT_ID, attemptId)}")
      case None =>
    }
  }

  /**
   * 根据原始事件日志大小和压缩算法，估算加载到内存后的内存占用
   * @param eventLogSize 原始事件日志文件大小
   * @param codec 压缩算法
   * @return 估算的内存占用大小
   */
  private def approximateMemoryUsage(eventLogSize: Long, codec: Option[String]): Long = {
    codec match {
      // ZSTD压缩比高，解压后内存膨胀约10倍
      case Some(CompressionCodec.ZSTD) =>
        eventLogSize * 10
      // 其他压缩算法解压后膨胀约4倍
      case Some(_) =>
        eventLogSize * 4
      // 未压缩，内存占用为原始大小的一半
      case None =>
        eventLogSize / 2
    }
  }
}
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

package org.apache.spark.util.logging

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._

/**
 * 文件滚动策略接口，定义RollingFileAppender如何生成滚动日志文件的核心契约
 */
private[spark] trait RollingPolicy {

  /** 判断当前是否需要触发日志滚动 */
  def shouldRollover(bytesToBeWritten: Long): Boolean

  /** 通知策略已完成日志滚动，更新内部状态 */
  def rolledOver(): Unit

  /** 通知策略当前已写入的字节数，用于更新策略内部状态 */
  def bytesWritten(bytes: Long): Unit

  /** 生成滚动后日志文件的后缀名 */
  def generateRolledOverFileSuffix(): String
}

/**
 * 基于时间间隔的日志滚动策略，每隔固定时间自动触发日志滚动
 */
private[spark] class TimeBasedRollingPolicy(
    var rolloverIntervalMillis: Long,
    rollingFileSuffixPattern: String,
    checkIntervalConstraint: Boolean = true   // set to false while testing
  ) extends RollingPolicy with Logging {

  import TimeBasedRollingPolicy._
  // 检查并修正滚动间隔不满足最小要求的情况
  if (checkIntervalConstraint && rolloverIntervalMillis < MINIMUM_INTERVAL_SECONDS * 1000L) {
    logWarning(log"Rolling interval [${MDC(TIME_UNITS, rolloverIntervalMillis)} " +
      log"ms] is too small. Setting the interval to the acceptable minimum of " +
      log"${MDC(MIN_TIME, MINIMUM_INTERVAL_SECONDS * 1000)} ms.")
    rolloverIntervalMillis = MINIMUM_INTERVAL_SECONDS * 1000L
  }

  // 下一次滚动时间，volatile保证多线程可见性
  @volatile private var nextRolloverTime = calculateNextRolloverTime()
  // 日期格式化器，用于生成滚动文件后缀
  private val formatter = new SimpleDateFormat(rollingFileSuffixPattern, Locale.US)

  /** 当前时间超过下一次滚动时间则触发滚动 */
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    System.currentTimeMillis > nextRolloverTime
  }

  /** 滚动完成后，计算下一次滚动时间 */
  def rolledOver(): Unit = {
    nextRolloverTime = calculateNextRolloverTime()
    logDebug(s"Current time: ${System.currentTimeMillis}, next rollover time: " + nextRolloverTime)
  }

  def bytesWritten(bytes: Long): Unit = { }  // nothing to do

  /** 计算下一次触发滚动的时间点 */
  private def calculateNextRolloverTime(): Long = {
    val now = System.currentTimeMillis()
    val targetTime = (
      math.ceil(now.toDouble / rolloverIntervalMillis) * rolloverIntervalMillis
    ).toLong
    logDebug(s"Next rollover time is $targetTime")
    targetTime
  }

  def generateRolledOverFileSuffix(): String = {
    formatter.format(Calendar.getInstance.getTime)
  }
}

/**
 * 时间滚动策略的全局配置常量
 */
private[spark] object TimeBasedRollingPolicy {
  // 最小滚动间隔：1分钟
  val MINIMUM_INTERVAL_SECONDS = 60L  // 1 minute
}

/**
 * 基于文件大小的日志滚动策略，当日志文件达到指定大小后触发滚动
 */
private[spark] class SizeBasedRollingPolicy(
    var rolloverSizeBytes: Long,
    checkSizeConstraint: Boolean = true     // set to false while testing
  ) extends RollingPolicy with Logging {

  import SizeBasedRollingPolicy._
  // 检查并修正滚动大小不满足最小要求的情况
  if (checkSizeConstraint && rolloverSizeBytes < MINIMUM_SIZE_BYTES) {
    logWarning(log"Rolling size [${MDC(NUM_BYTES, rolloverSizeBytes)} bytes] is too small. " +
      log"Setting the size to the acceptable minimum of ${MDC(MIN_SIZE, MINIMUM_SIZE_BYTES)} " +
      log"bytes.")
    rolloverSizeBytes = MINIMUM_SIZE_BYTES
  }

  // 上次滚动后已写入的字节数，volatile保证多线程可见性
  @volatile private var bytesWrittenSinceRollover = 0L
  // 日期格式化器，用于生成滚动文件后缀
  val formatter = new SimpleDateFormat("--yyyy-MM-dd--HH-mm-ss--SSSS", Locale.US)

  /** 待写入字节加上已写入字节超过大小限制则触发滚动 */
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    logDebug(s"$bytesToBeWritten + $bytesWrittenSinceRollover > $rolloverSizeBytes")
    bytesToBeWritten + bytesWrittenSinceRollover > rolloverSizeBytes
  }

  /** 滚动完成后，重置已写字节计数器 */
  def rolledOver(): Unit = {
    bytesWrittenSinceRollover = 0
  }

  /** 更新当前文件已写入的字节数 */
  def bytesWritten(bytes: Long): Unit = {
    bytesWrittenSinceRollover += bytes
  }

  /** 生成滚动后日志文件的后缀名 */
  def generateRolledOverFileSuffix(): String = {
    formatter.format(Calendar.getInstance.getTime)
  }
}

/**
 * 大小滚动策略的全局配置常量
 */
private[spark] object SizeBasedRollingPolicy {
  // 最小滚动大小：默认缓冲区大小的10倍
  val MINIMUM_SIZE_BYTES = RollingFileAppender.DEFAULT_BUFFER_SIZE * 10
}
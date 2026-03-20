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

package org.apache.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 心跳线程，按指定间隔定期调用心跳上报函数。
 *
 * @param reportHeartbeat 心跳上报回调函数
 * @param name 心跳线程名称
 * @param intervalMs 心跳间隔（毫秒）
 */
private[spark] class Heartbeater(
    reportHeartbeat: () => Unit,
    name: String,
    intervalMs: Long) extends Logging {
  // 单线程守护调度器
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor(name)

  /** 启动心跳定时任务，首次延迟随机抖动以避免多个 Executor 同步心跳 */
  def start(): Unit = {
    val initialDelay = intervalMs + (math.random() * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartbeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }

  /** 手动触发一次心跳上报 */
  def doReportHeartbeat(): Unit = {
    reportHeartbeat()
  }

  /** 停止心跳线程 */
  def stop(): Unit = {
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
  }
}

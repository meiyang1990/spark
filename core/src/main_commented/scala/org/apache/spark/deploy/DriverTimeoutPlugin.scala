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
package org.apache.spark.deploy

import java.util.{Map => JMap}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.DRIVER_TIMEOUT
import org.apache.spark.util.{SparkExitCode, ThreadUtils}

/**
 * 文件级注释：驱动程序超时检测内置插件实现，实现Driver运行超时自动终止功能
 * 属于Spark核心部署模块，提供Driver进程生命周期监控能力
 */

/**
 * 驱动超时插件主入口类，实现Spark插件接口，注册驱动端超时检测插件
 * 内置插件，提供Driver进程运行超时自动终止能力
 */
class DriverTimeoutPlugin extends SparkPlugin {
  /**
   * 返回驱动端插件实例，用于初始化超时检测逻辑
   * @return 驱动超时检测插件实例
   */
  override def driverPlugin(): DriverPlugin = new DriverTimeoutDriverPlugin()

  // No-op
  /**
   * 执行器端不需要插件，返回空实现
   * @return null 表示无执行器端插件
   */
  override def executorPlugin(): ExecutorPlugin = null
}

/**
 * 驱动端超时检测插件类，在Driver端执行超时检测调度，超时后自动终止Driver进程
 * 用于防止Driver进程异常挂起长时间占用集群资源
 */
class DriverTimeoutDriverPlugin extends DriverPlugin with Logging {

  // 调度超时检测任务的单线程定时执行器
  private val timeoutService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-timeout")

  /**
   * 初始化驱动超时检测插件，读取配置并启动超时调度任务
   * @param sc Spark上下文实例
   * @param ctx 插件上下文
   * @return 空配置映射
   */
  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    // 从配置读取超时时间，单位分钟
    val timeout = sc.conf.get(DRIVER_TIMEOUT)
    if (timeout == 0) {
      // 超时配置为0，禁用超时检测功能
      logWarning("Disabled with the timeout value 0.")
    } else {
      // 定义超时触发后的终止任务
      val task: Runnable = () => {
        logWarning(log"Terminate Driver JVM because it runs after " +
          log"${MDC(TIME_UNITS, timeout)} minute" +
          (if (timeout == 1) log"" else log"s"))
        // 不能使用SparkContext.stop，因为Driver可能已经处于异常状态无法正常关闭
        System.exit(SparkExitCode.DRIVER_TIMEOUT)
      }
      // 提交定时任务，超时后执行终止
      timeoutService.schedule(task, timeout, TimeUnit.MINUTES)
    }
    Map.empty[String, String].asJava
  }

  /**
   * 插件关闭时清理定时执行器资源
   */
  override def shutdown(): Unit = timeoutService.shutdown()
}
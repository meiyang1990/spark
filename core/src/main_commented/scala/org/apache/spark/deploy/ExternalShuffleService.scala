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

import java.io.File
import java.util.concurrent.CountDownLatch

import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{AUTH_ENABLED, PORT, SHUFFLE_DB_BACKEND_KEY, SHUFFLE_DB_BACKEND_NAME}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.crypto.AuthServerBootstrap
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.ExternalBlockHandler
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * 外部Shuffle服务，为Executor提供Shuffle文件读取服务，避免Executor退出后Shuffle文件不可访问。
 * 即使原始输出Shuffle数据的Executor被移除，仍能保证下游任务可以继续读取Shuffle数据，提升作业稳定性。
 * 可选支持SASL身份认证，保证服务访问安全。
 * 
 * 核心职责：在每个集群节点上独立运行，统一管理该节点上所有Executor输出的Shuffle文件，供下游任务拉取。
 */
private[deploy]
class ExternalShuffleService(sparkConf: SparkConf, securityManager: SecurityManager)
  extends Logging {
  // 指标系统，用于采集外部Shuffle服务的运行监控指标
  protected val masterMetricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.SHUFFLE_SERVICE, sparkConf)

  private val enabled = sparkConf.get(config.SHUFFLE_SERVICE_ENABLED)
  private val port = sparkConf.get(config.SHUFFLE_SERVICE_PORT)

  // 已注册Executor信息数据库文件名
  private val registeredExecutorsDB = "registeredExecutors"

  // 从Spark配置创建网络传输配置
  private val transportConf =
    SparkTransportConf.fromSparkConf(
      sparkConf,
      "shuffle",
      numUsableCores = 0,
      sslOptions = Some(securityManager.getRpcSSLOptions()))
  // Shuffle块处理器实例
  private val blockHandler = newShuffleBlockHandler(transportConf)
  private var transportContext: TransportContext = _

  private var server: TransportServer = _

  // 外部Shuffle服务指标源
  private val shuffleServiceSource = new ExternalShuffleServiceSource

  /**
   * 在Spark本地目录中查找已注册Executor数据库文件
   * @param dbName 数据库文件名
   * @return 数据库文件对象，如果未配置本地目录则返回null
   */
  protected def findRegisteredExecutorsDBFile(dbName: String): File = {
    val localDirs = sparkConf.getOption("spark.local.dir").map(_.split(",")).getOrElse(Array())
    if (localDirs.length >= 1) {
      new File(localDirs.find(new File(_, dbName).exists()).getOrElse(localDirs(0)), dbName)
    } else {
      logWarning("'spark.local.dir' should be set first when we use db in " +
        "ExternalShuffleService. Note that this only affects standalone mode.")
      null
    }
  }

  /** 获取Shuffle块处理器实例 */
  def getBlockHandler: ExternalBlockHandler = {
    blockHandler
  }

  /**
   * 创建新的Shuffle块处理器，提取为方法方便子类覆盖扩展
   * @param conf 网络传输配置
   * @return 新的Shuffle块处理器实例
   */
  protected def newShuffleBlockHandler(conf: TransportConf): ExternalBlockHandler = {
    if (sparkConf.get(config.SHUFFLE_SERVICE_DB_ENABLED) && enabled) {
      val shuffleDBName = sparkConf.get(config.SHUFFLE_SERVICE_DB_BACKEND)
      logInfo(
        log"Use ${MDC(SHUFFLE_DB_BACKEND_NAME, shuffleDBName.name())} as the implementation of " +
        log"${MDC(SHUFFLE_DB_BACKEND_KEY, config.SHUFFLE_SERVICE_DB_BACKEND.key)}")
      new ExternalBlockHandler(conf,
        findRegisteredExecutorsDBFile(shuffleDBName.fileName(registeredExecutorsDB)))
    } else {
      new ExternalBlockHandler(conf, null)
    }
  }

  /** 如果配置启用了外部Shuffle服务，则启动服务 */
  def startIfEnabled(): Unit = {
    if (enabled) {
      start()
    }
  }

  /** 启动外部Shuffle服务 */
  def start(): Unit = {
    require(server == null, "Shuffle server already started")
    val authEnabled = securityManager.isAuthenticationEnabled()
    logInfo(log"Starting shuffle service on port ${MDC(PORT, port)}" +
      log" (auth enabled = ${MDC(AUTH_ENABLED, authEnabled)})")
    // 准备服务启动引导链，如果开启认证则添加认证引导
    val bootstraps: Seq[TransportServerBootstrap] =
      if (authEnabled) {
        Seq(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    // 创建传输上下文和Netty服务端
    transportContext = new TransportContext(transportConf, blockHandler, true)
    server = transportContext.createServer(port, bootstraps.asJava)

    // 注册各项监控指标到指标系统
    shuffleServiceSource.registerMetricSet(server.getAllMetrics)
    blockHandler.getAllMetrics.getMetrics.put("numRegisteredConnections",
        server.getRegisteredConnections)
    shuffleServiceSource.registerMetricSet(blockHandler.getAllMetrics)
    masterMetricsSystem.registerSource(shuffleServiceSource)
    masterMetricsSystem.start()
  }

  /** 清理已退出应用关联的所有Shuffle文件 */
  def applicationRemoved(appId: String): Unit = {
    blockHandler.applicationRemoved(appId, true /* cleanupLocalDirs */)
  }

  /** 清理已退出Executor关联的所有非Shuffle文件 */
  def executorRemoved(executorId: String, appId: String): Unit = {
    blockHandler.executorRemoved(executorId, appId)
  }

  /** 停止外部Shuffle服务，释放所有资源 */
  def stop(): Unit = {
    if (server != null) {
      server.close()
      server = null
    }
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
  }
}

/**
 * 外部Shuffle服务的入口主类，用于独立进程启动外部Shuffle服务。
 */
object ExternalShuffleService extends Logging {
  @volatile
  private var server: ExternalShuffleService = _

  // 闭锁用于保持进程运行，直到被终止
  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    main(args, (conf: SparkConf, sm: SecurityManager) => new ExternalShuffleService(conf, sm))
  }

  /**
   * 辅助入口方法，支持调用方传入自定义Shuffle服务实现，方便扩展
   * @param args 命令行参数
   * @param newShuffleService 自定义Shuffle服务工厂方法
   */
  private[spark] def main(
      args: Array[String],
      newShuffleService: (SparkConf, SecurityManager) => ExternalShuffleService): Unit = {
    Utils.resetStructuredLogging()
    Utils.initDaemon(log)
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    // 加载配置后重新初始化日志系统，让spark.log.structuredLogging.enabled配置生效
    Utils.resetStructuredLogging(sparkConf)
    Logging.uninitialize()
    val securityManager = new SecurityManager(sparkConf)

    // 命令行启动强制启用Shuffle服务，假设用户明确要启动该服务
    sparkConf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
    server = newShuffleService(sparkConf, securityManager)
    server.start()

    logDebug("Adding shutdown hook") // 提前初始化日志器
    // 添加关闭钩子，进程退出时优雅停止服务
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down shuffle service.")
      server.stop()
      barrier.countDown()
    }

    // 阻塞进程保持运行，直到被终止
    barrier.await()
  }
}
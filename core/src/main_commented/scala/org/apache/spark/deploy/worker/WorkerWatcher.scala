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

package org.apache.spark.deploy.worker

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.LogKeys.WORKER_URL
import org.apache.spark.rpc._

/**
 * 文件: core/src/main/scala/org/apache/spark/deploy/worker/WorkerWatcher.scala
 * 所属模块: Spark core核心模块
 * 核心职责: 监控Worker进程与Executor子进程的连接状态，实现命运共享，当连接断开时退出当前JVM，保证故障及时被发现处理
 *
 * 端点: 负责连接到父Worker进程，当RPC连接断开时终止当前JVM，实现Worker与其子Executor进程的命运共享
 */
private[spark] class WorkerWatcher(
    override val rpcEnv: RpcEnv,
    workerUrl: String,
    isTesting: Boolean = false,
    isChildProcessStopping: AtomicBoolean = new AtomicBoolean(false))
  extends RpcEndpoint with Logging {

  logInfo(log"Connecting to worker ${MDC(WORKER_URL, workerUrl)}")
  // 非测试模式下异步建立到Worker的RPC端点引用
  if (!isTesting) {
    rpcEnv.asyncSetupEndpointRefByURI(workerUrl)
  }

  // 测试环境标记：标记是否已经触发关闭，避免单元测试中直接退出JVM
  // 正常场景下exitNonZero会调用System.exit(-1)退出JVM，单元测试中仅标记isShutDown供测试验证
  private[deploy] var isShutDown = false

  // 从Worker URL解析得到目标RPC地址
  private val expectedAddress = RpcAddress.fromUrlString(workerUrl)
  // 检查传入地址是否为目标Worker的地址
  private def isWorker(address: RpcAddress) = expectedAddress == address

  // 非零退出当前JVM的方法，区分测试和生产场景，避免重复退出导致死锁
  private def exitNonZero() =
    if (isTesting) {
      // 测试模式仅标记关闭状态，不退出JVM
      isShutDown = true
    } else if (isChildProcessStopping.compareAndSet(false, true)) {
      // SPARK-35714: 通过CAS避免重复调用System.exit导致死锁
      // SPARK-14180: 新建线程执行System.exit，避免因executor.stop关闭钩子导致死锁
      new Thread("WorkerWatcher-exit-executor") {
        override def run(): Unit = System.exit(-1)
      }.start()
    }

  override def receive: PartialFunction[Any, Unit] = {
    // 记录收到的未预期消息并警告
    case e => logWarning(log"Received unexpected message: ${MDC(LogKeys.ERROR, e)}")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // 成功连接到目标Worker，打印日志
      logInfo(log"Successfully connected to ${MDC(WORKER_URL, workerUrl)}")
    }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // 与Worker的RPC连接断开，触发当前JVM退出
      logError(log"Lost connection to worker rpc endpoint ${MDC(WORKER_URL, workerUrl)}. Exiting.")
      exitNonZero()
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // 连接Worker过程中发生网络错误，触发当前JVM退出
      logError(
        log"Could not initialize connection to worker ${MDC(WORKER_URL, workerUrl)}. Exiting.",
        cause)
      exitNonZero()
    }
  }
}
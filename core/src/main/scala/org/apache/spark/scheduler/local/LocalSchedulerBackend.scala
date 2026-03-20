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
// 这个文件已经全部加上中文注释

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle}
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.TaskThreadDump
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.util.Utils

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean, reason: String)

private case class StopExecutor()

/**
 * 对[[LocalSchedulerBackend]]的调用都通过LocalEndpoint序列化。
 * 使用RpcEndpoint使得对[[LocalSchedulerBackend]]的调用变为异步，这对于防止
 * [[LocalSchedulerBackend]]与[[TaskSchedulerImpl]]之间的死锁是必要的。
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalSchedulerBackend,
    private val totalCores: Int)
  extends ThreadSafeRpcEndpoint with Logging {

  private var freeCores = totalCores

  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = Utils.localCanonicalHostName()

  // local mode doesn't support extra resources like GPUs right now
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true,
    resources = Map.empty[String, ResourceInformation])

  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread, reason) =>
      executor.killTask(taskId, interruptThread, reason)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
    case TaskThreadDump(taskId) =>
      context.reply(executor.getTaskThreadDump(taskId))
  }

  def reviveOffers(): Unit = {
    // local mode doesn't support extra resources like GPUs right now
    val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores,
      Some(rpcEnv.address.hostPort)))
    for (task <- scheduler.resourceOffers(offers, true).flatten) {
      freeCores -= scheduler.CPUS_PER_TASK
      executor.launchTask(executorBackend, task)
    }
  }
}

/**
 * 用于运行本地版本Spark，其中Executor、后端和Master都运行在同一个JVM中。
 * 它位于[[TaskSchedulerImpl]]之后，负责在本地运行的单个Executor（由[[LocalSchedulerBackend]]创建）上启动任务。
 */
private[spark] class LocalSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {

  private val appId = conf.get("spark.test.appId", "local-" + System.currentTimeMillis)
  private var localEndpoint: RpcEndpointRef = null
  private val userClassPath = getUserClasspath(conf)
  private val listenerBus = scheduler.sc.listenerBus
  private val launcherBackend = new LauncherBackend() {
    override def conf: SparkConf = LocalSchedulerBackend.this.conf
    override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
  }

  /**
   * 返回表示用户类路径的URL列表。
   *
   * @param conf Spark配置
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    val userClassPathStr = conf.get(config.EXECUTOR_CLASS_PATH)
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  launcherBackend.connect()

  override def start(): Unit = {
    val rpcEnv = SparkEnv.get.rpcEnv
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint)
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty,
        Map.empty)))
    launcherBackend.setAppId(appId)
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
    reviveOffers()
  }

  override def stop(): Unit = {
    stop(SparkAppHandle.State.FINISHED)
  }

  override def reviveOffers(): Unit = {
    localEndpoint.send(ReviveOffers)
  }

  override def defaultParallelism(): Int =
    scheduler.conf.getInt(config.DEFAULT_PARALLELISM.key, totalCores)

  override def killTask(
      taskId: Long, executorId: String, interruptThread: Boolean, reason: String): Unit = {
    localEndpoint.send(KillTask(taskId, interruptThread, reason))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

  // Doesn't support different ResourceProfiles yet
  // so we expect all executors to be of same ResourceProfile
  override def maxNumConcurrentTasks(rp: ResourceProfile): Int = {
    val cpusPerTask = ResourceProfile.getTaskCpusOrDefaultForProfile(rp, conf)
    totalCores / cpusPerTask
  }

  private def stop(finalState: SparkAppHandle.State): Unit = {
    localEndpoint.ask(StopExecutor)
    try {
      launcherBackend.setState(finalState)
    } finally {
      launcherBackend.close()
    }
  }

  override def getTaskThreadDump(taskId: Long, executorId: String): Option[ThreadStackTrace] = {
    localEndpoint.askSync[Option[ThreadStackTrace]](TaskThreadDump(taskId))
  }
}

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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.{config, Logging, LogKeys}
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.Utils

/**
 * 本地Spark Standalone集群启动工具，用于测试场景。
 * 该类在同一个JVM进程中启动Master和所有Worker，Executor仍然在独立JVM中运行。
 * 可用于在不启动多个独立进程的情况下测试分布式运行和故障恢复功能。
 */
private[spark]
class LocalSparkCluster private (
    numWorkers: Int,
    coresPerWorker: Int,
    memoryPerWorker: Int,
    conf: SparkConf)
  extends Logging {

  private val localHostname = Utils.localHostName()
  private val masterRpcEnvs = ArrayBuffer[RpcEnv]()
  private val workerRpcEnvs = ArrayBuffer[RpcEnv]()
  // exposed for testing
  var masterWebUIPort = -1
  // for test only
  private val workerDirs = ArrayBuffer[String]()

  /**
   * 启动本地Spark集群，启动Master和所有Worker
   * @return 包含Master地址的数组，供客户端连接使用
   */
  def start(): Array[String] = {
    logInfo(log"Starting a local Spark cluster with " +
      log"${MDC(LogKeys.NUM_WORKERS, numWorkers)} workers.")

    // 除非显式配置，否则该模式下禁用Master的REST服务
    val _conf = conf.clone()
      .setIfMissing(config.MASTER_REST_SERVER_ENABLED, false)
      .set(config.SHUFFLE_SERVICE_ENABLED, false)

    /* 启动Master */
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, _conf)
    masterWebUIPort = webUiPort
    masterRpcEnvs += rpcEnv
    val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
    val masters = Array(masterUrl)

    /* 启动所有Worker */
    for (workerNum <- 1 to numWorkers) {
      // 测试场景下为每个Worker创建临时工作目录
      val workDir = if (Utils.isTesting) {
        Utils.createTempDir(namePrefix = "worker").getAbsolutePath
      } else null
      if (Utils.isTesting) {
        workerDirs += workDir
      }
      // 启动Worker的RPC环境和端点
      val workerEnv = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masters, workDir, Some(workerNum), _conf,
        conf.get(config.Worker.SPARK_WORKER_RESOURCE_FILE))
      workerRpcEnvs += workerEnv
    }

    masters
  }

  /**
   * 获取所有Worker产生的日志文件列表
   * @return Worker日志文件序列
   */
  def workerLogfiles(): Seq[File] = {
    workerDirs.toSeq.flatMap { dir =>
      Utils.recursiveList(new File(dir))
        .filter(f => f.isFile && """.*\.log$""".r.findFirstMatchIn(f.getName).isDefined)
    }
  }

  /**
   * 停止本地Spark集群，按顺序停止Worker和Master并清理资源
   */
  def stop(): Unit = {
    logInfo("Shutting down local Spark cluster.")
    // 先停止Worker，再停止Master，避免Worker报连接断开异常
    workerRpcEnvs.foreach(_.shutdown())
    workerRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.foreach(_.shutdown())
    masterRpcEnvs.foreach(_.awaitTermination())
    masterRpcEnvs.clear()
    workerRpcEnvs.clear()
    workerDirs.clear()
    LocalSparkCluster.clear()
  }
}

/**
 * LocalSparkCluster的单例工厂对象，用于管理全局本地集群实例
 */
private[spark] object LocalSparkCluster {

  private var localCluster: Option[LocalSparkCluster] = None

  private[spark] def get: Option[LocalSparkCluster] = localCluster

  private def clear(): Unit = localCluster = None

  /**
   * 创建并启动一个新的本地Spark集群，实例保存在全局单例中
   * @param numWorkers Worker节点数量
   * @param coresPerWorker 每个Worker分配的CPU核心数
   * @param memoryPerWorker 每个Worker分配的内存大小（单位：MB）
   * @param conf Spark配置对象
   * @return 本地Spark集群实例
   */
  def apply(
      numWorkers: Int,
      coresPerWorker: Int,
      memoryPerWorker: Int,
      conf: SparkConf): LocalSparkCluster = {
    localCluster =
      Some(new LocalSparkCluster(numWorkers, coresPerWorker, memoryPerWorker, conf))
    localCluster.get
  }
}
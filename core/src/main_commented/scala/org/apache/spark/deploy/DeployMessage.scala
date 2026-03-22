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

import scala.collection.immutable.List

import org.apache.spark.deploy.ExecutorState.ExecutorState
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.RecoveryState.MasterState
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.util.Utils

/**
 * Spark部署模式下所有RPC消息的根密封特质，所有部署消息都必须继承该特质
 */
private[deploy] sealed trait DeployMessage extends Serializable

/** 
 * 包含Spark独立部署模式中Master、Worker、AppClient、WebUI等各个组件之间交互的所有消息定义
 * 这些消息用于集群状态同步、资源调度、应用生命周期管理等核心交互流程
 */
private[deploy] object DeployMessages {
  // Worker to Master

  /**
   * Worker节点向Master发起注册的请求消息
   * @param id worker的唯一标识ID
   * @param host worker的主机地址
   * @param port worker的RPC服务端口
   * @param worker worker端点的RPC引用
   * @param cores worker可用CPU核心数
   * @param memory worker可用内存大小
   * @param workerWebUiUrl worker的Web UI地址
   * @param masterAddress worker连接的Master地址
   * @param resources worker的自定义资源信息（如GPU等）
   */
  case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      worker: RpcEndpointRef,
      cores: Int,
      memory: Int,
      workerWebUiUrl: String,
      masterAddress: RpcAddress,
      resources: Map[String, ResourceInformation] = Map.empty)
    extends DeployMessage {
    Utils.checkHost(host)
    assert (port > 0)
  }

  /**
   * Master内部异步处理停用Worker请求的消息，用于MasterWebUI触发批量停用操作后异步处理
   * @param ids 需要停用的Worker ID列表
   */
  case class DecommissionWorkers(ids: Seq[String]) extends DeployMessage

  /**
   * Master发送给Worker的停用消息，由MasterWebUI触发停用操作时发送
   * 停用Worker会导致该Worker上所有执行器也一并被停用
   */
  object DecommissionWorker extends DeployMessage

  /**
   * Worker自身发送的内部消息，用于在接收到停用信号后触发Worker自身的停用流程
   */
  object WorkerDecommissionSigReceived extends DeployMessage

  /**
   * Worker向Master发送的已开始停用的通知消息，用于Worker侧触发停用的场景
   * @param id worker ID
   * @param workerRef worker端点的RPC引用
   */
  case class WorkerDecommissioning(id: String, workerRef: RpcEndpointRef) extends DeployMessage

  /**
   * Worker向Master汇报执行器状态变更的消息
   */
  case class ExecutorStateChanged(
      appId: String,
      execId: Int,
      state: ExecutorState,
      message: Option[String],
      exitStatus: Option[Int])
    extends DeployMessage

  /**
   * Worker向Master汇报Driver状态变更的消息
   */
  case class DriverStateChanged(
      driverId: String,
      state: DriverState,
      exception: Option[Exception])
    extends DeployMessage

  /**
   * Worker向Master同步执行器资源信息的响应消息
   */
  case class WorkerExecutorStateResponse(
      desc: ExecutorDescription,
      resources: Map[String, ResourceInformation])

  /**
   * Worker向Master同步Driver资源信息的响应消息
   */
  case class WorkerDriverStateResponse(
      driverId: String,
      resources: Map[String, ResourceInformation])

  /**
   * Worker向Master同步所有调度状态（执行器和Driver）的响应消息
   */
  case class WorkerSchedulerStateResponse(
      id: String,
      execResponses: List[WorkerExecutorStateResponse],
      driverResponses: Seq[WorkerDriverStateResponse])

  /**
   * Worker注册完成后向Master发送当前Worker上最新的执行器和Driver列表，Master会对比移除未知的僵尸进程
   * @param id worker ID
   * @param executors 当前worker上的所有执行器列表
   * @param driverIds 当前worker上的所有Driver ID列表
   */
  case class WorkerLatestState(
      id: String,
      executors: Seq[ExecutorDescription],
      driverIds: Seq[String]) extends DeployMessage

  /**
   * Worker定时向Master发送的心跳消息，汇报存活状态
   */
  case class Heartbeat(workerId: String, worker: RpcEndpointRef) extends DeployMessage

  /**
   * MasterWebUI向Master发送的批量停用指定主机上所有Worker的请求
   * @param hostnames 需要停用Worker的主机名列表（不带端口）
   */
  case class DecommissionWorkersOnHosts(hostnames: Seq[String])

  // Master to Worker

  /**
   * Worker注册响应的根密封特质
   */
  sealed trait RegisterWorkerResponse

  /**
   * Master回复Worker注册成功的响应消息
   * @param master Master端点的RPC引用
   * @param masterWebUiUrl Master的Web UI地址
   * @param masterAddress Worker连接使用的Master地址，与Worker注册请求中的地址对应
   * @param duplicate 是否为重复的注册请求
   */
  case class RegisteredWorker(
      master: RpcEndpointRef,
      masterWebUiUrl: String,
      masterAddress: RpcAddress,
      duplicate: Boolean) extends DeployMessage with RegisterWorkerResponse

  /**
   * Master回复Worker注册失败的响应消息
   */
  case class RegisterWorkerFailed(message: String) extends DeployMessage with RegisterWorkerResponse

  /**
   * Master处于Standby状态的响应，当HA模式下当前Master不是Active时返回
   */
  case object MasterInStandby extends DeployMessage with RegisterWorkerResponse

  /**
   * Master要求Worker重新连接指定Master的消息
   */
  case class ReconnectWorker(masterUrl: String) extends DeployMessage

  /**
   * Master发送给Worker，要求杀死指定应用的指定执行器的消息
   */
  case class KillExecutor(masterUrl: String, appId: String, execId: Int) extends DeployMessage

  /**
   * Master发送给Worker，要求启动指定执行器的消息
   */
  case class LaunchExecutor(
      masterUrl: String,
      appId: String,
      execId: Int,
      rpId: Int,
      appDesc: ApplicationDescription,
      cores: Int,
      memory: Int,
      resources: Map[String, ResourceInformation] = Map.empty)
    extends DeployMessage

  /**
   * Master发送给Worker，要求启动指定Driver的消息
   */
  case class LaunchDriver(
      driverId: String,
      driverDesc: DriverDescription,
      resources: Map[String, ResourceInformation] = Map.empty) extends DeployMessage

  /**
   * Master发送给Worker，要求杀死指定Driver的消息
   */
  case class KillDriver(driverId: String) extends DeployMessage

  /**
   * Master通知Worker应用已完成的消息
   */
  case class ApplicationFinished(id: String)

  // Worker internal

  /**
   * Worker内部定时清理工作目录的消息
   */
  case object WorkDirCleanup

  /**
   * Worker内部触发重新向Master注册的消息，用于断连重连场景
   */
  case object ReregisterWithMaster

  // AppClient to Master

  /**
   * 应用客户端向Master注册应用的请求消息
   */
  case class RegisterApplication(appDescription: ApplicationDescription, driver: RpcEndpointRef)
    extends DeployMessage

  /**
   * 应用客户端向Master注销应用的请求消息
   */
  case class UnregisterApplication(appId: String)

  /**
   * 应用客户端确认Master切换完成的通知消息，HA场景主备切换后使用
   */
  case class MasterChangeAcknowledged(appId: String)

  /**
   * 应用客户端向Master请求增加执行器的消息，动态资源调整使用
   */
  case class RequestExecutors(appId: String, resourceProfileToTotalExecs: Map[ResourceProfile, Int])

  /**
   * 应用客户端向Master请求杀死指定执行器的消息
   */
  case class KillExecutors(appId: String, executorIds: Seq[String])

  // Master to AppClient

  /**
   * Master回复应用客户端注册成功的响应消息
   */
  case class RegisteredApplication(appId: String, master: RpcEndpointRef) extends DeployMessage

  /**
   * Master通知应用客户端新增执行器的消息
   */
  case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
    Utils.checkHostPort(hostPort)
  }

  /**
   * Master通知应用客户端执行器状态更新的消息
   * @param workerHost 如果Worker丢失或被停用，保存该Worker的主机地址，否则为None
   */
  case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String],
    exitStatus: Option[Int], workerHost: Option[String])

  /**
   * Master通知应用客户端应用已被移除的消息
   */
  case class ApplicationRemoved(message: String)

  /**
   * Master通知应用客户端Worker已被移除的消息
   */
  case class WorkerRemoved(id: String, host: String, message: String)

  // DriverClient <-> Master

  /**
   * Driver客户端向Master提交Driver的请求消息
   */
  case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage

  /**
   * Master回复Driver客户端提交Driver的响应消息
   */
  case class SubmitDriverResponse(
      master: RpcEndpointRef, success: Boolean, driverId: Option[String], message: String)
    extends DeployMessage

  /**
   * Driver客户端向Master请求杀死指定Driver的消息
   */
  case class RequestKillDriver(driverId: String) extends DeployMessage

  /**
   * Master回复Driver客户端杀死Driver的响应消息
   */
  case class KillDriverResponse(
      master: RpcEndpointRef, driverId: String, success: Boolean, message: String)
    extends DeployMessage

  /**
   * 请求Master杀死所有已完成的Driver
   */
  case object RequestKillAllDrivers extends DeployMessage

  /**
   * Master回复杀死所有Driver的响应消息
   */
  case class KillAllDriversResponse(
      master: RpcEndpointRef, success: Boolean, message: String)
    extends DeployMessage

  /**
   * Driver客户端向Master请求查询Driver状态的消息
   */
  case class RequestDriverStatus(driverId: String) extends DeployMessage

  /**
   * Master回复Driver状态查询的响应消息
   */
  case class DriverStatusResponse(found: Boolean, state: Option[DriverState],
    workerId: Option[String], workerHostPort: Option[String], exception: Option[Exception])

  /**
   * 请求Master清理所有已完成的Driver和应用
   */
  case object RequestClearCompletedDriversAndApps extends DeployMessage

  /**
   * 健康检查就绪探针请求消息
   */
  case object RequestReadyz extends DeployMessage

  // Internal message in AppClient

  /**
   * 应用客户端内部停止自身的消息
   */
  case object StopAppClient

  // Master to Worker & AppClient

  /**
   * HA场景主备切换后，新Master通知Worker和AppClientMaster已经切换的消息
   */
  case class MasterChanged(master: RpcEndpointRef, masterWebUiUrl: String)

  // MasterWebUI To Master

  /**
   * MasterWebUI向Master请求获取当前集群状态的消息
   */
  case object RequestMasterState

  // Master to MasterWebUI

  /**
   * Master回复MasterWebUI集群状态的响应消息
   * 包含当前Master状态、所有Worker、应用和Driver的信息
   */
  case class MasterStateResponse(
      host: String,
      port: Int,
      restPort: Option[Int],
      workers: Array[WorkerInfo],
      activeApps: Array[ApplicationInfo],
      completedApps: Array[ApplicationInfo],
      activeDrivers: Array[DriverInfo],
      completedDrivers: Array[DriverInfo],
      status: MasterState) {

    Utils.checkHost(host)
    assert (port > 0)

    def uri: String = "spark://" + host + ":" + port
    def restUri: Option[String] = restPort.map { p => "spark://" + host + ":" + p }
  }

  //  WorkerWebUI to Worker

  /**
   * WorkerWebUI向Worker请求获取当前Worker状态的消息
   */
  case object RequestWorkerState

  // Worker to WorkerWebUI

  /**
   * Worker回复WorkerWebUI当前Worker状态的响应消息
   * 包含Worker资源信息、所有执行器和Driver的运行状态
   */
  case class WorkerStateResponse(host: String, port: Int, workerId: String,
    executors: List[ExecutorRunner], finishedExecutors: List[ExecutorRunner],
    drivers: List[DriverRunner], finishedDrivers: List[DriverRunner], masterUrl: String,
    cores: Int, memory: Int, coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String,
    resources: Map[String, ResourceInformation] = Map.empty,
    resourcesUsed: Map[String, ResourceInformation] = Map.empty) {

    Utils.checkHost(host)
    assert (port > 0)
  }

  // Liveness checks in various places

  /**
   * 内部触发发送心跳的消息，用于各个组件的存活检测
   */
  case object SendHeartbeat

}
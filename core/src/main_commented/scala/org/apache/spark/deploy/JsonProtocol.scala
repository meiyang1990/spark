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

import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, WorkerStateResponse}
import org.apache.spark.deploy.master._
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.resource.{ResourceInformation, ResourceRequirement}

/**
 * Spark部署模块的JSON序列化工具类，用于将集群状态对象转换为JSON格式，
 * 供Web UI和REST API返回集群状态信息使用。
 */
private[deploy] object JsonProtocol {

  /**
   * 将资源信息Map转换为JSON对象
   * @param info 资源信息Map，key为资源名称，value为资源信息对象
   * @return 序列化后的JSON对象
   */
  private def writeResourcesInfo(info: Map[String, ResourceInformation]): JObject = {
    val jsonFields = info.map {
      case (k, v) => JField(k, v.toJson())
    }
    JObject(jsonFields.toList)
  }

  /**
   * 将资源需求对象转换为JSON对象
   * @param req 资源需求对象
   * @return 序列化后的JSON对象
   */
  private def writeResourceRequirement(req: ResourceRequirement): JObject = {
    ("name" -> req.resourceName) ~
    ("amount" -> req.amount)
  }

  /**
   * 将Worker节点信息对象转换为JSON对象
   * @param obj Worker信息对象
   * @return 包含Worker全部信息的JSON对象
   */
  def writeWorkerInfo(obj: WorkerInfo): JObject = {
    ("id" -> obj.id) ~
    ("host" -> obj.host) ~
    ("port" -> obj.port) ~
    ("webuiaddress" -> obj.webUiAddress) ~
    ("cores" -> obj.cores) ~
    ("coresused" -> obj.coresUsed) ~
    ("coresfree" -> obj.coresFree) ~
    ("memory" -> obj.memory) ~
    ("memoryused" -> obj.memoryUsed) ~
    ("memoryfree" -> obj.memoryFree) ~
    ("resources" -> writeResourcesInfo(obj.resourcesInfo)) ~
    ("resourcesused" -> writeResourcesInfo(obj.resourcesInfoUsed)) ~
    ("resourcesfree" -> writeResourcesInfo(obj.resourcesInfoFree)) ~
    ("state" -> obj.state.toString) ~
    ("lastheartbeat" -> obj.lastHeartbeat)
  }

  /**
   * 将应用信息对象转换为JSON对象
   * @param obj 应用信息对象
   * @return 包含应用全部信息的JSON对象
   */
  def writeApplicationInfo(obj: ApplicationInfo): JObject = {
    ("id" -> obj.id) ~
    ("starttime" -> obj.startTime) ~
    ("name" -> obj.desc.name) ~
    ("cores" -> obj.coresGranted) ~
    ("user" -> obj.desc.user) ~
    ("memoryperexecutor" -> obj.desc.memoryPerExecutorMB) ~
    ("memoryperslave" -> obj.desc.memoryPerExecutorMB) ~
    ("resourcesperexecutor" -> obj.desc.resourceReqsPerExecutor
      .toList.map(writeResourceRequirement)) ~
    ("resourcesperslave" -> obj.desc.resourceReqsPerExecutor
      .toList.map(writeResourceRequirement)) ~
    ("submitdate" -> obj.submitDate.toString) ~
    ("state" -> obj.state.toString) ~
    ("duration" -> obj.duration)
  }

  /**
   * 将应用描述对象转换为JSON对象
   * @param obj 应用描述对象
   * @return 包含应用描述全部信息的JSON对象
   */
  def writeApplicationDescription(obj: ApplicationDescription): JObject = {
    ("name" -> obj.name) ~
    ("cores" -> obj.maxCores.getOrElse(0)) ~
    ("memoryperexecutor" -> obj.memoryPerExecutorMB) ~
    ("resourcesperexecutor" -> obj.resourceReqsPerExecutor.toList.map(writeResourceRequirement)) ~
    ("memoryperslave" -> obj.memoryPerExecutorMB) ~
    ("resourcesperslave" -> obj.resourceReqsPerExecutor.toList.map(writeResourceRequirement)) ~
    ("user" -> obj.user) ~
    ("command" -> obj.command.toString)
  }

  /**
   * 将Executor运行对象转换为JSON对象
   * @param obj Executor运行对象
   * @return 包含Executor全部信息的JSON对象
   */
  def writeExecutorRunner(obj: ExecutorRunner): JObject = {
    ("id" -> obj.execId) ~
    ("memory" -> obj.memory) ~
    ("resources" -> writeResourcesInfo(obj.resources)) ~
    ("appid" -> obj.appId) ~
    ("appdesc" -> writeApplicationDescription(obj.appDesc))
  }

  /**
   * 将Driver信息对象转换为JSON对象
   * @param obj Driver信息对象
   * @return 包含Driver全部信息的JSON对象
   */
  def writeDriverInfo(obj: DriverInfo): JObject = {
    ("id" -> obj.id) ~
    ("starttime" -> obj.startTime.toString) ~
    ("state" -> obj.state.toString) ~
    ("cores" -> obj.desc.cores) ~
    ("memory" -> obj.desc.mem) ~
    ("resources" -> writeResourcesInfo(obj.resources)) ~
    ("submitdate" -> obj.submitDate.toString) ~
    ("worker" -> obj.worker.map(_.id).getOrElse("None")) ~
    ("mainclass" -> obj.desc.command.arguments(2))
  }

  /**
   * 将Master状态响应对象转换为JSON对象，支持只返回指定字段
   * @param obj Master状态响应对象
   * @param field 需要返回的指定字段，None表示返回全部字段
   * @return 包含Master状态信息的JSON对象
   */
  def writeMasterState(obj: MasterStateResponse, field: Option[String] = None): JObject = {
    // 过滤出存活的Worker节点
    val aliveWorkers = obj.workers.filter(_.isAlive())
    field match {
      case None =>
        ("url" -> obj.uri) ~
        ("workers" -> obj.workers.toList.map (writeWorkerInfo) ) ~
        ("aliveworkers" -> aliveWorkers.length) ~
        ("cores" -> aliveWorkers.map (_.cores).sum) ~
        ("coresused" -> aliveWorkers.map (_.coresUsed).sum) ~
        ("memory" -> aliveWorkers.map (_.memory).sum) ~
        ("memoryused" -> aliveWorkers.map (_.memoryUsed).sum) ~
        ("resources" -> aliveWorkers.map (_.resourcesInfo).toList.map (writeResourcesInfo) ) ~
        ("resourcesused" ->
          aliveWorkers.map (_.resourcesInfoUsed).toList.map (writeResourcesInfo) ) ~
        ("activeapps" -> obj.activeApps.toList.map (writeApplicationInfo) ) ~
        ("completedapps" -> obj.completedApps.toList.map (writeApplicationInfo) ) ~
        ("activedrivers" -> obj.activeDrivers.toList.map (writeDriverInfo) ) ~
        ("completeddrivers" -> obj.completedDrivers.toList.map (writeDriverInfo) ) ~
        ("status" -> obj.status.toString)
      case Some(field) =>
        field match {
          case "url" =>
            ("url" -> obj.uri)
          case "workers" =>
            ("workers" -> obj.workers.toList.map (writeWorkerInfo) )
          case "aliveworkers" =>
            ("aliveworkers" -> aliveWorkers.length)
          case "cores" =>
            ("cores" -> aliveWorkers.map (_.cores).sum)
          case "coresused" =>
            ("coresused" -> aliveWorkers.map (_.coresUsed).sum)
          case "memory" =>
            ("memory" -> aliveWorkers.map (_.memory).sum)
          case "memoryused" =>
            ("memoryused" -> aliveWorkers.map (_.memoryUsed).sum)
          case "resources" =>
            ("resources" -> aliveWorkers.map (_.resourcesInfo).toList.map (writeResourcesInfo) )
          case "resourcesused" =>
            ("resourcesused" ->
              aliveWorkers.map (_.resourcesInfoUsed).toList.map (writeResourcesInfo) )
          case "activeapps" =>
            ("activeapps" -> obj.activeApps.toList.map (writeApplicationInfo) )
          case "completedapps" =>
            ("completedapps" -> obj.completedApps.toList.map (writeApplicationInfo) )
          case "activedrivers" =>
            ("activedrivers" -> obj.activeDrivers.toList.map (writeDriverInfo) )
          case "completeddrivers" =>
            ("completeddrivers" -> obj.completedDrivers.toList.map (writeDriverInfo) )
          case "status" =>
            ("status" -> obj.status.toString)
          case field => (field -> "")
        }
    }
  }

  /**
   * 将Worker状态响应对象转换为JSON对象
   * @param obj Worker状态响应对象
   * @return 包含Worker状态信息的JSON对象
   */
  def writeWorkerState(obj: WorkerStateResponse): JObject = {
    ("id" -> obj.workerId) ~
    ("masterurl" -> obj.masterUrl) ~
    ("masterwebuiurl" -> obj.masterWebUiUrl) ~
    ("cores" -> obj.cores) ~
    ("coresused" -> obj.coresUsed) ~
    ("memory" -> obj.memory) ~
    ("memoryused" -> obj.memoryUsed) ~
    ("resources" -> writeResourcesInfo(obj.resources)) ~
    ("resourcesused" -> writeResourcesInfo(obj.resourcesUsed)) ~
    ("executors" -> obj.executors.map(writeExecutorRunner)) ~
    ("finishedexecutors" -> obj.finishedExecutors.map(writeExecutorRunner))
  }

  /**
   * 根据Master状态计算集群利用率并转换为JSON对象
   * @param obj Master状态响应对象
   * @return 包含集群资源利用率信息的JSON对象
   */
  def writeClusterUtilization(obj: MasterStateResponse): JObject = {
    val aliveWorkers = obj.workers.filter(_.isAlive())
    val cores = aliveWorkers.map(_.cores).sum
    val coresUsed = aliveWorkers.map(_.coresUsed).sum
    val memory = aliveWorkers.map(_.memory).sum
    val memoryUsed = aliveWorkers.map(_.memoryUsed).sum
    ("waitingDrivers" -> obj.activeDrivers.count(_.state == DriverState.SUBMITTED)) ~
    ("cores" -> cores) ~
    ("coresused" -> coresUsed) ~
    ("coresutilization" -> (if (cores == 0) 100 else 100 * coresUsed / cores)) ~
    ("memory" -> memory) ~
    ("memoryused" -> memoryUsed) ~
    ("memoryutilization" -> (if (memory == 0) 100 else 100 * memoryUsed / memory))
  }
}
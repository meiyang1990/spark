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

package org.apache.spark.deploy.master

import scala.collection.mutable

import org.apache.spark.resource.{ResourceAllocator, ResourceInformation, ResourceRequirement}
import org.apache.spark.resource.ResourceAmountUtils.ONE_ENTIRE_RESOURCE
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * Worker节点单个资源类型的信息，负责该资源的分配与释放管理
 * @param name 资源名称
 * @param addresses 资源可用地址列表，如GPU设备编号
 */
private[spark] case class WorkerResourceInfo(name: String, addresses: Seq[String])
  extends Serializable with ResourceAllocator {

  override protected def resourceName = this.name
  override protected def resourceAddresses = this.addresses

  /**
   * 申请指定数量的资源
   * @param amount 需要申请的资源地址数量
   * @return 分配得到的资源信息
   */
  def acquire(amount: Int): ResourceInformation = {
    // 从可用地址中取出指定数量的资源
    val addresses = availableAddrs.take(amount)
    assert(addresses.length == amount)

    acquire(addresses.map(addr => addr -> ONE_ENTIRE_RESOURCE).toMap)
    new ResourceInformation(resourceName, addresses.toArray)
  }
}

/**
 * Master节点维护的Worker节点信息，保存Worker节点的资源状态、运行中的应用等信息
 * @param id Worker全局唯一ID
 * @param host Worker节点主机地址
 * @param port Worker节点通信端口
 * @param cores Worker总CPU核心数
 * @param memory Worker总内存大小(MB)
 * @param endpoint Worker节点的RPC通信端点引用
 * @param webUiAddress Worker Web UI地址
 * @param resources Worker自定义资源信息映射
 */
private[spark] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val endpoint: RpcEndpointRef,
    val webUiAddress: String,
    val resources: Map[String, WorkerResourceInfo])
  extends Serializable {

  // 校验主机地址格式合法性
  Utils.checkHost(host)
  assert (port > 0)

  @transient var executors: mutable.HashMap[String, ExecutorDesc] = _ // 该Worker上运行的Executor映射 key: executorId
  @transient var drivers: mutable.HashMap[String, DriverInfo] = _ // 该Worker上运行的Driver映射 key: driverId
  @transient var state: WorkerState.Value = _ // Worker当前状态
  @transient var coresUsed: Int = _ // 已使用的CPU核心数
  @transient var memoryUsed: Int = _ // 已使用的内存大小(MB)

  @transient var lastHeartbeat: Long = _ // 最后一次心跳接收时间戳

  init()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed
  def resourcesAmountFree: Map[String, Int] = {
    resources.map { case (rName, rInfo) =>
      rName -> rInfo.availableAddrs.length
    }
  }

  def resourcesInfo: Map[String, ResourceInformation] = {
    resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.addresses.toArray)
    }
  }

  def resourcesInfoFree: Map[String, ResourceInformation] = {
    resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.availableAddrs.toArray)
    }
  }

  def resourcesInfoUsed: Map[String, ResourceInformation] = {
    resources.map { case (rName, rInfo) =>
      rName -> new ResourceInformation(rName, rInfo.assignedAddrs.toArray)
    }
  }

  /**
   * Java反序列化后重新初始化 transient 变量
   */
  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  /**
   * 初始化Worker信息的运行时状态
   */
  private def init(): Unit = {
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  /**
   * 添加一个Executor到该Worker，更新已使用资源统计
   * @param exec 要添加的Executor描述信息
   */
  def addExecutor(exec: ExecutorDesc): Unit = {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  /**
   * 从该Worker移除一个Executor，释放对应资源
   * @param exec 要移除的Executor描述信息
   */
  def removeExecutor(exec: ExecutorDesc): Unit = {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
      releaseResources(exec.resources)
    }
  }

  /**
   * 检查该Worker是否运行了指定应用的Executor
   * @param app 应用信息
   * @return 是否存在该应用的Executor
   */
  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  /**
   * 添加一个Driver到该Worker，更新已使用资源统计
   * @param driver 要添加的Driver信息
   */
  def addDriver(driver: DriverInfo): Unit = {
    drivers(driver.id) = driver
    memoryUsed += driver.desc.mem
    coresUsed += driver.desc.cores
  }

  /**
   * 从该Worker移除一个Driver，释放对应资源
   * @param driver 要移除的Driver信息
   */
  def removeDriver(driver: DriverInfo): Unit = {
    drivers -= driver.id
    memoryUsed -= driver.desc.mem
    coresUsed -= driver.desc.cores
    releaseResources(driver.resources)
  }

  /**
   * 更新Worker状态
   * @param state 新的状态
   */
  def setState(state: WorkerState.Value): Unit = {
    this.state = state
  }

  def isAlive(): Boolean = this.state == WorkerState.ALIVE

  /**
   * 为Driver或Executor申请指定数量的自定义资源
   * @param resourceReqs 资源需求列表
   * @return 分配得到的资源信息映射
   */
  def acquireResources(resourceReqs: Seq[ResourceRequirement])
    : Map[String, ResourceInformation] = {
    resourceReqs.map { req =>
      val rName = req.resourceName
      val amount = req.amount
      rName -> resources(rName).acquire(amount)
    }.toMap
  }

  /**
   * Master恢复时，重新分配已被占用的资源
   * @param expected 预期已经被分配的资源信息
   */
  def recoverResources(expected: Map[String, ResourceInformation]): Unit = {
    expected.foreach { case (rName, rInfo) =>
      resources(rName).acquire(rInfo.addresses.map(addr => addr -> ONE_ENTIRE_RESOURCE).toMap)
    }
  }

  /**
   * 释放Driver或Executor占用的自定义资源
   * @param allocated 之前分配给Driver/Executor的资源
   */
  private def releaseResources(allocated: Map[String, ResourceInformation]): Unit = {
    allocated.foreach { case (rName, rInfo) =>
      resources(rName).release(rInfo.addresses.map(addrs => addrs -> ONE_ENTIRE_RESOURCE).toMap)
    }
  }
}
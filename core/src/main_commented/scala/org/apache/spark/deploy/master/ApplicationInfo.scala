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

import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.resource.{ResourceInformation, ResourceProfile, ResourceUtils}
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * Spark Standalone模式中Master侧的应用信息实体，维护一个Spark应用的所有核心状态信息
 * 包括资源分配情况、执行器列表、应用状态、资源配置等，支撑Master对应用的调度管理
 */
private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: RpcEndpointRef,
    defaultCores: Int)
  extends Serializable {

  @transient var state: ApplicationState.Value = _
  @transient var executors: mutable.HashMap[Int, ExecutorDesc] = _
  @transient var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  @transient var coresGranted: Int = _
  @transient var endTime: Long = _
  @transient var appSource: ApplicationSource = _

  @transient private var executorsPerResourceProfileId: mutable.HashMap[Int, mutable.Set[Int]] = _
  @transient private var targetNumExecutorsPerResourceProfileId: mutable.HashMap[Int, Int] = _
  @transient private var rpIdToResourceProfile: mutable.HashMap[Int, ResourceProfile] = _
  @transient private var rpIdToResourceDesc: mutable.HashMap[Int, ExecutorResourceDescription] = _

  @transient private var nextExecutorId: Int = _

  init()

  /**
   * Java反序列化回调，反序列化后重新初始化 transient 成员
   */
  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  /**
   * 初始化应用所有状态信息，包括状态、执行器容器、资源配置映射等
   */
  private def init(): Unit = {
    // 初始状态设为等待调度
    state = ApplicationState.WAITING
    executors = new mutable.HashMap[Int, ExecutorDesc]
    coresGranted = 0
    endTime = -1L
    // 创建应用监控指标来源
    appSource = new ApplicationSource(this)
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
    // 获取初始执行器数量限制，默认无限制
    val initialExecutorLimit = desc.initialExecutorLimit.getOrElse(Integer.MAX_VALUE)

    rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]()
    // 添加默认资源配置
    rpIdToResourceProfile(DEFAULT_RESOURCE_PROFILE_ID) = desc.defaultProfile
    rpIdToResourceDesc = new mutable.HashMap[Int, ExecutorResourceDescription]()
    // 为默认资源配置创建资源描述
    createResourceDescForResourceProfile(desc.defaultProfile)

    targetNumExecutorsPerResourceProfileId = new mutable.HashMap[Int, Int]()
    // 设置默认资源配置的目标执行器数量
    targetNumExecutorsPerResourceProfileId(DEFAULT_RESOURCE_PROFILE_ID) = initialExecutorLimit

    executorsPerResourceProfileId = new mutable.HashMap[Int, mutable.Set[Int]]()
  }

  /**
   * 获取指定资源profile对应的执行器ID集合，不存在则创建空集合
   * @param rpId 资源profile ID
   * @return 执行器ID集合
   */
  private[deploy] def getOrUpdateExecutorsForRPId(rpId: Int): mutable.Set[Int] = {
    executorsPerResourceProfileId.getOrElseUpdate(rpId, mutable.HashSet[Int]())
  }

  /**
   * 获取指定资源profile的目标执行器数量
   * @param rpId 资源profile ID
   * @return 目标执行器数量，不存在则返回0
   */
  private[deploy] def getTargetExecutorNumForRPId(rpId: Int): Int = {
    targetNumExecutorsPerResourceProfileId.getOrElse(rpId, 0)
  }

  /**
   * 获取应用所有请求过的资源profile ID，按升序排序
   * @return 排序后的资源profile ID序列
   */
  private[deploy] def getRequestedRPIds(): Seq[Int] = {
    rpIdToResourceProfile.keys.toSeq.sorted
  }

  /**
   * 为指定资源profile创建调度用的资源描述信息，已存在则跳过
   * @param resourceProfile 资源profile对象
   */
  private def createResourceDescForResourceProfile(resourceProfile: ResourceProfile): Unit = {
    if (!rpIdToResourceDesc.contains(resourceProfile.id)) {
      // 从应用描述中获取默认资源配置
      val defaultMemoryMbPerExecutor = desc.memoryPerExecutorMB
      val defaultCoresPerExecutor = desc.coresPerExecutor
      // 优先使用资源profile中定义的配置，不存在则回退到默认配置
      val coresPerExecutor = resourceProfile.getExecutorCores
        .orElse(defaultCoresPerExecutor)
      val memoryMbPerExecutor = resourceProfile.getExecutorMemory
        .map(_.toInt)
        .getOrElse(defaultMemoryMbPerExecutor)
      // 转换自定义资源请求为调度需求格式
      val customResources = ResourceUtils.executorResourceRequestToRequirement(
        resourceProfile.getCustomExecutorResources().values.toSeq.sortBy(_.resourceName))

      // 保存资源描述
      rpIdToResourceDesc(resourceProfile.id) =
        ExecutorResourceDescription(coresPerExecutor, memoryMbPerExecutor, customResources)
    }
  }

  /**
   * 获取指定资源profile的调度所需资源描述
   * @param rpId 资源profile ID
   * @return 调度用资源描述
   */
  private[deploy] def getResourceDescriptionForRpId(rpId: Int): ExecutorResourceDescription = {
    rpIdToResourceDesc(rpId)
  }

  /**
   * 请求调整各资源profile的目标执行器数量，更新对应资源配置信息
   * @param resourceProfileToTotalExecs 资源profile到目标总执行器数量的映射
   */
  private[deploy] def requestExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {
    resourceProfileToTotalExecs.foreach { case (rp, num) =>
      // 创建资源描述，确保已注册
      createResourceDescForResourceProfile(rp)

      if (!rpIdToResourceProfile.contains(rp.id)) {
        rpIdToResourceProfile(rp.id) = rp
      }

      // 更新目标执行器数量
      targetNumExecutorsPerResourceProfileId(rp.id) = num
    }
  }

  /**
   * 根据资源profile ID获取对应的资源profile对象
   * @param rpId 资源profile ID
   * @return 资源profile对象
   */
  private[deploy] def getResourceProfileById(rpId: Int): ResourceProfile = {
    rpIdToResourceProfile(rpId)
  }

  /**
   * 生成新的执行器ID，支持指定ID场景
   * @param useID 可选的指定ID，如果提供则更新下一个ID生成起点
   * @return 新执行器ID
   */
  private def newExecutorId(useID: Option[Int] = None): Int = {
    useID match {
      case Some(id) =>
        // 更新下一个ID为当前最大ID+1，保证ID不重复
        nextExecutorId = math.max(nextExecutorId, id + 1)
        id
      case None =>
        val id = nextExecutorId
        nextExecutorId += 1
        id
    }
  }

  /**
   * 向应用添加一个新分配的执行器，更新资源统计
   * @param worker 执行器所在Worker节点信息
   * @param cores 分配给执行器的核数
   * @param memoryMb 分配给执行器的内存大小(MB)
   * @param resources 分配给执行器的自定义资源
   * @param rpId 执行器所属资源profile ID
   * @param useID 可选的指定执行器ID
   * @return 新创建的执行器描述对象
   */
  private[master] def addExecutor(
      worker: WorkerInfo,
      cores: Int,
      memoryMb: Int,
      resources: Map[String, ResourceInformation],
      rpId: Int,
      useID: Option[Int] = None): ExecutorDesc = {
    val exec = new ExecutorDesc(
      newExecutorId(useID), this, worker, cores, memoryMb, resources, rpId)
    // 添加到执行器总表
    executors(exec.id) = exec
    // 添加到对应资源profile的执行器集合
    getOrUpdateExecutorsForRPId(rpId).add(exec.id)
    // 更新已分配核数统计
    coresGranted += cores
    exec
  }

  /**
   * 从应用移除一个已释放的执行器，更新资源统计
   * @param exec 要移除的执行器描述对象
   */
  private[master] def removeExecutor(exec: ExecutorDesc): Unit = {
    if (executors.contains(exec.id)) {
      // 保存到已移除执行器列表
      removedExecutors += executors(exec.id)
      // 从总表删除
      executors -= exec.id
      // 从对应资源profile集合删除
      executorsPerResourceProfileId(exec.rpId) -= exec.id
      // 扣减已分配核数
      coresGranted -= exec.cores
    }
  }

  // 应用请求的总核数，未指定则使用默认值
  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  /**
   * 计算应用剩余可分配的核数
   * @return 剩余核数
   */
  private[master] def coresLeft: Int = requestedCores - coresGranted

  private var _retryCount = 0

  /**
   * 获取应用当前重试次数
   * @return 重试次数
   */
  private[master] def retryCount = _retryCount

  /**
   * 应用重试次数自增
   * @return 自增后的重试次数
   */
  private[master] def incrementRetryCount() = {
    _retryCount += 1
    _retryCount
  }

  /**
   * 重置应用重试次数为0
   */
  private[master] def resetRetryCount() = _retryCount = 0

  /**
   * 标记应用为已结束状态，记录结束时间
   * @param endState 结束状态（成功/失败/杀死等）
   */
  private[master] def markFinished(endState: ApplicationState.Value): Unit = {
    state = endState
    endTime = System.currentTimeMillis()
  }

  /**
   * 判断应用是否已经结束
   * @return true表示已结束，false表示运行/等待中
   */
  private[master] def isFinished: Boolean = {
    state != ApplicationState.WAITING && state != ApplicationState.RUNNING
  }

  /**
   * Return the total limit on the number of executors for all resource profiles.
   */
  private[deploy] def getExecutorLimit: Int = {
    targetNumExecutorsPerResourceProfileId.values.sum
  }

  /**
   * 计算应用运行总时长
   * @return 运行时长（毫秒）
   */
  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
}
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
import java.nio.file.Files

import scala.collection.mutable
import scala.util.control.NonFatal

import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.COMPONENT
import org.apache.spark.resource.{ResourceAllocation, ResourceID, ResourceInformation, ResourceRequirement}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Standalone 部署模式资源工具类，提供资源信息处理、持久化和格式化显示功能
 * 为 Standalone 集群的资源分配和管理提供通用工具能力
 */
private[spark] object StandaloneResourceUtils extends Logging {

  /**
   * 可变资源信息容器，支持高效修改地址集合，用于资源分配过程中的动态更新
   * @param name 资源名称
   * @param addresses 资源地址集合，代表可分配的资源实例地址（如GPU序号）
   */
  private[spark] case class MutableResourceInfo(name: String, addresses: mutable.HashSet[String]) {

    /**
     * 合并另一个可变资源信息到当前对象，修改当前对象的地址集合
     * @param other 待合并的可变资源信息
     * @return 当前对象本身
     */
    def + (other: MutableResourceInfo): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.add)
      this
    }

    /**
     * 添加一个不可变资源信息的地址到当前对象
     * @param other 待添加的不可变资源信息
     * @return 当前对象本身
     */
    def + (other: ResourceInformation): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.add)
      this
    }

    /**
     * 从当前对象移除指定不可变资源信息中的地址
     * @param other 待移除的资源信息
     * @return 当前对象本身
     */
    def - (other: ResourceInformation): this.type = {
      assert(name == other.name, s"Inconsistent resource name, expected $name, " +
        s"but got ${other.name}")
      other.addresses.foreach(this.addresses.remove)
      this
    }

    /**
     * 转换为不可变的ResourceInformation对象，供外部使用
     * @return 不可变资源信息对象
     */
    def toResourceInformation: ResourceInformation = {
      new ResourceInformation(name, addresses.toArray)
    }
  }

  /**
   * Standalone 模式专属资源分配记录，跟踪Worker/ Driver（仅客户端模式）进程分配的资源
   * @param pid 进程ID
   * @param allocations 各资源的分配信息列表
   */
  case class StandaloneResourceAllocation(pid: Int, allocations: Seq[ResourceAllocation]) {
    /**
     * 将分配信息转换为按资源名称组织的ResourceInformation映射表
     * @return 资源名称到资源信息的映射
     */
    def toResourceInformationMap: Map[String, ResourceInformation] = {
      allocations.map { allocation =>
        allocation.id.resourceName -> allocation.toResourceInformation
      }.toMap
    }
  }

  /**
   * 将Driver（仅集群模式）或Executor分配到的资源保存为JSON格式文件，供Standalone模式使用
   * @param componentName 组件名称，为 spark.driver 或 spark.executor
   * @param resources 分配给组件的资源信息映射
   * @param dir 存放资源文件的目标目录
   * @return 如果无资源返回None，否则返回生成的资源文件对象
   */
  def prepareResourcesFile(
      componentName: String,
      resources: Map[String, ResourceInformation],
      dir: File): Option[File] = {
    if (resources.isEmpty) {
      return None
    }

    // 截取组件短名称，去掉前面的包路径前缀
    val compShortName = componentName.substring(componentName.lastIndexOf(".") + 1)
    // 创建临时文件用于写入，保证原子性
    val tmpFile = Utils.tempFileWith(dir)
    // 构建ResourceAllocation列表
    val allocations = resources.map { case (rName, rInfo) =>
      ResourceAllocation(new ResourceID(componentName, rName), rInfo.addresses.toImmutableArraySeq)
    }.toSeq
    try {
      // 写入JSON到临时文件
      writeResourceAllocationJson(allocations, tmpFile)
    } catch {
      case NonFatal(e) =>
        val errMsg =
          log"Exception threw while preparing resource file for ${MDC(COMPONENT, compShortName)}"
        logError(errMsg, e)
        throw new SparkException(errMsg.message, e)
    }
    // 原子重命名临时文件为最终文件名
    val resourcesFile = File.createTempFile(s"resource-$compShortName-", ".json", dir)
    tmpFile.renameTo(resourcesFile)
    Some(resourcesFile)
  }

  /**
   * 将资源分配列表序列化为JSON并写入文件
   * @param allocations 资源分配列表
   * @param jsonFile 目标写入文件
   */
  private def writeResourceAllocationJson[T](
      allocations: Seq[T],
      jsonFile: File): Unit = {
    implicit val formats: Formats = DefaultFormats
    val allocationJson = Extraction.decompose(allocations)
    Files.write(jsonFile.toPath, compact(render(allocationJson)).getBytes())
  }

  /**
   * 将不可变资源信息映射转换为可变资源信息映射，用于动态修改
   * @param immutableResources 不可变资源信息映射
   * @return 可变资源信息映射
   */
  def toMutable(immutableResources: Map[String, ResourceInformation])
    : Map[String, MutableResourceInfo] = {
    immutableResources.map { case (rName, rInfo) =>
      val mutableAddress = new mutable.HashSet[String]()
      mutableAddress ++= rInfo.addresses
      rName -> MutableResourceInfo(rInfo.name, mutableAddress)
    }
  }

  /**
   * 格式化资源已用和空闲详情，供Web UI显示
   * @param usedInfo 已用资源信息映射
   * @param freeInfo 空闲资源信息映射
   * @return 格式化后的字符串
   */
  def formatResourcesDetails(
      usedInfo: Map[String, ResourceInformation],
      freeInfo: Map[String, ResourceInformation]): String = {
    usedInfo.map { case (rName, rInfo) =>
      val used = rInfo.addresses.mkString("[", ", ", "]")
      val free = freeInfo(rName).addresses.mkString("[", ", ", "]")
      s"$rName: Free: $free / Used: $used"
    }.mkString(", ")
  }

  /**
   * 格式化资源地址列表，供Web UI显示
   * @param resources 资源信息映射
   * @return 格式化后的字符串
   */
  def formatResourcesAddresses(resources: Map[String, ResourceInformation]): String = {
    resources.map { case (rName, rInfo) =>
      s"$rName: ${rInfo.addresses.mkString("[", ", ", "]")}"
    }.mkString(", ")
  }

  /**
   * 格式化资源已用量和总量，供Web UI显示
   * @param resourcesTotal 资源总量映射，key为资源名，value为总量
   * @param resourcesUsed 已用资源量映射，key为资源名，value为已用量
   * @return 格式化后的字符串
   */
  def formatResourcesUsed(
      resourcesTotal: Map[String, Int],
      resourcesUsed: Map[String, Int]): String = {
    resourcesTotal.map { case (rName, totalSize) =>
      val used = resourcesUsed(rName)
      val total = totalSize
      s"$used / $total $rName"
    }.mkString(", ")
  }

  /**
   * 格式化资源需求列表，供Web UI显示
   * @param requirements 资源需求列表
   * @return 格式化后的字符串
   */
  def formatResourceRequirements(requirements: Seq[ResourceRequirement]): String = {
    requirements.map(req => s"${req.amount} ${req.resourceName}").mkString(", ")
  }
}
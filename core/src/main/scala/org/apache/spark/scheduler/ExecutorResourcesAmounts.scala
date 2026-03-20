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

package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

import org.apache.spark.SparkException
import org.apache.spark.resource.{ResourceAmountUtils, ResourceProfile}
import org.apache.spark.resource.ResourceAmountUtils.ONE_ENTIRE_RESOURCE

/**
 * 保存Executor上一系列资源信息的类。资源可以是GPU、FPGA等。
 * 在 [[TaskSchedulerImpl]] 向任务提供资源时，用作计算资源分配量的临时类。
 *
 * 以GPU为例，addresses为GPU的设备索引。
 *
 * @param resources Executor可用的资源及数量。例如：
 *                  Map("gpu" -> Map("0" -> 内部表示的0.2, "1" -> 内部表示的1.0),
 *                      "fpga" -> Map("a" -> 内部表示的0.3, "b" -> 内部表示的0.9))
 */
private[spark] class ExecutorResourcesAmounts(
    private val resources: Map[String, Map[String, Long]]) extends Serializable {

  /**
   * 将资源转换为可变HashMap，以便动态分配和释放。
   */
  private val internalResources: Map[String, HashMap[String, Long]] = {
    resources.map { case (rName, addressAmounts) =>
      rName -> HashMap(addressAmounts.toSeq: _*)
    }
  }

  /**
   * 每种资源的地址总数。例如，如果gpu有3个地址、fpga有2个地址，
   * 则返回 Map("gpu" -> 3, "fpga" -> 2)。
   */
  lazy val resourceAddressAmount: Map[String, Int] = internalResources.map {
    case (rName, addressMap) => rName -> addressMap.size
  }

  /**
   * 测试用途。将内部资源表示转换回分数形式的资源量。
   */
  private[spark] def availableResources: Map[String, Map[String, Double]] = {
    internalResources.map { case (rName, addressMap) =>
      rName -> addressMap.map { case (address, amount) =>
        address -> ResourceAmountUtils.toFractionalResource(amount)
      }.toMap
    }
  }

  /**
   * 获取（占用）资源。从可用资源池中扣减指定量。
   * @param assignedResource 要分配的资源信息
   */
  def acquire(assignedResource: Map[String, Map[String, Long]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = internalResources.getOrElse(rName,
        throw new SparkException(s"Try to acquire an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prevInternalTotalAmount = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to acquire an address that doesn't exist. $rName " +
            s"address $address doesn't exist."))

        val left = prevInternalTotalAmount - amount
        if (left < 0) {
          throw new SparkException(s"The total amount " +
            s"${ResourceAmountUtils.toFractionalResource(left)} " +
            s"after acquiring $rName address $address should be >= 0")
        }
        internalResources(rName)(address) = left
      }
    }
  }

  /**
   * 将已分配的资源释放回资源池。
   * @param assignedResource 要释放的资源
   */
  def release(assignedResource: Map[String, Map[String, Long]]): Unit = {
    assignedResource.foreach { case (rName, taskResAmounts) =>
      val availableResourceAmounts = internalResources.getOrElse(rName,
        throw new SparkException(s"Try to release an address from $rName that doesn't exist"))
      taskResAmounts.foreach { case (address, amount) =>
        val prevInternalTotalAmount = availableResourceAmounts.getOrElse(address,
          throw new SparkException(s"Try to release an address that is not assigned. $rName " +
            s"address $address is not assigned."))
        val total = prevInternalTotalAmount + amount
        if (total > ONE_ENTIRE_RESOURCE) {
          throw new SparkException(s"The total amount " +
            s"${ResourceAmountUtils.toFractionalResource(total)} " +
            s"after releasing $rName address $address should be <= 1.0")
        }
        internalResources(rName)(address) = total
      }
    }
  }

  /**
   * 根据任务的资源需求尝试分配地址。此函数始终从"最小"地址开始遍历可用资源，
   * 如果某个地址的可用资源量满足任务需求，则将该地址分配给任务。
   *
   * 例如：可用资源为 {"gpu" -> {"0"-> 0.7, "1" -> 1.0}}，任务需求为0.5，
   * 返回 Some(Map("gpu" -> {"0" -> 0.5}))。
   *
   * TODO: 由于总是从最小地址开始分配，可能导致资源碎片化浪费。
   *
   * @param taskSetProf 基于哪个ResourceProfile进行资源分配
   * @return 分配的资源量（Optional）。如果任何资源需求无法满足，返回None。
   */
  def assignAddressesCustomResources(taskSetProf: ResourceProfile):
      Option[Map[String, Map[String, Long]]] = {
    // only look at the resource other than cpus
    val tsResources = taskSetProf.getCustomTaskResources()
    if (tsResources.isEmpty) {
      return Some(Map.empty)
    }

    val allocatedAddresses = HashMap[String, Map[String, Long]]()

    // Go through all resources here so that we can make sure they match and also get what the
    // assignments are for the next task
    for ((rName, taskReqs) <- tsResources) {
      // TaskResourceRequest checks the task amount should be in (0, 1] or a whole number
      var taskAmount = taskReqs.amount

      internalResources.get(rName) match {
        case Some(addressesAmountMap) =>
          val allocatedAddressesMap = HashMap[String, Long]()

          // Always sort the addresses
          val addresses = addressesAmountMap.keys.toSeq.sorted

          // task.amount is a whole number
          if (taskAmount >= 1.0) {
            for (address <- addresses if taskAmount > 0) {
              // The address is still a whole resource
              if (ResourceAmountUtils.isOneEntireResource(addressesAmountMap(address))) {
                taskAmount -= 1.0
                // Assign the full resource of the address
                allocatedAddressesMap(address) = ONE_ENTIRE_RESOURCE
              }
            }
          } else if (taskAmount > 0.0) { // 0 < task.amount < 1.0
            val internalTaskAmount = ResourceAmountUtils.toInternalResource(taskAmount)
            for (address <- addresses if taskAmount > 0) {
              if (addressesAmountMap(address) >= internalTaskAmount) {
                // Assign the part of the address.
                allocatedAddressesMap(address) = internalTaskAmount
                taskAmount = 0
              }
            }
          }

          if (taskAmount == 0 && allocatedAddressesMap.size > 0) {
            allocatedAddresses.put(rName, allocatedAddressesMap.toMap)
          } else {
            return None
          }

        case None => return None
      }
    }
    Some(allocatedAddresses.toMap)
  }

}

private[spark] object ExecutorResourcesAmounts {

  /**
   * 创建一个空的ExecutorResourcesAmounts实例
   */
  def empty: ExecutorResourcesAmounts = new ExecutorResourcesAmounts(Map.empty)

  /**
   * 将Executor资源信息映射转换为ExecutorResourcesAmounts实例
   */
  def apply(executorInfos: Map[String, ExecutorResourceInfo]): ExecutorResourcesAmounts = {
    new ExecutorResourcesAmounts(
      executorInfos.map { case (rName, rInfo) => rName -> rInfo.resourcesAmounts }
    )
  }

}

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

package org.apache.spark.resource

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.resource.ResourceAmountUtils.ONE_ENTIRE_RESOURCE

/**
 * ResourceAmountUtils - 资源数量工具类
 * 
 * 解决浮点数精度问题：
 * 使用 double 进行资源计算可能遇到精度损失。例如将 1.0 分配给 9 个任务（每个 1.0/9），
 * 由于浮点数精度问题，最后一个任务无法分配（剩余 0.111... 小于 0.111...）。
 * 
 * 解决方案：
 * 将 double 乘以 ONE_ENTIRE_RESOURCE (10^16) 转换为 long，避免精度损失。
 * Double 最多显示 16 位小数，因此使用 10^16 作为缩放因子。
 */
private[spark] object ResourceAmountUtils {
  /**
   * 表示一个完整资源的内部单位（10^16）
   * 用于将 double 转换为 long 以避免浮点数精度问题
   */
  final val ONE_ENTIRE_RESOURCE: Long = 10000000000000000L

  def isOneEntireResource(amount: Long): Boolean = amount == ONE_ENTIRE_RESOURCE

  def toInternalResource(amount: Double): Long = (amount * ONE_ENTIRE_RESOURCE).toLong

  def toFractionalResource(amount: Long): Double = amount.toDouble / ONE_ENTIRE_RESOURCE

}

/**
 * ResourceAllocator - 资源分配器 Trait
 * 
 * 帮助 Executor/Worker 分配和管理资源（如 GPU、FPGA）。
 * 
 * 核心功能：
 * 1. 跟踪每个资源地址的可用性（默认 1.0，支持分数资源）
 * 2. acquire()：为任务获取资源地址
 * 3. release()：任务完成后释放资源地址
 * 
 * 设计要点：
 * - 使用内部单位（乘以 ONE_ENTIRE_RESOURCE）避免浮点数精度问题
 * - 地址可用性 > 0 表示可用，= 0 表示已完全分配
 * - 支持分数资源（如 0.5 表示 2 个任务共享 1 个地址）
 * 
 * @note 此 trait 仅用于单线程环境
 */
private[spark] trait ResourceAllocator {

  protected def resourceName: String
  protected def resourceAddresses: Seq[String]

  /**
   * Map from an address to its availability default to 1.0 (we multiply ONE_ENTIRE_RESOURCE
   * to avoid precision error), a value &gt; 0 means the address is available, while value of
   * 0 means the address is fully assigned.
   */
  private lazy val addressAvailabilityMap = {
    mutable.HashMap(resourceAddresses.map(address => address -> ONE_ENTIRE_RESOURCE): _*)
  }

  /**
   * Get the amounts of resources that have been multiplied by ONE_ENTIRE_RESOURCE.
   * @return the resources amounts
   */
  def resourcesAmounts: Map[String, Long] = addressAvailabilityMap.toMap

  /**
   * Sequence of currently available resource addresses which are not fully assigned.
   */
  def availableAddrs: Seq[String] = addressAvailabilityMap
    .filter(addresses => addresses._2 > 0).keys.toSeq.sorted

  /**
   * Sequence of currently assigned resource addresses.
   */
  private[spark] def assignedAddrs: Seq[String] = addressAvailabilityMap
    .filter(addresses => addresses._2 < ONE_ENTIRE_RESOURCE).keys.toSeq.sorted

  /**
   * Acquire a sequence of resource addresses (to a launched task), these addresses must be
   * available. When the task finishes, it will return the acquired resource addresses.
   * Throw an Exception if an address is not available or doesn't exist.
   */
  def acquire(addressesAmounts: Map[String, Long]): Unit = {
    addressesAmounts.foreach { case (address, amount) =>
      val prevAmount = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to acquire an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))

      val left = prevAmount - amount

      if (left < 0) {
        throw new SparkException(s"Try to acquire $resourceName address $address " +
          s"amount: ${ResourceAmountUtils.toFractionalResource(amount)}, but only " +
          s"${ResourceAmountUtils.toFractionalResource(prevAmount)} left.")
      } else {
        addressAvailabilityMap(address) = left
      }
    }
  }

  /**
   * Release a sequence of resource addresses, these addresses must have been assigned. Resource
   * addresses are released when a task has finished.
   * Throw an Exception if an address is not assigned or doesn't exist.
   */
  def release(addressesAmounts: Map[String, Long]): Unit = {
    addressesAmounts.foreach { case (address, amount) =>
      val prevAmount = addressAvailabilityMap.getOrElse(address,
        throw new SparkException(s"Try to release an address that doesn't exist. $resourceName " +
          s"address $address doesn't exist."))

      val total = prevAmount + amount

      if (total > ONE_ENTIRE_RESOURCE) {
        throw new SparkException(s"Try to release $resourceName address $address " +
          s"amount: ${ResourceAmountUtils.toFractionalResource(amount)}. But the total amount: " +
          s"${ResourceAmountUtils.toFractionalResource(total)} " +
          s"after release should be <= 1")
      } else {
        addressAvailabilityMap(address) = total
      }
    }
  }
}

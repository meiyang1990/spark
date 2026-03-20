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

import org.apache.spark.resource.{ResourceAllocator, ResourceInformation}

/**
 * 保存Executor上某种资源类型信息的类。
 * 此信息由SchedulerBackend管理，TaskScheduler根据这些信息在空闲Executor上调度任务。
 *
 * @param name 资源名称（如"gpu"、"fpga"等）
 * @param addresses Executor提供的资源地址列表（如GPU的设备索引）
 */
private[spark] class ExecutorResourceInfo(
    name: String,
    addresses: Seq[String])
  extends ResourceInformation(name, addresses.toArray) with ResourceAllocator {

  override protected def resourceName = this.name       // 资源名称
  override protected def resourceAddresses = this.addresses // 资源地址列表
  /** 资源地址的总数量 */
  def totalAddressesAmount: Int = this.addresses.length

}

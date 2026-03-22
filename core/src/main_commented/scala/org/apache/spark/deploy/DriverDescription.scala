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

import org.apache.spark.resource.ResourceRequirement

/**
 * Driver描述信息容器，存储Spark Driver程序的启动配置和资源需求
 * 仅在Spark部署模块内部可见，用于集群调度器调度Driver资源时传递描述信息
 * 
 * @param jarUrl Driver程序Jar包的访问URL
 * @param mem Driver需要的内存大小，单位MB
 * @param cores Driver需要的CPU核心数
 * @param supervise 是否需要监控Driver，异常退出后自动重启
 * @param command 启动Driver的命令和参数
 * @param resourceReqs Driver自定义资源需求（如GPU、FPGA等扩展资源）
 */
private[deploy] case class DriverDescription(
    jarUrl: String,
    mem: Int,
    cores: Int,
    supervise: Boolean,
    command: Command,
    resourceReqs: Seq[ResourceRequirement] = Seq.empty) {

  /** 重写toString，输出Driver主类信息，方便日志调试 */
  override def toString: String = s"DriverDescription (${command.mainClass})"
}
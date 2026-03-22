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

/**
 * Spark Standalone集群Driver运行状态枚举定义
 * 定义了Driver在Master中的所有可能生命周期状态，用于Master对Driver的状态管理和调度
 */
private[deploy] object DriverState extends Enumeration {

  type DriverState = Value

  // SUBMITTED: 已提交但尚未调度到Worker运行
  // RUNNING: 已分配给Worker正在运行
  // FINISHED: 运行完成并正常退出
  // RELAUNCHING: 因非零退出或Worker故障，需要重新启动但尚未再次运行
  // UNKNOWN: Master故障恢复过程中，Driver状态暂时无法确定
  // KILLED: 用户手动请求杀死该Driver
  // FAILED: Driver非零退出且不需要重启监督
  // ERROR: 因不可恢复错误（如缺失jar包）无法运行或重启
  val SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR = Value
}
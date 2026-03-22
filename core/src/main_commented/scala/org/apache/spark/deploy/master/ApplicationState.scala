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
 * Spark应用在Master节点的状态枚举定义，用于标识Spark应用当前的运行状态
 */
private[master] object ApplicationState extends Enumeration {

  type ApplicationState = Value

  /** 所有应用可能状态枚举值：等待调度、运行中、已完成、运行失败、被杀死、未知状态 */
  val WAITING, RUNNING, FINISHED, FAILED, KILLED, UNKNOWN = Value
}
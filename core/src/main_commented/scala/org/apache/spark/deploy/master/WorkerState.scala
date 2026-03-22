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
 * Worker节点状态枚举类，定义了Spark Standalone集群中Worker节点的所有可能状态
 * 仅对master包可见，用于Master节点管理Worker节点生命周期状态
 */
private[master] object WorkerState extends Enumeration {
  type WorkerState = Value

  /** 存活状态：Worker正常运行可分配任务 | 死亡状态：Worker失联被判定死亡 | 已下线：Worker被主动退役下线 | 未知状态：Worker状态无法确定 */
  val ALIVE, DEAD, DECOMMISSIONED, UNKNOWN = Value
}
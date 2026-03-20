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

/**
 * 调度模式枚举：
 * - FAIR：公平调度，在Schedulable的子队列之间公平分配资源
 * - FIFO：先进先出调度，按提交顺序执行
 * - NONE：无子队列时使用（叶子节点）
 */
object SchedulingMode extends Enumeration {

  type SchedulingMode = Value
  val FAIR, FIFO, NONE = Value
}

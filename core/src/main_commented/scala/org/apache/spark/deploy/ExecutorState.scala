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

/**
 * Executor运行状态枚举，定义了Spark部署模式下Executor所有可能的生命周期状态
 */
private[deploy] object ExecutorState extends Enumeration {

  /** 状态定义：启动中、运行中、已杀死、失败、丢失、退出、已退役 */
  val LAUNCHING, RUNNING, KILLED, FAILED, LOST, EXITED, DECOMMISSIONED = Value

  /** 类型别名，指向状态值类型 */
  type ExecutorState = Value

  // DECOMMISSIONED不在已结束状态中，因为我们不希望从Worker上移除该Executor，Executor进程仍然存在
  // 但我们需要避免向已退役的Executor分配新任务
  /** 存储所有已结束的Executor状态集合 */
  private val finishedStates = Seq(KILLED, FAILED, LOST, EXITED)

  /**
   * 判断给定Executor状态是否属于已结束状态
   * @param state 待判断的Executor状态
   * @return 如果属于已结束状态返回true，否则返回false
   */
  def isFinished(state: ExecutorState): Boolean = finishedStates.contains(state)
}
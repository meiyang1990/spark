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

package org.apache.spark.ui.jobs

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.internal.config.UI.UI_THREAD_DUMPS_ENABLED
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
 * Spark Web UI的Stage标签页，负责展示所有Stage的执行进度与状态信息
 * 核心职责：管理Stage相关页面路由，提供Stage杀死功能，根据调度器模式显示对应信息
 * @param parent 父级SparkUI实例
 * @param store 应用状态存储，用于查询Stage等运行时数据
 */
private[ui] class StagesTab(val parent: SparkUI, val store: AppStatusStore)
  extends SparkUITab(parent, "stages") {

  val sc = parent.sc
  val conf = parent.conf
  val killEnabled = parent.killEnabled
  // 线程转储功能仅在应用存活且配置开启时可用
  val threadDumpEnabled =
    parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)

  // 注册所有Stage列表页面
  attachPage(new AllStagesPage(this))
  // 注册单个Stage详情页面
  attachPage(new StagePage(this, store))
  // 注册调度池页面（仅公平调度器可用）
  attachPage(new PoolPage(this))
  // 若线程转储开启，注册任务线程转储详情页面
  if (threadDumpEnabled) attachPage(new TaskThreadDumpPage(this, sc))

  /**
   * 判断当前应用是否使用FAIR公平调度模式，决定是否展示调度池信息
   * @return true表示当前是公平调度模式，false为其他模式
   */
  // Show pool information for only live UI.
  def isFairScheduler: Boolean = {
    sc.isDefined &&
    store
      .environmentInfo()
      .sparkProperties
      .contains((SCHEDULER_MODE.key, SchedulingMode.FAIR.toString))
  }

  /**
   * 处理Web UI发起的Stage杀死请求
   * @param request HTTP servlet请求对象，包含请求参数和用户信息
   */
  def handleKillRequest(request: HttpServletRequest): Unit = {
    // 校验杀死功能开启且用户拥有修改权限
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      // 从请求参数解析Stage ID
      Option(request.getParameter("id")).map(_.toInt).foreach { id =>
        // 查询Stage最新尝试信息
        store.asOption(store.lastStageAttempt(id)).foreach { stage =>
          val status = stage.status
          // 仅允许杀死处于活跃或待处理状态的Stage
          if (status == StageStatus.ACTIVE || status == StageStatus.PENDING) {
            // 调用SparkContext执行Stage取消
            sc.foreach(_.cancelStage(id, "killed via the Web UI"))
            // Do a quick pause here to give Spark time to kill the stage so it shows up as
            // killed after the refresh. Note that this will block the serving thread so the
            // time should be limited in duration.
            // 短暂等待，让Spark完成杀死操作，保证刷新后状态已更新
            Thread.sleep(100)
          }
        }
      }
    }
  }

}
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

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._

/**
 * Spark Web UI的作业标签页，展示给定SparkContext中所有作业的运行进度状态。
 * 负责管理作业列表页和单个作业详情页，并处理UI端的作业终止请求。
 * @param parent 父级Spark UI实例
 * @param store 应用状态存储，用于查询作业、阶段等运行时状态数据
 */
private[ui] class JobsTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "jobs") {

  val sc = parent.sc
  val conf = parent.conf
  val killEnabled = parent.killEnabled

  /**
   * 检测当前调度模式是否为公平调度器，仅在实时UI中展示池信息
   * @return true如果当前使用FAIR调度模式，否则返回false
   */
  // Show pool information for only live UI.
  def isFairScheduler: Boolean = {
    sc.isDefined &&
    store
      .environmentInfo()
      .sparkProperties
      .contains((SCHEDULER_MODE.key, SchedulingMode.FAIR.toString))
  }

  def getSparkUser: String = parent.getSparkUser

  // 注册所有作业列表页面
  attachPage(new AllJobsPage(this, store))
  // 注册单个作业详情页面
  attachPage(new JobPage(this, store))

  /**
   * 处理Web UI发起的作业终止请求，校验权限后调用Spark核心接口终止指定运行中作业
   * @param request HTTP Servlet请求对象，包含作业ID参数和用户权限信息
   */
  def handleKillRequest(request: HttpServletRequest): Unit = {
    // 校验终止功能开启且用户拥有修改权限
    if (killEnabled && parent.securityManager.checkModifyPermissions(request.getRemoteUser)) {
      // 解析请求参数中的作业ID
      Option(request.getParameter("id")).map(_.toInt).foreach { id =>
        // 确认作业存在
        store.asOption(store.job(id)).foreach { job =>
          // 仅终止运行中的作业
          if (job.status == JobExecutionStatus.RUNNING) {
            // 调用Spark核心接口取消作业，记录终止来源为Web UI
            sc.foreach(_.cancelJob(id, "killed via Web UI"))
            // 短暂休眠等待作业状态更新，确保刷新后能显示已终止状态，休眠时间不能过长避免阻塞服务线程
            // Do a quick pause here to give Spark time to kill the job so it shows up as
            // killed after the refresh. Note that this will block the serving thread so the
            // time should be limited in duration.
            Thread.sleep(100)
          }
        }
      }
    }
  }
}
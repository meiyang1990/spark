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

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.status.PoolData
import org.apache.spark.ui.{UIUtils, WebUIPage}

/**
 * 公平调度器池详情页面，展示指定调度池的概要信息和当前活跃的Stage列表
 * @param parent 所属的Stages标签页实例，提供上下文信息和数据存储
 */
private[ui] class PoolPage(parent: StagesTab) extends WebUIPage("pool") {

  /**
   * 渲染公平调度池详情页面
   * @param request HTTP请求对象，包含请求参数
   * @return 渲染完成的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val poolName = Option(request.getParameter("poolname")).map { poolname =>
      // 对URL编码的池名称进行解码
      UIUtils.decodeURLParameter(poolname)
    }.getOrElse {
      // 请求参数缺失池名称，抛出异常
      throw new IllegalArgumentException(s"Missing poolname parameter")
    }

    // 目前仅运行中的UI可获取调度池信息，从Spark上下文查找指定名称的调度池
    val pool = parent.sc.flatMap(_.getPoolForName(poolName)).getOrElse {
      // 找不到对应调度池，抛出异常
      throw new IllegalArgumentException(s"Unknown pool: $poolName")
    }

    // 从存储中获取调度池的展示数据，不存在则创建空数据
    val uiPool = parent.store.asOption(parent.store.pool(poolName)).getOrElse(
      new PoolData(poolName, Set()))
    // 获取调度池中所有活跃Stage的最新尝试信息
    val activeStages = uiPool.stageIds.toSeq.map(parent.store.lastStageAttempt(_))
    // 创建活跃Stage表格用于页面展示
    val activeStagesTable =
      new StageTableBase(parent.store, request, activeStages, "", "activeStage", parent.basePath,
        "stages/pool", parent.isFairScheduler, parent.killEnabled, false)

    // 创建调度池概要信息表格
    val poolTable = new PoolTable(Map(pool -> uiPool), parent)
    // 初始化页面内容，添加调度池概要标题和表格
    var content = <h4>Summary </h4> ++ poolTable.toNodeSeq(request)
    // 如果调度池存在活跃Stage，添加可折叠的活跃Stage列表区域
    if (activeStages.nonEmpty) {
      content ++=
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-poolActiveStages"
            aria-expanded="true" aria-controls="aggregated-poolActiveStages"
            data-collapse-name="collapse-aggregated-poolActiveStages">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Active Stages ({activeStages.size})</a>
          </h4>
        </span> ++
        <div class="collapsible-table collapse show" id="aggregated-poolActiveStages">
          {activeStagesTable.toNodeSeq}
        </div>
    }

    // 使用Spark UI标准页面框架包装内容并返回
    UIUtils.headerSparkPage(request, "Fair Scheduler Pool: " + poolName, content, parent)
  }
}
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

import scala.xml.{Attribute, Elem, Node, NodeSeq, Null, Text}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.status.{AppSummary, PoolData}
import org.apache.spark.status.api.v1.{StageData, StageStatus}
import org.apache.spark.ui.{UIUtils, WebUIPage}

/**
 * 所有Stage列表页面，展示应用所有运行中、已完成和失败的Stage，以及调度池信息
 * 属于Spark UI的作业监控模块，用于给用户提供全局Stage概览
 */
private[ui] class AllStagesPage(parent: StagesTab) extends WebUIPage("") {
  private val sc = parent.sc
  private val subPath = "stages"

  /**
   * 渲染所有Stage页面的HTML内容
   * @param request HTTP请求对象
   * @return 生成的页面XML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 仅在运行中的应用UI可获取调度池信息
    val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable]).map { pool =>
      val uiPool = parent.store.asOption(parent.store.pool(pool.name)).getOrElse(
        new PoolData(pool.name, Set()))
      pool -> uiPool
    }.toMap
    // 构建调度池表格组件
    val poolTable = new PoolTable(pools, parent)

    // 所有可能的Stage状态枚举
    val allStatuses = Seq(StageStatus.ACTIVE, StageStatus.PENDING, StageStatus.COMPLETE,
      StageStatus.SKIPPED, StageStatus.FAILED)

    // 从存储获取所有Stage列表和应用概要信息
    val allStages = parent.store.stageList(null)
    val appSummary = parent.store.appSummary()

    // 按状态分组生成状态汇总和Stage表格
    val (summaries, tables) = allStatuses.map(
      summaryAndTableForStatus(allStages, appSummary, _, request)).unzip

    // 生成状态汇总区域HTML
    val summary: NodeSeq =
      <div>
        <ul class="list-unstyled">
          {summaries.flatten}
        </ul>
      </div>

    // 如果使用公平调度器，生成调度池展示区域
    val poolsDescription = if (parent.isFairScheduler) {
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-poolTable"
            aria-expanded="true" aria-controls="aggregated-poolTable"
            data-collapse-name="collapse-aggregated-poolTable">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Fair Scheduler Pools ({pools.size})</a>
          </h4>
        </span> ++
        <div class="collapsible-table collapse show" id="aggregated-poolTable">
          {poolTable.toNodeSeq(request)}
        </div>
      } else {
        Seq.empty[Node]
      }

    // 组合所有内容区域
    val content = summary ++ poolsDescription ++ tables.flatten.flatten

    // 使用通用Spark页面模板渲染最终页面
    UIUtils.headerSparkPage(request, "Stages for All Jobs", content, parent)
  }

  /**
   * 为指定状态的Stage生成汇总信息和表格HTML
   * @param allStages 所有Stage列表
   * @param appSummary 应用概要信息
   * @param status 目标Stage状态
   * @param request HTTP请求
   * @return (汇总元素, 表格节点)的二元组，无对应Stage时返回(None, None)
   */
  private def summaryAndTableForStatus(
      allStages: Seq[StageData],
      appSummary: AppSummary,
      status: StageStatus,
      request: HttpServletRequest): (Option[Elem], Option[NodeSeq]) = {
    // 失败Stage按倒序排列，最新失败的显示在前面
    val stages = if (status == StageStatus.FAILED) {
      allStages.filter(_.status == status).reverse
    } else {
      allStages.filter(_.status == status)
    }

    if (stages.isEmpty) {
      (None, None)
    } else {
      // 仅活跃Stage支持终止操作
      val killEnabled = status == StageStatus.ACTIVE && parent.killEnabled
      val isFailedStage = status == StageStatus.FAILED

      // 构建Stage表格基础组件
      val stagesTable =
        new StageTableBase(parent.store, request, stages, statusName(status), stageTag(status),
          parent.basePath, subPath, parent.isFairScheduler, killEnabled, isFailedStage)
      val stagesSize = stages.size
      (Some(summary(appSummary, status, stagesSize)),
        Some(table(appSummary, status, stagesTable, stagesSize)))
    }
  }

  /** 获取Stage状态对应的字符串名称 */
  private def statusName(status: StageStatus): String = status match {
    case StageStatus.ACTIVE => "active"
    case StageStatus.COMPLETE => "completed"
    case StageStatus.FAILED => "failed"
    case StageStatus.PENDING => "pending"
    case StageStatus.SKIPPED => "skipped"
  }

  /** 获取Stage状态对应的CSS类标签 */
  private def stageTag(status: StageStatus): String = s"${statusName(status)}Stage"

  /** 获取状态标题文本，首字母大写 */
  private def headerDescription(status: StageStatus): String = statusName(status).capitalize

  /** 生成汇总内容文本，已完成Stage如果有截断显示实际总数和展示数量 */
  private def summaryContent(appSummary: AppSummary, status: StageStatus, size: Int): String = {
    if (status == StageStatus.COMPLETE && appSummary.numCompletedStages != size) {
      s"${appSummary.numCompletedStages}, only showing $size"
    } else {
      s"$size"
    }
  }

  /** 生成状态汇总的HTML元素 */
  private def summary(appSummary: AppSummary, status: StageStatus, size: Int): Elem = {
    val summary =
      <li>
        <a href={s"#${statusName(status)}"}>
          <strong>{headerDescription(status)} Stages:</strong>
        </a>
        {summaryContent(appSummary, status, size)}
      </li>

    // 已完成汇总添加特定ID用于页面锚点
    if (status == StageStatus.COMPLETE) {
      summary % Attribute(None, "id", Text("completed-summary"), Null)
    } else {
      summary
    }
  }

  /** 生成对应状态Stage的可折叠表格HTML */
  private def table(
      appSummary: AppSummary,
      status: StageStatus,
      stagesTable: StageTableBase,
      size: Int): NodeSeq = {
    val classSuffix = s"${statusName(status).capitalize}Stages"
    <span id={statusName(status)}
          class="collapse-table" data-bs-toggle="collapse"
          data-bs-target={s"#aggregated-all$classSuffix"}
          aria-expanded="true" aria-controls={s"aggregated-all$classSuffix"}
          data-collapse-name={s"collapse-aggregated-all$classSuffix"}>
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>{headerDescription(status)} Stages ({summaryContent(appSummary, status, size)})</a>
      </h4>
    </span> ++
      <div class="collapsible-table collapse show" id={s"aggregated-all$classSuffix"}>
        {stagesTable.toNodeSeq}
      </div>
  }
}
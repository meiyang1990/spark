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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.scheduler.Schedulable
import org.apache.spark.status.PoolData
import org.apache.spark.ui.UIUtils

/**
 * 调度池列表展示表格，用于Spark Web UI中展示公平调度器的调度池信息
 * @param pools 待展示的调度池数据，key为调度可执行对象，value为调度池统计数据
 * @param parent 所属的Stage页面标签页
 */
private[ui] class PoolTable(pools: Map[Schedulable, PoolData], parent: StagesTab) {

  /**
   * 将调度池表格转换为XML节点序列，用于页面渲染
   * @param request HTTP请求对象，用于获取基础路径信息
   * @return 渲染完成的表格XML节点序列
   */
  def toNodeSeq(request: HttpServletRequest): Seq[Node] = {
    <table class="table table-bordered table-striped table-sm sortable table-fixed">
      <thead>
        <tr>
          <th>Pool Name</th>
          <th>
            {UIUtils.tooltipSpan(<xml:group>Minimum Share</xml:group>,
              "Pool's minimum share of CPU cores")}
          </th>
          <th>
            {UIUtils.tooltipSpan(<xml:group>Pool Weight</xml:group>,
              "Pool's share of cluster resources relative to others")}
          </th>
          <th>Active Stages</th>
          <th>Running Tasks</th>
          <th>SchedulingMode</th>
        </tr>
      </thead>
      <tbody>
        {pools.map { case (s, p) => poolRow(request, s, p) }}
      </tbody>
    </table>
  }

  /**
   * 生成单个调度池的表格行HTML节点
   * @param request HTTP请求对象，用于获取基础路径信息
   * @param s 调度可执行对象，包含调度配置信息
   * @param p 调度池统计数据，包含运行时信息
   * @return 调度池行的XML节点序列
   */
  private def poolRow(request: HttpServletRequest, s: Schedulable, p: PoolData): Seq[Node] = {
    val activeStages = p.stageIds.size
    val href = "%s/stages/pool?poolname=%s"
      .format(UIUtils.prependBaseUri(request, parent.basePath),
        URLEncoder.encode(p.name, StandardCharsets.UTF_8.name()))
    <tr>
      <td>
        <a href={href}>{p.name}</a>
      </td>
      <td>{s.minShare}</td>
      <td>{s.weight}</td>
      <td>{activeStages}</td>
      <td>{s.runningTasks}</td>
      <td>{s.schedulingMode}</td>
    </tr>
  }
}
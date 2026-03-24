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

package org.apache.spark.ui.exec

import scala.xml.{Node, Text}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.ui.{SparkUITab, UIUtils, WebUIPage}

/**
 * 执行器堆直方图页面，负责渲染Executor堆内存使用统计信息的Web UI页面
 * 展示Executor中各个类的实例数量和内存占用，用于内存使用分析和内存泄漏排查
 */
private[ui] class ExecutorHeapHistogramPage(
    parent: SparkUITab,
    sc: Option[SparkContext]) extends WebUIPage("heapHistogram") {

  // 正则匹配包含对象信息的行，提取排名、实例数、字节数、类名和模块信息
  val pattern = """\s*([0-9]+):\s+([0-9]+)\s+([0-9]+)\s+(\S+)(.*)""".r

  /**
   * 渲染Executor堆直方图页面
   * @param request HTTP请求对象，包含executorId参数
   * @return 渲染后的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map { executorId =>
      UIUtils.decodeURLParameter(executorId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }
    // 记录当前时间，用于显示更新时间
    val time = System.currentTimeMillis()
    // 从SparkContext获取目标Executor的堆直方图数据
    val maybeHeapHistogram = sc.get.getExecutorHeapHistogram(executorId)

    // 生成页面HTML内容
    val content = maybeHeapHistogram.map { heapHistogram =>
      // 将每行堆数据转换为HTML表格行
      val rows = heapHistogram.map { row =>
        row match {
          // 匹配包含模块信息的行
          case pattern(rank, instances, bytes, name, module) =>
            <tr class="accordion-heading">
              <td>{rank}</td>
              <td>{instances}</td>
              <td>{bytes}</td>
              <td>{name}</td>
              <td>{module}</td>
            </tr>
          // 匹配不包含模块信息的行
          case pattern(rank, instances, bytes, name) =>
            <tr class="accordion-heading">
              <td>{rank}</td>
              <td>{instances}</td>
              <td>{bytes}</td>
              <td>{name}</td>
              <td></td>
            </tr>
          // 忽略表头和汇总行，不生成表格行
          case _ =>
            // Ignore the first two lines and the last line
            //
            //  num     #instances         #bytes  class name (module)
            // -------------------------------------------------------
            // ...
            // Total       1267867       72845688
        }
      }
      // 构建完整页面结构
      <div class="row">
        <div class="col-12">
          <p>Updated at {UIUtils.formatDate(time)}</p>
          <table class={UIUtils.TABLE_CLASS_STRIPED + " accordion-group" + " sortable"}>
            <thead>
              <th>Rank</th>
              <th>Instances</th>
              <th>Bytes</th>
              <th>Class Name</th>
              <th>Module</th>
            </thead>
            <tbody>{rows}</tbody>
          </table>
        </div>
      </div>
    // 获取堆数据失败时显示错误信息
    }.getOrElse(Text("Error fetching heap histogram"))
    // 使用Spark页面模板生成完整HTML页面
    UIUtils.headerSparkPage(request, s"Heap Histogram for Executor $executorId", content, parent)
  }
}
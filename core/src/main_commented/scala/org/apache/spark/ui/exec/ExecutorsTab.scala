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

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.config.UI._
import org.apache.spark.ui.{CspNonce, SparkUI, SparkUITab, UIUtils, WebUIPage}

/**
 * Spark Web UI 执行器页面标签页，负责展示所有执行器的监控信息
 * 挂载在Spark UI下，提供执行器列表、线程dump、堆直方图等功能入口
 * @param parent 父Spark UI实例
 */
private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {

  init()

  /**
   * 初始化执行器标签页，根据配置注册对应的页面
   */
  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)
    val heapHistogramEnabled =
      parent.sc.isDefined && parent.conf.get(UI_HEAP_HISTOGRAM_ENABLED)

    attachPage(new ExecutorsPage(this, threadDumpEnabled, heapHistogramEnabled))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
    if (heapHistogramEnabled) {
      attachPage(new ExecutorHeapHistogramPage(this, parent.sc))
    }
  }

}

/**
 * 执行器列表主页面，渲染所有执行器的基础统计信息页面
 * @param parent 父执行器标签页实例
 * @param threadDumpEnabled 是否开启线程dump功能
 * @param heapHistogramEnabled 是否开启堆直方图功能
 */
private[ui] class ExecutorsPage(
    parent: SparkUITab,
    threadDumpEnabled: Boolean,
    heapHistogramEnabled: Boolean)
  extends WebUIPage("") {

  /**
   * 渲染执行器列表页面的HTML内容
   * @param request HTTP请求对象
   * @return 渲染后的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 导入前端JavaScript方法，开启CSP兼容
    val imported = UIUtils.formatImportJavaScript(
      request,
      "/static/executorspage.js",
      "setThreadDumpEnabled",
      "setHeapHistogramEnabled")
    // 生成初始化JavaScript，向前端传递功能开关配置
    val js =
      s"""
         |$imported
         |
         |setThreadDumpEnabled($threadDumpEnabled);
         |setHeapHistogramEnabled($heapHistogramEnabled)
         |""".stripMargin
    // 构建页面HTML结构，包含执行器列表容器和执行器详情侧滑面板
    val content =
      {
        <div id="active-executors"></div> ++
        <div class="offcanvas offcanvas-end" tabindex="-1" id="executor-detail-offcanvas"
             aria-labelledby="executor-detail-offcanvas-label"
             style="width: 60vw; max-width: 900px;">
          <div class="offcanvas-resize-handle" id="offcanvas-resize-handle"></div>
          <div class="offcanvas-header">
            <h5 class="offcanvas-title" id="executor-detail-offcanvas-label"></h5>
            <button type="button" class="btn-close" data-bs-dismiss="offcanvas"
                    aria-label="Close"></button>
          </div>
          <div class="offcanvas-body" id="executor-detail-offcanvas-body">
          </div>
        </div> ++
        // 引入公共工具JS
        <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
        // 引入执行器页面业务JS
        <script type="module"
                src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
        // 注入初始化配置脚本
        <script type="module" nonce={CspNonce.get}>{Unparsed(js)}</script>
      }

    // 使用Spark UI标准页面模板包装生成最终页面
    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }
}
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

import scala.xml.{Node, Text, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.internal.config.UI.UI_FLAMEGRAPH_ENABLED
import org.apache.spark.status.api.v1.ThreadStackTrace
import org.apache.spark.ui.{CspNonce, SparkUITab, UIUtils, WebUIPage}
import org.apache.spark.ui.UIUtils.{formatImportJavaScript, prependBaseUri}
import org.apache.spark.ui.flamegraph.FlamegraphNode

/**
 * 执行器线程栈Dump页面，用于在Spark WebUI展示指定Executor的所有线程栈信息，支持火焰图可视化
 */
private[ui] class ExecutorThreadDumpPage(
    parent: SparkUITab,
    sc: Option[SparkContext]) extends WebUIPage("threadDump") {

  // 标记火焰图功能是否启用，由配置UI_FLAMEGRAPH_ENABLED决定
  private val flamegraphEnabled = sc.isDefined && sc.get.conf.get(UI_FLAMEGRAPH_ENABLED)

  /**
   * 渲染线程Dump页面主内容
   * @param request HTTP请求对象
   * @return 渲染后的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 解析并获取请求中的Executor ID参数
    val executorId = Option(request.getParameter("executorId")).map { executorId =>
      UIUtils.decodeURLParameter(executorId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }
    // 记录当前请求时间，用于显示更新时间
    val time = System.currentTimeMillis()
    // 从SparkContext获取指定Executor的线程栈Dump
    val maybeThreadDump = sc.get.getExecutorThreadDump(executorId)

    // 生成页面主内容
    val content = maybeThreadDump.map { threadDump =>
      // 为每个线程生成表格行
      val dumpRows = threadDump.map { thread =>
        val threadId = thread.threadId
        // 处理线程阻塞信息，如果被其他线程阻塞，生成跳转链接
        val blockedBy = thread.blockedByThreadId match {
          case Some(blockingThreadId) =>
            <div>
              Blocked by <a href={s"#${blockingThreadId}_td_id"}>
              Thread {blockingThreadId} {thread.blockedByLock}</a>
            </div>
          case None => Text("")
        }
        // 整理线程持有的同步锁和监视器信息
        val synchronizers = thread.synchronizers.map(l => s"Lock($l)")
        val monitors = thread.monitors.map(m => s"Monitor($m)")
        val heldLocks = (synchronizers ++ monitors).mkString(", ")

        <tr id={s"thread_${threadId}_tr"} class="accordion-heading"
            data-thread-id={threadId.toString}>
          <td id={s"${threadId}_td_id"}>{threadId}</td>
          <td id={s"${threadId}_td_name"}>{thread.threadName}</td>
          <td id={s"${threadId}_td_state"}>{thread.threadState}</td>
          <td id={s"${threadId}_td_locking"}>{blockedBy}{heldLocks}</td>
          <td id={s"${threadId}_td_stacktrace"} class="d-none">{thread.stackTrace.html}</td>
        </tr>
      }

    <div class="row">
      <div class="col-12">
        <p>Updated at {UIUtils.formatDate(time)}</p>
        {threadDumpSummary(threadDump)}
        { if (flamegraphEnabled) {
            drawExecutorFlamegraph(request, threadDump) }
          else {
            Seq.empty
          }
        }
        {
          // scalastyle:off
          <p></p>
          <span class="collapse-table" data-bs-toggle="collapse"
                data-bs-target="#thead-stack-trace-table"
                aria-expanded="true" aria-controls="thead-stack-trace-table"
                data-collapse-name="collapse-thead-stack-trace-table">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Thread Stack Trace</a>
            </h4>
          </span>
        }
        <div class="collapsible-table collapse show" id="thead-stack-trace-table">
          {
          // scalastyle:off
          <div class="thead-stack-trace-table-button d-flex align-items-center">
            <a class="expandbutton" data-action="expandAllThreadStackTrace">Expand All</a>
            <a class="expandbutton d-none" data-action="collapseAllThreadStackTrace">Collapse All</a>
            // 生成线程Dump文件下载链接
            <a class="downloadbutton" href={"data:text/plain;charset=utf-8," + threadDump.map(_.toString).mkString} download={"threaddump_" + executorId + ".txt"}>Download</a>
            <div class="d-flex">
              <div class="bs-example" data-example-id="simple-d-flex">
                <div class="mb-3">
                  <div class="input-group">
                    <label class="me-2" for="search">Search:</label>
                    // 线程栈搜索输入框
                    <input type="text" class="form-control" id="search" data-search-input="true"></input>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <p></p>
          }
          <table class={UIUtils.TABLE_CLASS_STRIPED + " accordion-group" + " sortable"}>
            <thead>
              <th data-action="collapseAllThreadStackTrace" data-toggle-button="false">Thread ID</th>
              <th data-action="collapseAllThreadStackTrace" data-toggle-button="false">Thread Name</th>
              <th data-action="collapseAllThreadStackTrace" data-toggle-button="false">Thread State</th>
              <th data-action="collapseAllThreadStackTrace" data-toggle-button="false">
                {UIUtils.tooltipSpan(<xml:group>Thread Locks</xml:group>,
                  "Objects whose lock the thread currently holds")}
              </th>
            </thead>
            <tbody>{dumpRows}</tbody>
          </table>
        </div>
      </div>
    </div>
    }.getOrElse(Text("Error fetching thread dump"))
    // 使用Spark UI标准页面框架包装内容并返回
    UIUtils.headerSparkPage(request, s"Thread dump for executor $executorId", content, parent)
  }

  /**
   * 生成线程栈火焰图的HTML内容，用于可视化调用栈分布
   * @param request HTTP请求对象
   * @param thread 线程栈数组
   * @return 火焰图区域HTML节点序列
   */
  private def drawExecutorFlamegraph(request: HttpServletRequest, thread: Array[ThreadStackTrace]): Seq[Node] = {
    // 初始化火焰图绘制的JS代码
    val js =
      s"""
         |${formatImportJavaScript(request, "/static/flamegraph.js", "drawFlamegraph", "toggleFlamegraph")}
         |
         |drawFlamegraph();
         |toggleFlamegraph();
         |""".stripMargin
    <div>
      <div>
        <span id="executor-flamegraph-header" class="cursor-pointer">
          <h4>
            <span id="executor-flamegraph-arrow" class="arrow-open"></span>
            <a>Flame Graph</a>
          </h4>
        </span>
      </div>
      // 存储序列化后的火焰图JSON数据，隐藏不展示
      <div id="executor-flamegraph-data" class="d-none">{FlamegraphNode(thread).toJsonString}</div>
      // 火焰图绘图容器，引入依赖的CSS和JS资源
      <div id="executor-flamegraph-chart">
        <link rel="stylesheet" type="text/css" href={prependBaseUri(request, "/static/d3-flamegraph.css")}></link>
        <script src={UIUtils.prependBaseUri(request, "/static/d3.min.js")}></script>
        <script src={UIUtils.prependBaseUri(request, "/static/d3-flamegraph.min.js")}></script>
        <script type="module" src={UIUtils.prependBaseUri(request, "/static/flamegraph.js")}></script>
        <script type="module" nonce={CspNonce.get}>{Unparsed(js)}</script>
      </div>
    </div>
  }


  /**
   * 生成线程Dump统计摘要，按线程状态分组统计数量和占比
   * @param threadDump 线程栈数组
   * @return 统计摘要HTML节点序列
   */
  private def threadDumpSummary(threadDump: Array[ThreadStackTrace]): Seq[Node] = {
    val totalCount = threadDump.length
    <div>
      <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#thread-dump-summary-table"
            aria-expanded="true" aria-controls="thread-dump-summary-table"
            data-collapse-name="thead-dump-summary">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Thread Dump Summary: { totalCount }</a>
        </h4>
      </span>
      <div class="collapsible-table collapse show" id="thread-dump-summary-table">
      <table class={UIUtils.TABLE_CLASS_STRIPED + " accordion-group" + " sortable"}>
        <thead><th>Thread State</th><th>Count</th><th>Percentage</th></thead>
        <tbody>
          {
          // 按线程状态分组，生成统计行
          threadDump.groupBy(_.threadState).map { case (state, threads) =>
            <tr>
              <td>{state}</td>
              <td>{threads.length}</td>
              <td>{"%.2f%%".format(threads.length * 100.0 / totalCount)}</td>
            </tr>
          }.toSeq
          }
        </tbody>
      </table>
      </div>
    </div>
  }
  // scalastyle:on
}
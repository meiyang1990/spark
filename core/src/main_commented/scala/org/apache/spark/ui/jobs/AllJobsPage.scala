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
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Date, Locale}

import scala.collection.mutable.ListBuffer
import scala.xml._

import jakarta.servlet.http.HttpServletRequest
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/**
 * Spark Web UI中显示所有正在运行和已结束Job列表的页面
 * 按状态分类展示活动/完成/失败Job，提供时间线视图和分页表格展示
 * @param parent 所属JobsTab父标签页
 * @param store 应用状态存储，用于获取Job和Executor数据
 */
private[ui] class AllJobsPage(parent: JobsTab, store: AppStatusStore) extends WebUIPage("") {

  import ApiHelper._

  // 是否启用时间线视图，从配置读取
  private val TIMELINE_ENABLED = parent.conf.get(UI_TIMELINE_ENABLED)
  // 时间线最大展示Job数量，从配置读取
  private val MAX_TIMELINE_JOBS = parent.conf.get(UI_TIMELINE_JOBS_MAXIMUM)
  // 时间线最大展示Executor事件数量，从配置读取
  private val MAX_TIMELINE_EXECUTORS = parent.conf.get(UI_TIMELINE_EXECUTORS_MAXIMUM)

  // Job状态图例HTML，移除换行符适配前端渲染
  private val JOBS_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="succeeded-job-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Succeeded</text>
      <rect class="failed-job-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="running-job-legend"
        x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Running</text>
    </svg></div>.toString.filter(_ != '\n')

  // Executor状态图例HTML，移除换行符适配前端渲染
  private val EXECUTORS_LEGEND =
    <div class="legend-area"><svg width="150px" height="55px">
      <rect class="executor-added-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Added</text>
      <rect class="executor-removed-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Removed</text>
    </svg></div>.toString.filter(_ != '\n')

  /**
   * 将Job数据转换为时间线组件需要的JSON事件字符串
   * @param jobs 待转换的Job列表
   * @return 每个Job对应一个JSON格式事件字符串
   */
  private def makeJobEvent(jobs: Seq[v1.JobData]): Seq[String] = {
    val now = System.currentTimeMillis()
    // 过滤未知状态和未提交的Job
    jobs.filter { job =>
      job.status != JobExecutionStatus.UNKNOWN && job.submissionTime.isDefined
    // 按完成时间（未完成用当前时间）、提交时间排序
    }.sortBy { j =>
      (j.completionTime.map(_.getTime).getOrElse(now), j.submissionTime.get.getTime)
    // 只保留最近N个Job，最多展示配置限制数量
    }.takeRight(MAX_TIMELINE_JOBS).map { job =>
      val jobId = job.jobId
      val status = job.status
      // 获取最后一个Stage的名称和描述，用于展示Job描述
      val (_, lastStageDescription) = lastStageNameAndDescription(store, job)
      val jobDescription = UIUtils.makeDescription(
        job.description.getOrElse(lastStageDescription),
        "",
        plainText = true).text

      val submissionTime = job.submissionTime.get.getTime()
      val completionTime = job.completionTime.map(_.getTime()).getOrElse(now)
      // 根据Job状态设置CSS类名，用于前端样式区分
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // 时间线库按HTML解析内容，需要转义；嵌入JS字符串需要额外转义
      val escapedDesc = Utility.escape(jobDescription)
      val jsEscapedDescForTooltip = StringEscapeUtils.escapeEcmaScript(Utility.escape(escapedDesc))
      val jsEscapedDescForLabel = StringEscapeUtils.escapeEcmaScript(escapedDesc)
      // 构造Job事件JSON，包含状态、时间、提示信息等
      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content"' +
           |     'data-bs-html="true" data-bs-toggle="tooltip"' +
           |     'data-bs-title="${jsEscapedDescForTooltip} (Job ${jobId})<br>' +
           |     'Status: ${status}<br>' +
           |     'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |     '${
                     if (status != JobExecutionStatus.RUNNING) {
                       s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
                     } else {
                       ""
                     }
                  }">' +
           |    '${jsEscapedDescForLabel} (Job ${jobId})</div>'
           |}
         """.stripMargin
      jobEventJsonAsStr
    }
  }

  /**
   * 将Executor添加/移除事件转换为时间线组件需要的JSON事件字符串
   * @param executors 待转换的Executor列表
   * @return 每个添加/移除事件对应一个JSON格式事件字符串
   */
  private def makeExecutorEvent(executors: Seq[v1.ExecutorSummary]):
      Seq[String] = {
    val events = ListBuffer[String]()
    // 按添加/移除时间排序，只保留最近N个Executor
    executors.sortBy { e =>
      e.removeTime.map(_.getTime).getOrElse(e.addTime.getTime)
    }.takeRight(MAX_TIMELINE_EXECUTORS).foreach { e =>
      // 构造Executor添加事件JSON
      val addedEvent =
        s"""
           |{
           |  'className': 'executor added',
           |  'group': 'executors',
           |  'start': new Date(${e.addTime.getTime()}),
           |  'content': '<div class="executor-event-content"' +
           |    'data-bs-toggle="tooltip"' +
           |    'data-bs-title="Executor ${e.id}<br>' +
           |    'Added at ${UIUtils.formatDate(e.addTime)}"' +
           |    'data-bs-html="true">Executor ${e.id} added</div>'
           |}
         """.stripMargin
      events += addedEvent

      // 如果Executor已移除，添加移除事件JSON
      e.removeTime.foreach { removeTime =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${removeTime.getTime()}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-bs-toggle="tooltip"' +
             |    'data-bs-title="Executor ${e.id}<br>' +
             |    'Removed at ${UIUtils.formatDate(removeTime)}' +
             |    '${
                      e.removeReason.map { reason =>
                        s"""<br>Reason: ${StringEscapeUtils.escapeEcmaScript(
                          reason.replace("\n", " "))}"""
                      }.getOrElse("")
                   }"' +
             |    'data-bs-html="true">Executor ${e.id} removed</div>'
             |}
           """.stripMargin
        events += removedEvent
      }
    }
    events.toSeq
  }

  /**
   * 生成Job和Executor事件时间线的HTML节点
   * @param jobs 所有Job数据
   * @param executors 所有Executor数据
   * @param startTime 应用启动时间，用于时间轴基准
   * @return 时间线区域的XML节点序列
   */
  private def makeTimeline(
      jobs: Seq[v1.JobData],
      executors: Seq[v1.ExecutorSummary],
      startTime: Long): Seq[Node] = {

    // 配置禁用时间线则返回空
    if (!TIMELINE_ENABLED) return Seq.empty[Node]

    val jobEventJsonAsStrSeq = makeJobEvent(jobs)
    val executorEventJsonAsStrSeq = makeExecutorEvent(executors)

    // 时间线分组配置JSON，包含分组标题和图例
    val groupJsonArrayAsStr =
      s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${EXECUTORS_LEGEND}',
          |  },
          |  {
          |    'id': 'jobs',
          |    'content': '<div>Jobs</div>${JOBS_LEGEND}',
          |  }
          |]
        """.stripMargin

    // 合并所有事件，生成事件数组JSON字符串
    val eventArrayAsStr =
      (jobEventJsonAsStrSeq ++ executorEventJsonAsStrSeq).mkString("[", ",", "]")

    // 时间线展开按钮
    <span class="expand-application-timeline">
      <span class="expand-application-timeline-arrow arrow-closed"></span>
      {UIUtils.tooltipLink(<xml:group>Event Timeline</xml:group>, ToolTips.JOB_TIMELINE)}
    </span> ++
    // 时间线容器
    <div id="application-timeline" class="collapsed">
      {
        // Job数量超过最大限制，显示提示信息
        if (MAX_TIMELINE_JOBS < jobs.size) {
          <div>
            <strong>
              Only the most recent {MAX_TIMELINE_JOBS} submitted/completed jobs
              (of {jobs.size} total) are shown.
            </strong>
          </div>
        } else {
          Seq.empty
        }
      }
      {
        // Executor数量超过最大限制，显示提示信息
        if (MAX_TIMELINE_EXECUTORS < executors.size) {
          <div>
            <strong>
              Only the most recent {MAX_TIMELINE_EXECUTORS} added/removed executors
              (of {executors.size} total) are shown.
            </strong>
          </div>
        } else {
          Seq.empty
        }
      }
      // 缩放控制面板
      <div class="control-panel">
        <div id="application-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
    </div> ++
    // 调用前端JS绘制时间线，注入配置和事件数据
    <script type="text/javascript" nonce={CspNonce.get}>
      {Unparsed(s"drawApplicationTimeline(${groupJsonArrayAsStr}," +
      s"${eventArrayAsStr}, ${startTime}, ${UIUtils.getTimeZoneOffset()});")}
    </script>
  }

  /**
   * 生成指定分类Job的分页表格HTML
   * @param request HTTP请求对象
   * @param tableHeaderId 表格锚点ID
   * @param jobTag Job分类标签（active/completed/failed）
   * @param jobs 该分类下的Job列表
   * @param killEnabled 是否允许终止Job
   * @return Job表格的XML节点序列
   */
  private def jobsTable(
      request: HttpServletRequest,
      tableHeaderId: String,
      jobTag: String,
      jobs: Seq[v1.JobData],
      killEnabled: Boolean): Seq[Node] = {

    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)
    // 根据是否有JobGroup调整Job ID列标题
    val jobIdTitle = if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"
    // 从请求参数获取当前页码，默认第1页
    val jobPage = Option(request.getParameter(jobTag + ".page")).map(_.toInt).getOrElse(1)

    try {
      // 构造分页表格并渲染指定页
      new JobPagedTable(
        request,
        store,
        jobs,
        tableHeaderId,
        jobTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "jobs", // subPath
        killEnabled,
        jobIdTitle
      ).table(jobPage)
    } catch {
      // 页码异常捕获，显示错误信息
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        <div class="alert alert-danger">
          <p>Error while rendering job table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }

  /**
   * 渲染所有Jobs页面主内容，是页面入口方法
   * @param request HTTP请求对象
   * @return 完整页面XML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val appInfo = store.applicationInfo()
    val startDate = appInfo.attempts.head.startTime
    val startTime = startDate.getTime()
    val endTime = appInfo.attempts.head.endTime.getTime()

    // 按状态分类存储Job
    val activeJobs = new ListBuffer[v1.JobData]()
    val completedJobs = new ListBuffer[v1.JobData]()
    val failedJobs = new ListBuffer[v1.JobData]()

    // 从存储加载所有Job并分类
    store.jobsList(null).foreach { job =>
      job.status match {
        case JobExecutionStatus.SUCCEEDED =>
          completedJobs += job
        case JobExecutionStatus.FAILED =>
          failedJobs += job
        case _ =>
          activeJobs += job
      }
    }

    // 生成各分类Job表格
    val activeJobsTable =
      jobsTable(request, "active", "activeJob", activeJobs.toSeq, killEnabled = parent.killEnabled)
    val completedJobsTable =
      jobsTable(request, "completed", "completedJob", completedJobs.toSeq, killEnabled = false)
    val failedJobsTable =
      jobsTable(request, "failed", "failedJob", failedJobs.toSeq, killEnabled = false)

    // 判断是否需要展示对应分类
    val shouldShowActiveJobs = activeJobs.nonEmpty
    val shouldShowCompletedJobs = completedJobs.nonEmpty
    val shouldShowFailedJobs = failedJobs.nonEmpty

    val appSummary = store.appSummary()
    // 已完成Job数量描述，如果存储中总数大于当前展示，说明截断显示
    val completedJobNumStr = if (completedJobs.size == appSummary.numCompletedJobs) {
      s"${completedJobs.size}"
    } else {
      s"${appSummary.numCompletedJobs}, only showing ${completedJobs.size}"
    }

    // SPARK-33991 处理枚举转换错误，获取调度模式名称
    val schedulingMode = store.environmentInfo().sparkProperties.toMap
      .get(SCHEDULER_MODE.key)
      .map {
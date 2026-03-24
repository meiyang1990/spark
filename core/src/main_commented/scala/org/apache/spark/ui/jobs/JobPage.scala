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

import java.util.Locale

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.xml.{Node, NodeSeq, Unparsed, Utility}

import jakarta.servlet.http.HttpServletRequest
import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.config.UI._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._

/**
 * Spark UI 单个Job详情页面，展示指定Job的统计信息和所属Stage列表
 * @param parent 父级JobsTab页面
 * @param store 应用状态存储，用于查询Job和Stage数据
 */
private[ui] class JobPage(parent: JobsTab, store: AppStatusStore) extends WebUIPage("job") {

  // 是否启用时间线视图，从UI配置读取
  private val TIMELINE_ENABLED = parent.conf.get(UI_TIMELINE_ENABLED)
  // 时间线最多展示的Stage数量
  private val MAX_TIMELINE_STAGES = parent.conf.get(UI_TIMELINE_STAGES_MAXIMUM)
  // 时间线最多展示的Executor事件数量
  private val MAX_TIMELINE_EXECUTORS = parent.conf.get(UI_TIMELINE_EXECUTORS_MAXIMUM)

  // Stage状态图例HTML字符串，移除换行符
  private val STAGES_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="completed-stage-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Completed</text>
      <rect class="failed-stage-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="active-stage-legend"
        x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Active</text>
    </svg></div>.toString.filter(_ != '\n')

  // Executor生命周期图例HTML字符串，移除换行符
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
   * 为时间线视图生成Stage事件的JSON对象字符串
   * @param stageInfos Stage数据列表
   * @return 每个Stage对应一个JSON格式字符串
   */
  private def makeStageEvent(stageInfos: Seq[v1.StageData]): Seq[String] = {
    val now = System.currentTimeMillis()
    stageInfos.sortBy { s =>
      (s.completionTime.map(_.getTime).getOrElse(now), s.submissionTime.get.getTime)
    }.takeRight(MAX_TIMELINE_STAGES).map { stage =>
      val stageId = stage.stageId
      val attemptId = stage.attemptId
      val name = stage.name
      val status = stage.status.toString.toLowerCase(Locale.ROOT)
      val submissionTime = stage.submissionTime.get.getTime()
      val completionTime = stage.completionTime.map(_.getTime())
        .getOrElse(now)

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a JavaScript string literal.
      val escapedName = Utility.escape(name)
      val jsEscapedNameForTooltip = StringEscapeUtils.escapeEcmaScript(Utility.escape(escapedName))
      val jsEscapedNameForLabel = StringEscapeUtils.escapeEcmaScript(escapedName)
      s"""
         |{
         |  'className': 'stage job-timeline-object ${status}',
         |  'group': 'stages',
         |  'start': new Date(${submissionTime}),
         |  'end': new Date(${completionTime}),
         |  'content': '<div class="job-timeline-content" data-bs-toggle="tooltip"' +
         |   'data-bs-html="true"' +
         |   'data-bs-title="${jsEscapedNameForTooltip} (Stage ${stageId}.${attemptId})<br>' +
         |   'Status: ${status.toUpperCase(Locale.ROOT)}<br>' +
         |   'Submitted: ${UIUtils.formatDate(submissionTime)}' +
         |   '${
                 if (status != "running") {
                   s"""<br>Completed: ${UIUtils.formatDate(completionTime)}"""
                 } else {
                   ""
                 }
              }">' +
         |    '${jsEscapedNameForLabel} (Stage ${stageId}.${attemptId})</div>',
         |}
       """.stripMargin
    }
  }

  /**
   * 为时间线视图生成Executor添加/移除事件的JSON对象字符串
   * @param executors Executor汇总信息列表
   * @return 每个事件对应一个JSON格式字符串
   */
  def makeExecutorEvent(executors: Seq[v1.ExecutorSummary]): Seq[String] = {
    val events = ListBuffer[String]()
    executors.sortBy { e =>
      e.removeTime.map(_.getTime).getOrElse(e.addTime.getTime)
    }.takeRight(MAX_TIMELINE_EXECUTORS).foreach { e =>
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
   * 生成Job执行时间线视图的HTML节点，包含Stage和Executor生命周期的时间线展示
   * @param stages Stage数据列表
   * @param executors Executor汇总信息列表
   * @param appStartTime 应用启动时间戳
   * @return 时间线视图HTML节点序列
   */
  private def makeTimeline(
      stages: Seq[v1.StageData],
      executors: Seq[v1.ExecutorSummary],
      appStartTime: Long): Seq[Node] = {

    if (!TIMELINE_ENABLED) return Seq.empty[Node]

    val stageEventJsonAsStrSeq = makeStageEvent(stages)
    val executorsJsonAsStrSeq = makeExecutorEvent(executors)

    val groupJsonArrayAsStr =
      s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${EXECUTORS_LEGEND}',
          |  },
          |  {
          |    'id': 'stages',
          |    'content': '<div>Stages</div>${STAGES_LEGEND}',
          |  }
          |]
        """.stripMargin

    val eventArrayAsStr =
      (stageEventJsonAsStrSeq ++ executorsJsonAsStrSeq).mkString("[", ",", "]")

    <span class="expand-job-timeline">
      <span class="expand-job-timeline-arrow arrow-closed"></span>
      {UIUtils.tooltipLink(<xml:group>Event Timeline</xml:group>, ToolTips.STAGE_TIMELINE)}
    </span> ++
    <div id="job-timeline" class="collapsed">
      {
      // 超过最大展示数量时显示提示信息
      if (MAX_TIMELINE_STAGES < stages.size) {
        <div>
          <strong>
            Only the most recent {MAX_TIMELINE_STAGES} submitted/completed stages
            (of {stages.size} total) are shown.
          </strong>
        </div>
      } else {
        Seq.empty
      }
      }
      {
      // 超过最大展示数量时显示提示信息
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
      <div class="control-panel">
        <div id="job-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
    </div> ++
    // 注入JavaScript代码渲染时间线
    <script type="text/javascript" nonce={CspNonce.get}>
      {Unparsed(s"drawJobTimeline(${groupJsonArrayAsStr}, ${eventArrayAsStr}, " +
      s"${appStartTime}, ${UIUtils.getTimeZoneOffset()});")}
    </script>
  }

  /**
   * 渲染Job详情页面的主入口，处理HTTP请求生成完整HTML页面
   * @param request HTTP请求对象
   * @return 渲染完成的完整HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val jobId = parameterId.toInt
    // 查询Job数据和关联的SQL执行ID
    val (jobData, sqlExecutionId) = store.asOption(store.jobWithAssociatedSql(jobId)).getOrElse {
      val content =
        <div id="no-info">
          <p>No information to display for job {jobId}</p>
        </div>
      return UIUtils.headerSparkPage(
        request, s"Details for Job $jobId", content, parent)
    }

    val isComplete = jobData.status != JobExecutionStatus.RUNNING
    val stages = jobData.stageIds.map { stageId =>
      // This could be empty if the listener hasn't received information about the
      // stage or if the stage information has been garbage collected
      store.asOption(store.lastStageAttempt(stageId)).getOrElse {
        // 找不到Stage信息时构造占位的空StageData
        new v1.StageData(
          status = v1.StageStatus.PENDING,
          stageId = stageId,
          attemptId = 0,
          numTasks = 0,
          numActiveTasks = 0,
          numCompleteTasks = 0,
          numFailedTasks = 0,
          numKilledTasks = 0,
          numCompletedIndices = 0,

          submissionTime = None,
          firstTaskLaunchedTime = None,
          completionTime = None,
          failureReason = None,

          executorDeserializeTime = 0L,
          executorDeserializeCpuTime = 0L,
          executorRunTime = 0L,
          executorCpuTime = 0L,
          resultSize = 0L,
          jvmGcTime = 0L,
          resultSerializationTime = 0L,
          memoryBytesSpilled = 0L,
          diskBytesSpilled = 0L,
          peakExecutionMemory = 0L,
          inputBytes = 0L,
          inputRecords = 0L,
          outputBytes = 0L,
          outputRecords = 0L,
          shuffleRemoteBlocksFetched = 0L,
          shuffleLocalBlocksFetched = 0L,
          shuffleFetchWaitTime = 0L,
          shuffleRemoteBytesRead = 0L,
          shuffleRemoteBytesReadToDisk = 0L,
          shuffleLocalBytesRead = 0L,
          shuffleReadBytes = 0L,
          shuffleReadRecords = 0L,
          shuffleCorruptMergedBlockChunks = 0L,
          shuffleMergedFetchFallbackCount = 0L,
          shuffleMergedRemoteBlocksFetched = 0L,
          shuffleMergedLocalBlocksFetched = 0L,
          shuffleMergedRemoteChunksFetched = 0L,
          shuffleMergedLocalChunksFetched = 0L,
          shuffleMergedRemoteBytesRead = 0L,
          shuffleMergedLocalBytesRead = 0L,
          shuffleRemoteReqsDuration = 0L,
          shuffleMergedRemoteReqsDuration = 0L,
          shuffleWriteBytes = 0L,
          shuffleWriteTime = 0L,
          shuffleWriteRecords = 0L,

          name = "Unknown",
          description = None,
          details = "Unknown",
          schedulingPool = null,

          rddIds = Nil,
          accumulatorUpdates = Nil,
          tasks = None,
          executorSummary = None,
          speculationSummary = None,
          killedTasksSummary = Map(),
          ResourceProfile.UNKNOWN_RESOURCE_PROFILE_ID,
          peakExecutorMetrics = None,
          taskMetricsDistributions = None,
          executorMetricsDistributions = None,
          isShufflePushEnabled = false,
          shuffleMergersCount = 0)
      }
    }

    // 按状态分组Stage
    val activeStages = Buffer[v1.StageData]()
    val completedStages = Buffer[v1.StageData]()
    // If the job is completed, then any pending stages are displayed as "skipped":
    val pendingOrSkippedStages = Buffer[v1.StageData]()
    val failedStages = Buffer[v1.StageData]()
    for (stage <- stages) {
      if (stage.submissionTime.isEmpty) {
        pendingOrSkippedStages += stage
      } else if (stage.completionTime.isDefined) {
        if (stage.status == v1.StageStatus.FAILED) {
          failedStages += stage
        } else {
          completedStages += stage
        }
      } else {
        activeStages += stage
      }
    }

    val basePath = "jobs/job"

    // Job已完成时，未执行的Pending Stage显示为跳过
    val pendingOrSkippedTableId =
      if (isComplete) {
        "skipped"
      } else {
        "pending"
      }

    // 为不同状态的Stage分别生成表格
    val activeStagesTable =
      new StageTableBase(store, request, activeStages.toSeq, "active", "activeStage",
        parent.basePath, basePath, parent.isFairScheduler,
        killEnabled = parent.killEnabled, isFailedStage = false)
    val pendingOrSkippedStagesTable =
      new StageTableBase(store, request, pendingOrSkippedStages.toSeq, pendingOrSkipped
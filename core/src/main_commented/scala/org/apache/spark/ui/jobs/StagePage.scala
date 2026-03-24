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

import java.util.Date

import scala.collection.mutable.HashSet
import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.status._
import org.apache.spark.status.api.v1._
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils.formatImportJavaScript
import org.apache.spark.util.Utils

/**
 * Spark UI 单个Stage详情页面，展示指定Stage的统计信息和任务列表
 * 属于StagesTab的子页面，提供Stage详细数据可视化和任务明细查询
 */
private[ui] class StagePage(parent: StagesTab, store: AppStatusStore) extends WebUIPage("stage") {
  import ApiHelper._

  // 是否启用任务时间线可视化功能，从配置读取
  private val TIMELINE_ENABLED = parent.conf.get(UI_TIMELINE_ENABLED)

  // 时间线图例的SVG模板，定义各阶段耗时色块的说明
  private val TIMELINE_LEGEND = {
    <div class="legend-area">
      <svg>
        {
          val legendPairs = List(("scheduler-delay-proportion", "Scheduler Delay"),
            ("deserialization-time-proportion", "Task Deserialization Time"),
            ("shuffle-read-time-proportion", "Shuffle Read Time"),
            ("executor-runtime-proportion", "Executor Computing Time"),
            ("shuffle-write-time-proportion", "Shuffle Write Time"),
            ("serialization-time-proportion", "Result Serialization Time"),
            ("getting-result-time-proportion", "Getting Result Time"))

          legendPairs.zipWithIndex.map {
            case ((classAttr, name), index) =>
              <rect x={s"${5 + (index / 3) * 210}px"} y={s"${10 + (index % 3) * 15}px"}
                width="10px" height="10px" class={classAttr}></rect>
                <text x={s"${25 + (index / 3) * 210}px"}
                  y={s"${20 + (index % 3) * 15}px"}>{name}</text>
          }
        }
      </svg>
    </div>
  }

  // TODO: We should consider increasing the number of this parameter over time
  // if we find that it's okay.
  // 时间线最多展示的任务数量，从配置读取，避免渲染过多任务导致页面卡顿
  private val MAX_TIMELINE_TASKS = parent.conf.get(UI_TIMELINE_TASKS_MAXIMUM)

  /**
   * 生成本地性分布统计的字符串，将TaskLocality计数转换为可读文本
   * @param localitySummary 不同本地性级别对应的任务数量Map
   * @return 格式化后的本地性分布描述字符串
   */
  private def getLocalitySummaryString(localitySummary: Map[String, Long]): String = {
    val names = Map(
      TaskLocality.PROCESS_LOCAL.toString() -> "Process local",
      TaskLocality.NODE_LOCAL.toString() -> "Node local",
      TaskLocality.RACK_LOCAL.toString() -> "Rack local",
      TaskLocality.ANY.toString() -> "Any")
    val localityNamesAndCounts = names.flatMap { case (key, name) =>
      localitySummary.get(key).map { count =>
        s"$name: $count"
      }
    }.toSeq
    localityNamesAndCounts.sorted.mkString("; ")
  }

  /**
   * 渲染Stage详情页面，处理HTTP请求生成HTML内容
   * @param request HTTP servlet请求对象
   * @return 生成的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val parameterAttempt = request.getParameter("attempt")
    require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")

    val parameterTaskSortColumn = request.getParameter("task.sort")
    val parameterTaskSortDesc = request.getParameter("task.desc")
    val parameterTaskPageSize = request.getParameter("task.pageSize")

    val eventTimelineParameterTaskPage = request.getParameter("task.eventTimelinePageNumber")
    val eventTimelineParameterTaskPageSize = request.getParameter("task.eventTimelinePageSize")
    var eventTimelineTaskPage = Option(eventTimelineParameterTaskPage).map(_.toInt).getOrElse(1)
    var eventTimelineTaskPageSize = Option(
      eventTimelineParameterTaskPageSize).map(_.toInt).getOrElse(100)

    // 解析任务排序参数，默认按任务索引升序排序
    val taskSortColumn = Option(parameterTaskSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse("Index")
    val taskSortDesc = Option(parameterTaskSortDesc).map(_.toBoolean).getOrElse(false)
    val taskPageSize = Option(parameterTaskPageSize).map(_.toInt).getOrElse(100)
    val stageId = parameterId.toInt
    val stageAttemptId = parameterAttempt.toInt

    val stageHeader = s"Details for Stage $stageId (Attempt $stageAttemptId)"
    // 从存储获取Stage尝试信息，如果不存在返回空信息页面
    val (stageData, stageJobIds) = parent.store
      .asOption(parent.store.stageAttempt(stageId, stageAttemptId, details = false))
      .getOrElse {
        val content =
          <div id="no-info">
            <p>No information to display for Stage {stageId} (Attempt {stageAttemptId})</p>
          </div>
        return UIUtils.headerSparkPage(request, stageHeader, content, parent)
      }

    // 获取本阶段任务数据本地性分布统计
    val localitySummary = store.localitySummary(stageData.stageId, stageData.attemptId)

    // 计算本阶段总任务数
    val totalTasks = stageData.numActiveTasks + stageData.numCompleteTasks +
      stageData.numFailedTasks + stageData.numKilledTasks
    // 如果还没有任务启动，返回空内容页面
    if (totalTasks == 0) {
      val content =
        <div>
          <h4>Summary Metrics</h4> No tasks have started yet
          <h4>Tasks</h4> No tasks have started yet
        </div>
      return UIUtils.headerSparkPage(request, stageHeader, content, parent)
    }

    // 校验并修正时间线分页参数，避免越界
    if (eventTimelineTaskPageSize < 1 || eventTimelineTaskPageSize > totalTasks) {
      eventTimelineTaskPageSize = totalTasks
    }
    val eventTimelineTotalPages =
      (totalTasks + eventTimelineTaskPageSize - 1) / eventTimelineTaskPageSize
    if (eventTimelineTaskPage < 1 || eventTimelineTaskPage > eventTimelineTotalPages) {
      eventTimelineTaskPage = 1
    }

    // 生成Stage摘要信息HTML
    val summary =
      <div>
        <ul class="list-unstyled">
          <li>
            <Strong>Submitted:</Strong>
            {StageDataUtil.getFormattedSubmissionTime(stageData)}
          </li>
          <li>
            <strong>Duration</strong>
            {StageDataUtil.getFormattedDuration(stageData)}
          </li>
          <li>
            <strong>Resource Profile Id: </strong>
            {stageData.resourceProfileId}
          </li>
          <li>
            <strong>Total Time Across All Tasks: </strong>
            {UIUtils.formatDuration(stageData.executorRunTime)}
          </li>
          <li>
            <strong>Locality Level Summary: </strong>
            {getLocalitySummaryString(localitySummary)}
          </li>
          {if (hasInput(stageData)) {
            <li>
              <strong>Input Size / Records: </strong>
              {s"${Utils.bytesToString(stageData.inputBytes)} / ${stageData.inputRecords}"}
            </li>
          }}
          {if (hasOutput(stageData)) {
            <li>
              <strong>Output Size / Records: </strong>
              {s"${Utils.bytesToString(stageData.outputBytes)} / ${stageData.outputRecords}"}
            </li>
          }}
          {if (hasShuffleRead(stageData)) {
            <li>
              <strong>Shuffle Read Size / Records: </strong>
              {s"${Utils.bytesToString(stageData.shuffleReadBytes)} / " +
               s"${stageData.shuffleReadRecords}"}
            </li>
          }}
          {if (hasShuffleWrite(stageData)) {
            <li>
              <strong>Shuffle Write Size / Records: </strong>
               {s"${Utils.bytesToString(stageData.shuffleWriteBytes)} / " +
               s"${stageData.shuffleWriteRecords}"}
            </li>
          }}
          {if (hasBytesSpilled(stageData)) {
            <li>
              <strong>Spill (Memory): </strong>
              {Utils.bytesToString(stageData.memoryBytesSpilled)}
            </li>
            <li>
              <strong>Spill (Disk): </strong>
              {Utils.bytesToString(stageData.diskBytesSpilled)}
            </li>
          }}
          {if (!stageJobIds.isEmpty) {
            <li>
              <strong>Associated Job Ids: </strong>
              {stageJobIds.sorted.map { jobId =>
                val jobURL = "%s/jobs/job/?id=%s"
                  .format(UIUtils.prependBaseUri(request, parent.basePath), jobId)
                <a href={jobURL}>{jobId.toString}</a><span>&nbsp;</span>
              }}
            </li>
          }}
        </ul>
      </div>

    // 生成Stage DAG可视化图
    val stageGraph = parent.store.asOption(parent.store.operationGraphForStage(stageId))
    val dagViz = UIUtils.showDagVizForStage(stageId, stageGraph)

    val currentTime = System.currentTimeMillis()

    // 生成页面初始化JavaScript代码，配置线程转储功能开关
    val js =
      s"""
         |${formatImportJavaScript(request, "/static/stagepage.js", "setTaskThreadDumpEnabled")}
         |
         |setTaskThreadDumpEnabled(${parent.threadDumpEnabled});
         |""".stripMargin
    // 组装页面所有内容区块
    val content =
      summary ++
      dagViz ++ <div id="showAdditionalMetrics"></div> ++
      makeTimeline(
        // 只分页获取当前页需要展示的任务数据
        () => {
          val from = (eventTimelineTaskPage - 1) * eventTimelineTaskPageSize
          val dataSize = store.taskCount(stageData.stageId, stageData.attemptId).toInt
          val to = dataSize.min(eventTimelineTaskPage * eventTimelineTaskPageSize)
          val sliceData = store.taskList(stageData.stageId, stageData.attemptId, from, to - from,
            indexName(taskSortColumn), !taskSortDesc)
          sliceData
        }, currentTime,
        eventTimelineTaskPage, eventTimelineTaskPageSize, eventTimelineTotalPages, stageId,
        stageAttemptId, totalTasks) ++
        <div id="parent-container">
          <script type="module" src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script>
          <script type="module"
                  src={UIUtils.prependBaseUri(request, "/static/stagepage.js")}></script>
          <script type="module" nonce={CspNonce.get}>{Unparsed(js)}</script>
        </div>
    // 包装为标准Spark页面结构返回
    UIUtils.headerSparkPage(request, stageHeader, content, parent, showVisualization = true,
      useDataTables = true)

  }

  /**
   * 生成任务事件时间线可视化区域，展示不同Executor上任务执行时间分布
   * @param tasksFunc 获取当前页任务数据的函数
   * @param currentTime 当前时间戳，用于处理未完成任务
   * @param page 当前页码
   * @param pageSize 每页任务数
   * @param totalPages 总页数
   * @param stageId Stage ID
   * @param stageAttemptId Stage尝试ID
   * @param totalTasks 总任务数
   * @return 时间线区域HTML节点序列
   */
  private def makeTimeline(
      tasksFunc: () => Seq[TaskData],
      currentTime: Long,
      page: Int,
      pageSize: Int,
      totalPages: Int,
      stageId: Int,
      stageAttemptId: Int,
      totalTasks: Int): Seq[Node] = {

    // 如果时间线功能关闭，返回空
    if (!TIMELINE_ENABLED) return Seq.empty[Node]

    val tasks = tasksFunc()

    // 收集涉及的Executor集合，计算所有任务的时间范围
    val executorsSet = new HashSet[(String, String)]
    var minLaunchTime = Long.MaxValue
    var maxFinishTime = Long.MinValue

    // 遍历任务生成时间线所需的JSON数据结构
    val executorsArrayStr =
      tasks.sortBy(-_.launchTime.getTime()).take(MAX_TIMELINE_TASKS).map { taskInfo =>
        val executorId = taskInfo.executorId
        val host = taskInfo.host
        executorsSet += ((executorId, host))

        val launchTime = taskInfo.launchTime.getTime()
        val finishTime = taskInfo.duration.map(taskInfo.launchTime.getTime() + _)
          .getOrElse(currentTime)
        val totalExecutionTime = finishTime - launchTime
        // 更新全局时间范围边界
        minLaunchTime = launchTime.min(minLaunchTime)
        maxFinishTime = finishTime.max(maxFinishTime)

        // 计算各阶段耗时占总任务执行时间的百分比
        def toProportion(time: Long) = time.toDouble / totalExecutionTime * 100

        val metricsOpt = taskInfo.taskMetrics
        val shuffleReadTime =
          metricsOpt.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(0L)
        val shuffleReadTimeProportion = toProportion(shuffleReadTime)
        val shuffleWriteTime =
          (metricsOpt.map(_.shuffleWriteMetrics.writeTime).getOrElse(0L) / 1e6).toLong
        val shuffleWriteTimeProportion = toProportion(shuffleWriteTime)

        val serializationTime = metricsOpt.map(_.resultSerializationTime).getOrElse(0L)
        val serializationTimeProportion = toProportion(serializationTime)
        val deserializationTime = metricsOpt.map(_.executorDeserializeTime).getOrElse(0L)
        val deserializationTimeProportion = toProportion(deserializationTime)
        val gettingResultTime = AppStatusUtils.gettingResultTime(taskInfo)
        val gettingResultTimeProportion = toProportion(gettingResultTime)
        val schedulerDelay = AppStatusUtils.schedulerDelay(taskInfo)
        val schedulerDelayProportion = toProportion(schedulerDelay)

        // 计算Executor实际计算时间占比
        val executorOverhead = serializationTime + deserializationTime
        val executorRunTime = if (taskInfo.duration.isDefined) {
          math.max(totalExecutionTime - executorOverhead - gettingResultTime - schedulerDelay, 0)
        } else {
          metricsOpt.map(_.executorRunTime).getOrElse(
            math.max(totalExecutionTime - executorOverhead - gettingResultTime - schedulerDelay, 0))
        }
        val executorComputingTime = executorRunTime - shuffleReadTime - shuffleWriteTime
        val executorComputingTimeProportion =
          math.max(100 - schedulerDelayProportion - shuffleReadTimeProportion -
            shuffleWriteTimeProportion - serializationTimeProportion -
            deserializationTimeProportion - gettingResultTimeProportion, 0)

        // 计算各阶段色块在时间条上的起始位置百分比
        val schedulerDelayProportionPos = 0
        val deserializationTimeProportionPos =
          schedulerDelayProportionPos + schedulerDelayProportion
        val shuffleReadTimeProportionPos =
          deserializationTimeProportionPos + deserializationTimeProportion
        val executorRuntimeProportionPos =
          shuffleReadTimeProportionPos + shuffleReadTimeProportion
        val shuffleWriteTimeProportionPos =
          executorRuntimeProportionPos + executorComputingTimeProportion
        val serialization
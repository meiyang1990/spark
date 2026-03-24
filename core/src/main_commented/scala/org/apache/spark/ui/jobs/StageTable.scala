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
import java.util.Date

import scala.xml._

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/**
 * Stage表格基础容器，负责初始化分页参数并渲染Stage表格组件
 * 用于Spark WebUI中展示Stage列表的基础容器类
 * @param store 应用状态存储，用于查询Stage和RDD信息
 * @param request HTTP请求对象，包含分页、排序等请求参数
 * @param stages 需要展示的Stage数据列表
 * @param tableHeaderID 表格表头ID，用于页面锚点跳转
 * @param stageTag 表格标识前缀，用于区分不同表格的请求参数
 * @param basePath UI基础路径
 * @param subPath UI子路径
 * @param isFairScheduler 是否使用公平调度器
 * @param killEnabled 是否允许终止Stage
 * @param isFailedStage 是否展示失败的Stage
 */
private[ui] class StageTableBase(
    store: AppStatusStore,
    request: HttpServletRequest,
    stages: Seq[v1.StageData],
    tableHeaderID: String,
    stageTag: String,
    basePath: String,
    subPath: String,
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    isFailedStage: Boolean) {

  // 获取当前页码，默认第一页
  val stagePage = Option(request.getParameter(stageTag + ".page")).map(_.toInt).getOrElse(1)

  // 当前时间，用于计算未完成Stage的运行时长
  val currentTime = System.currentTimeMillis()

  // 尝试生成分页表格的XML节点，捕获异常展示错误信息
  val toNodeSeq = try {
    new StagePagedTable(
      store,
      stages,
      tableHeaderID,
      stageTag,
      basePath,
      subPath,
      isFairScheduler,
      killEnabled,
      currentTime,
      isFailedStage,
      request
    ).table(stagePage)
  } catch {
    case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
      <div class="alert alert-danger">
        <p>Error while rendering stage table:</p>
        <pre>
          {Utils.exceptionString(e)}
        </pre>
      </div>
  }
}

/**
 * Stage表格行数据结构，预格式化所有展示信息用于分页和排序
 * 避免重复计算格式化内容，提升渲染效率
 */
private[ui] class StageTableRowData(
    val stage: v1.StageData,
    val option: Option[v1.StageData],
    val stageId: Int,
    val attemptId: Int,
    val schedulingPool: String,
    val descriptionOption: Option[String],
    val submissionTime: Date,
    val formattedSubmissionTime: String,
    val duration: Long,
    val formattedDuration: String,
    val inputRead: Long,
    val inputReadWithUnit: String,
    val outputWrite: Long,
    val outputWriteWithUnit: String,
    val shuffleRead: Long,
    val shuffleReadWithUnit: String,
    val shuffleWrite: Long,
    val shuffleWriteWithUnit: String)

/**
 * 分页Stage表格实现，负责渲染表格头、分页链接和每一行数据
 * 继承自通用PagedTable，实现Stage相关的展示逻辑
 */
private[ui] class StagePagedTable(
    store: AppStatusStore,
    stages: Seq[v1.StageData],
    tableHeaderId: String,
    stageTag: String,
    basePath: String,
    subPath: String,
    isFairScheduler: Boolean,
    killEnabled: Boolean,
    currentTime: Long,
    isFailedStage: Boolean,
    request: HttpServletRequest) extends PagedTable[StageTableRowData] {

  override def tableId: String = stageTag + "-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageSizeFormField: String = stageTag + ".pageSize"

  override def pageNumberFormField: String = stageTag + ".page"

  // 从请求中获取排序、分页参数，默认按Stage Id排序
  private val (sortColumn, desc, pageSize) = getTableParameters(request, stageTag, "Stage Id")

  // URL编码排序列名，用于分页链接参数
  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  // 拼接基础参数路径，保留其他表格的请求参数
  private val parameterPath = UIUtils.prependBaseUri(request, basePath) + s"/$subPath/?" +
    getParameterOtherTable(request, stageTag)

  // 初始化数据源，包含排序和分页逻辑
  override val dataSource = new StageDataSource(
    store,
    stages,
    currentTime,
    pageSize,
    sortColumn,
    desc
  )

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$stageTag.sort=$encodedSortColumn" +
      s"&$stageTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  override def goButtonFormPath: String =
    s"$parameterPath&$stageTag.sort=$encodedSortColumn&$stageTag.desc=$desc#$tableHeaderId"

  override def headers: Seq[Node] = {
    // 构建表头配置，每个元组包含(表头名称, 是否可排序, 提示信息)
    val stageHeadersAndCssClasses: Seq[(String, Boolean, Option[String])] =
      Seq(("Stage Id", true, None)) ++
      {if (isFairScheduler) {Seq(("Pool Name", true, None))} else Seq.empty} ++
      Seq(
        ("Description", true, None),
        ("Submitted", true, None),
        ("Duration", true, Some(ToolTips.DURATION)),
        ("Tasks: Succeeded/Total", false, None),
        ("Input", true, Some(ToolTips.INPUT)),
        ("Output", true, Some(ToolTips.OUTPUT)),
        ("Shuffle Read", true, Some(ToolTips.SHUFFLE_READ)),
        ("Shuffle Write", true, Some(ToolTips.SHUFFLE_WRITE))
      ) ++
      {if (isFailedStage) {Seq(("Failure Reason", false, None))} else Seq.empty}

    // 校验排序列是否合法
    isSortColumnValid(stageHeadersAndCssClasses, sortColumn)

    // 生成表头HTML
    headerRow(stageHeadersAndCssClasses, desc, pageSize, sortColumn, parameterPath,
      stageTag, tableHeaderId)
  }

  override def row(data: StageTableRowData): Seq[Node] = {
    <tr id={"stage-" + data.stageId + "-" + data.attemptId}>
      {rowContent(data)}
    </tr>
  }

  // 生成单行表格内容
  private def rowContent(data: StageTableRowData): Seq[Node] = {
    data.option match {
      case None => missingStageRow(data.stageId)
      case Some(stageData) =>
        val info = data.stage

        // 展示Stage Id，重试次数大于0时显示重试次数
        {if (data.attemptId > 0) {
          <td>{data.stageId} (retry {data.attemptId})</td>
        } else {
          <td>{data.stageId}</td>
        }} ++
        // 公平调度模式下展示调度池链接
        {if (isFairScheduler) {
          <td>
            <a href={"%s/stages/pool?poolname=%s"
              .format(UIUtils.prependBaseUri(request, basePath), data.schedulingPool)}>
              {data.schedulingPool}
            </a>
          </td>
        } else {
          Seq.empty
        }} ++
        // 描述信息列
        <td>{makeDescription(info, data.descriptionOption)}</td>
        // 提交时间列
        <td valign="middle">
          {data.formattedSubmissionTime}
        </td>
        // 运行时长列
        <td>{data.formattedDuration}</td>
        // 任务进度进度条
        <td class="progress-cell">
          {UIUtils.makeProgressBar(started = stageData.numActiveTasks,
          completed = stageData.numCompleteTasks, failed = stageData.numFailedTasks,
          skipped = 0, reasonToNumKilled = stageData.killedTasksSummary, total = info.numTasks)}
        </td>
        // 输入数据量
        <td>{data.inputReadWithUnit}</td>
        // 输出数据量
        <td>{data.outputWriteWithUnit}</td>
        // Shuffle读数据量
        <td>{data.shuffleReadWithUnit}</td>
        // Shuffle写数据量
        <td>{data.shuffleWriteWithUnit}</td> ++
        // 失败Stage展示失败原因列
        {
          if (isFailedStage) {
            UIUtils.errorMessageCell(info.failureReason.getOrElse(""))
          } else {
            Seq.empty
          }
        }
    }
  }

  // 生成Stage描述列内容，包含终止链接、名称链接和详情展开
  private def makeDescription(s: v1.StageData, descriptionOption: Option[String]): Seq[Node] = {
    val basePathUri = UIUtils.prependBaseUri(request, basePath)

    // 允许终止时生成终止链接
    val killLink = if (killEnabled) {
      // SPARK-6846 此处应为POST，但YARN代理不支持POST，故使用GET
      val killLinkUri = s"$basePathUri/stages/stage/kill/?id=${s.stageId}"
      <a href={killLinkUri}
         data-kill-message={s"Are you sure you want to kill stage ${s.stageId} ?"}
         class="kill-link float-end">(kill)</a>
    } else {
      Seq.empty
    }

    // Stage详情页链接
    val nameLinkUri = s"$basePathUri/stages/stage/?id=${s.stageId}&attempt=${s.attemptId}"
    val nameLink = <a href={nameLinkUri} class="name-link">{s.name}</a>

    // 过滤出当前Stage包含的RDD信息
    val cachedRddInfos = store.rddList().filter { rdd => s.rddIds.contains(rdd.id) }
    // 生成详情展开区域，包含RDD链接和Stage详情
    val details = if (s.details != null && s.details.nonEmpty) {
      <span data-toggle-details=".stage-details"
            class="expand-details float-end">
        +details
      </span> ++
      <div class="stage-details collapsed">
        {if (cachedRddInfos.nonEmpty) {
          Text("RDD: ") ++
          cachedRddInfos.map { i =>
            <a href={s"$basePathUri/storage/rdd/?id=${i.id}"}>{i.name}</a>
          }
        }}
        <pre>{s.details}</pre>
      </div>
    }

    // 拼接所有描述内容
    val stageDesc = descriptionOption.map(UIUtils.makeDescription(_, basePathUri))
    <div>{stageDesc.getOrElse("")} {killLink} {nameLink} {details}</div>
  }

  // 生成缺失Stage的空行，展示无数据提示
  protected def missingStageRow(stageId: Int): Seq[Node] = {
    <td>{stageId}</td> ++
    {if (isFairScheduler) {<td>-</td>} else Seq.empty} ++
    <td>No data available for this stage</td> ++ // Description
    <td></td> ++ // Submitted
    <td></td> ++ // Duration
    <td></td> ++ // Tasks: Succeeded/Total
    <td></td> ++ // Input
    <td></td> ++ // Output
    <td></td> ++ // Shuffle Read
    <td></td> // Shuffle Write
  }
}

/**
 * Stage分页数据源，负责将原始Stage数据转换为行数据、排序和分页切片
 * 预计算所有行数据，提升分页渲染效率
 */
private[ui] class StageDataSource(
    store: AppStatusStore,
    stages: Seq[v1.StageData],
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[StageTableRowData](pageSize) {
  // 转换所有Stage为行数据并按排序规则排序
  private val data = stages.map(stageRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[StageTableRowData] = data.slice(from, to)

  // 将单个StageData转换为渲染用的行数据，预格式化所有展示内容
  private def stageRow(stageData: v1.StageData): StageTableRowData = {
    val formattedSubmissionTime = stageData.submissionTime match {
      case Some(t) => UIUtils.formatDate(t)
      case None => "Unknown"
    }
    val finishTime = stageData.completionTime.map(_.getTime()).getOrElse(currentTime)

    // 计算运行时长：从第一个任务启动开始算，不包含Stage提交后等待调度的时间（SPARK-10930）
    val duration = stageData.firstTaskLaunchedTime.map { date =>
      val time = date.getTime()
      if (finishTime > time) {
        finishTime - time
      } else {
        currentTime - time
      }
    }
    val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")

    val inputRead = stageData.inputBytes
    val inputReadWithUnit = if (inputRead > 0) Utils.bytesToString(inputRead) else ""
    val outputWrite = stageData.outputBytes
    val outputWriteWithUnit = if (outputWrite > 0) Utils.bytesToString(outputWrite) else ""
    val shuffleRead = stageData.shuffleReadBytes
    val shuffleReadWithUnit = if (shuffleRead > 0) Utils.bytesToString(shuffleRead) else ""
    val shuffleWrite = stageData.shuffleWriteBytes
    val shuffleWriteWithUnit = if (shuffleWrite > 0) Utils.bytesToString(shuffleWrite) else ""

    new StageTableRowData(
      stageData,
      Some(stageData),
      stageData.stageId,
      stageData.attemptId,
      stageData.schedulingPool,
      stageData.description,
      stageData.submissionTime.getOrElse(new Date(0)),
      formattedSubmissionTime,
      duration.getOrElse(-1),
      formattedDuration,
      inputRead,
      inputReadWithUnit,
      outputWrite,
      outputWriteWithUnit,
      shuffleRead,
      shuffleReadWithUnit,
      shuffleWrite,
      shuffleWriteWithUnit
    )
  }

  /**
   * 根据排序列和排序方向生成对应的排序器
   * @param sortColumn 排序列名
   * @param desc 是否降序
   * @return 排序器
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[StageTableRowData] = {
    val ordering: Ordering[StageTableRowData] = sortColumn match {
      case "Stage Id" => Ordering.by(_.stageId)
      case "Pool Name" => Ordering.by(_.schedulingPool)
      case "Description" => Ordering.by(x => (x.descriptionOption, x.stage.name))
      case "Submitted" => Ordering.by(_.submissionTime)
      case "Duration" => Ordering.by(_.duration)
      case "Input" => Ordering.by(_.inputRead)
      case "Output" => Ordering.by(_.outputWrite)
      case "Shuffle Read" => Ordering.by(_.shuffleRead)
      case "Shuffle Write" => Ordering.by(_.shuffleWrite)
      case "Tasks: Succeeded/Total" =>
        throw new IllegalArgumentException(s"Unsortable column: $sortColumn")
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
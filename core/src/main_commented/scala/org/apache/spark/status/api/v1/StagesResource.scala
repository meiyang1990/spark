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

/**
 * Stage 状态查询 REST API 资源类，为Spark WebUI提供Stage和任务相关的状态数据查询接口
 * 属于Spark核心模块的监控API v1版本，提供Stage列表、Stage详情、任务列表、任务统计等查询能力
 */
package org.apache.spark.status.api.v1

import java.util.{HashMap, List => JList, Locale}

import scala.jdk.CollectionConverters._

import jakarta.ws.rs.{NotFoundException => _, _}
import jakarta.ws.rs.core.{Context, MediaType, MultivaluedMap, UriInfo}

import org.apache.spark.status.api.v1.TaskStatus._
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.jobs.ApiHelper._
import org.apache.spark.util.Utils

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class StagesResource extends BaseAppResource {

  /**
   * 获取按条件过滤的Stage列表
   * @param statuses 按Stage状态过滤列表
   * @param details 是否返回详细信息
   * @param withSummaries 是否返回任务指标汇总
   * @param quantileString 分位数计算点字符串
   * @param taskStatus 按任务状态过滤列表
   * @return 符合条件的Stage数据列表
   */
  @GET
  def stageList(
      @QueryParam("status") statuses: JList[StageStatus],
      @QueryParam("details") @DefaultValue("false") details: Boolean,
      @QueryParam("withSummaries") @DefaultValue("false") withSummaries: Boolean,
      @QueryParam("quantiles") @DefaultValue("0.0,0.25,0.5,0.75,1.0") quantileString: String,
      @QueryParam("taskStatus") taskStatus: JList[TaskStatus]): Seq[StageData] = {
    withUI {
      val quantiles = parseQuantileString(quantileString)
      ui => {
        ui.store.stageList(statuses, details, withSummaries, quantiles, taskStatus)
          .filter { stage =>
            if (details && taskStatus.asScala.nonEmpty) {
              // 根据任务状态匹配Stage
              taskStatus.asScala.exists {
                case FAILED => stage.numFailedTasks > 0
                case KILLED => stage.numKilledTasks > 0
                case RUNNING => stage.numActiveTasks > 0
                case SUCCESS => stage.numCompleteTasks > 0
                case UNKNOWN => stage.numTasks - stage.numFailedTasks - stage.numKilledTasks -
                  stage.numActiveTasks - stage.numCompleteTasks > 0
              }
            } else {
              true
            }
          }
      }
    }
  }

  /**
   * 获取指定Stage所有尝试的Stage数据
   * @param stageId Stage ID
   * @param details 是否返回详细信息
   * @param taskStatus 按任务状态过滤列表
   * @param withSummaries 是否返回任务指标汇总
   * @param quantileString 分位数计算点字符串
   * @return 指定Stage的所有尝试数据列表，不存在则返回404
   */
  @GET
  @Path("{stageId: \\d+}")
  def stageData(
      @PathParam("stageId") stageId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean,
      @QueryParam("taskStatus") taskStatus: JList[TaskStatus],
      @QueryParam("withSummaries") @DefaultValue("false") withSummaries: Boolean,
      @QueryParam("quantiles") @DefaultValue("0.0,0.25,0.5,0.75,1.0") quantileString: String):
  Seq[StageData] = {
    withUI { ui =>
      val quantiles = parseQuantileString(quantileString)
      val ret = ui.store.stageData(stageId, details = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = quantiles)
      if (ret.nonEmpty) {
        ret
      } else {
        throw new NotFoundException(s"unknown stage: $stageId")
      }
    }
  }

  /**
   * 获取指定Stage指定尝试的详细数据
   * @param stageId Stage ID
   * @param stageAttemptId Stage尝试ID
   * @param details 是否返回详细信息
   * @param taskStatus 按任务状态过滤列表
   * @param withSummaries 是否返回任务指标汇总
   * @param quantileString 分位数计算点字符串
   * @return 指定Stage尝试的详细数据，不存在则返回404
   */
  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}")
  def oneAttemptData(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @QueryParam("details") @DefaultValue("true") details: Boolean,
      @QueryParam("taskStatus") taskStatus: JList[TaskStatus],
      @QueryParam("withSummaries") @DefaultValue("false") withSummaries: Boolean,
      @QueryParam("quantiles") @DefaultValue("0.0,0.25,0.5,0.75,1.0") quantileString: String):
  StageData = withUI { ui =>
    try {
      val quantiles = parseQuantileString(quantileString)
      ui.store.stageAttempt(stageId, stageAttemptId, details = details, taskStatus = taskStatus,
        withSummaries = withSummaries, unsortedQuantiles = quantiles)._1
    } catch {
      case _: NoSuchElementException =>
        // 根据是否存在该Stage的其他尝试返回不同错误信息
        val all = ui.store.stageData(stageId, false, taskStatus)
        val msg = if (all.nonEmpty) {
          val ids = all.map(_.attemptId)
          s"unknown attempt for stage $stageId.  Found attempts: [${ids.mkString(",")}]"
        } else {
          s"unknown stage: $stageId"
        }
        throw new NotFoundException(msg)
    }
  }

  /**
   * 获取指定Stage尝试的任务指标分位数分布
   * @param stageId Stage ID
   * @param stageAttemptId Stage尝试ID
   * @param quantileString 分位数计算点字符串
   * @return 任务指标分位数分布，没有指标数据则返回404
   */
  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskSummary")
  def taskSummary(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0.05,0.25,0.5,0.75,0.95") @QueryParam("quantiles") quantileString: String)
  : TaskMetricDistributions = withUI { ui =>
    val quantiles = parseQuantileString(quantileString)
    ui.store.taskSummary(stageId, stageAttemptId, quantiles).getOrElse(
      throw new NotFoundException(s"No tasks reported metrics for $stageId / $stageAttemptId yet."))
  }

  /**
   * 获取指定Stage尝试分页后的任务列表
   * @param stageId Stage ID
   * @param stageAttemptId Stage尝试ID
   * @param offset 分页起始偏移
   * @param length 分页每页大小
   * @param sortBy 排序字段
   * @param statuses 按任务状态过滤列表
   * @return 分页后的任务数据列表
   */
  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskList")
  def taskList(
      @PathParam("stageId") stageId: Int,
      @PathParam("stageAttemptId") stageAttemptId: Int,
      @DefaultValue("0") @QueryParam("offset") offset: Int,
      @DefaultValue("20") @QueryParam("length") length: Int,
      @DefaultValue("ID") @QueryParam("sortBy") sortBy: TaskSorting,
      @QueryParam("status") statuses: JList[TaskStatus]): Seq[TaskData] = {
    withUI(_.store.taskList(stageId, stageAttemptId, offset, length, sortBy, statuses))
  }

  // This api needs to stay formatted exactly as it is below, since, it is being used by the
  // datatables for the stages page.
  /**
   * 为前端Stage页面DataTables提供服务端分页、搜索、排序的任务表格数据
   * 保持返回格式兼容DataTables服务端处理协议
   * @param stageId Stage ID
   * @param stageAttemptId Stage尝试ID
   * @param details 是否返回详细信息
   * @param uriInfo JAX-RS URI信息，包含所有查询参数
   * @return 符合DataTables协议要求的JSON数据结构
   */
  @GET
  @Path("{stageId: \\d+}/{stageAttemptId: \\d+}/taskTable")
  def taskTable(
    @PathParam("stageId") stageId: Int,
    @PathParam("stageAttemptId") stageAttemptId: Int,
    @QueryParam("details") @DefaultValue("true") details: Boolean,
    @Context uriInfo: UriInfo):
  HashMap[String, Object] = {
    withUI { ui =>
      // 解码两次URL参数避免重复编码问题
      val uriQueryParameters = UIUtils.decodeURLParameter(uriInfo.getQueryParameters(true))
      val totalRecords = uriQueryParameters.getFirst("numTasks")
      var isSearch = false
      var searchValue: String = null
      var filteredRecords = totalRecords
      // The datatables client API sends a list of query parameters to the server which contain
      // information like the columns to be sorted, search value typed by the user in the search
      // box, pagination index etc. For more information on these query parameters,
      // refer https://datatables.net/manual/server-side.
      if (uriQueryParameters.getFirst("search[value]") != null &&
        uriQueryParameters.getFirst("search[value]").length > 0) {
        isSearch = true
        searchValue = uriQueryParameters.getFirst("search[value]")
      }
      // 执行分页获取待展示任务数据
      val _tasksToShow: Seq[TaskData] = doPagination(uriQueryParameters, stageId, stageAttemptId,
        isSearch, totalRecords.toInt)
      val ret = new HashMap[String, Object]()
      if (_tasksToShow.nonEmpty) {
        // 根据用户搜索关键词执行服务端搜索
        if (isSearch) {
          val filteredTaskList = filterTaskList(_tasksToShow, searchValue)
          filteredRecords = filteredTaskList.length.toString
          if (filteredTaskList.length > 0) {
            val pageStartIndex = uriQueryParameters.getFirst("start").toInt
            val pageLength = uriQueryParameters.getFirst("length").toInt
            // 对搜索结果再次分页返回
            ret.put("aaData", filteredTaskList.slice(
              pageStartIndex, pageStartIndex + pageLength))
          } else {
            ret.put("aaData", filteredTaskList)
          }
        } else {
          // 无搜索直接返回分页结果
          ret.put("aaData", _tasksToShow)
        }
      } else {
        ret.put("aaData", _tasksToShow)
      }
      // 填充DataTables要求的总记录数和过滤后记录数
      ret.put("recordsTotal", totalRecords)
      ret.put("recordsFiltered", filteredRecords)
      ret
    }
  }

  /**
   * 服务端分页处理，根据查询参数解析排序和分页信息并获取对应任务数据
   * 搜索场景需要获取所有行才能进行全表搜索，非搜索场景仅获取当前页数据
   * @param queryParameters DataTables传递的查询参数
   * @param stageId Stage ID
   * @param stageAttemptId Stage尝试ID
   * @param isSearch 是否为搜索场景
   * @param totalRecords 总任务数
   * @return 分页后的任务数据列表
   */
  // Performs pagination on the server side
  def doPagination(queryParameters: MultivaluedMap[String, String], stageId: Int,
    stageAttemptId: Int, isSearch: Boolean, totalRecords: Int): Seq[TaskData] = {
    var columnNameToSort = queryParameters.getFirst("columnNameToSort")
    // 日志列不支持排序，默认使用索引排序
    if (columnNameToSort.equalsIgnoreCase("Logs")) {
      columnNameToSort = "Index"
    }
    val isAscendingStr = queryParameters.getFirst("order[0][dir]")
    var pageStartIndex = 0
    var pageLength = totalRecords
    // We fetch only the desired rows upto the specified page length for all cases except when a
    // search query is present, in that case, we need to fetch all the rows to perform the search
    // on the entire table
    if (!isSearch) {
      pageStartIndex = queryParameters.getFirst("start").toInt
      pageLength = queryParameters.getFirst("length").toInt
    }
    withUI(_.store.taskList(stageId, stageAttemptId, pageStartIndex, pageLength,
      indexName(columnNameToSort), "asc".equalsIgnoreCase(isAscendingStr)))
  }

  /**
   * 根据搜索关键词过滤任务列表，匹配任务的各个字段和指标
   * @param taskDataList 待过滤的任务数据列表
   * @param searchValue 用户输入的搜索关键词
   * @return 匹配搜索关键词的任务列表
   */
  // Filters task list based on search parameter
  def filterTaskList(
    taskDataList: Seq[TaskData],
    searchValue: String): Seq[TaskData] = {
    val defaultOptionString: String = "d"
    val searchValueLowerCase = searchValue.toLowerCase(Locale.ROOT)
    // 判断任意对象是否包含搜索关键词（小写匹配）
    val containsValue = (taskDataParams: Any) => taskDataParams.toString.toLowerCase(
      Locale.ROOT).contains(searchValueLowerCase)
    // 判断任务指标中是否包含搜索关键词
    val taskMetricsContainsValue = (task: TaskData) => task.taskMetrics match {
      case None => false
      case Some(metrics) =>
        (containsValue(UIUtils.formatDuration(task.taskMetrics.get.executorDeserializeTime))
        || containsValue(UIUtils.formatDuration(task.taskMetrics.get.executorRunTime))
        || containsValue(UIUtils.formatDuration(task.taskMetrics.get.jvmGcTime))
        || containsValue(UIUtils.formatDuration(task.taskMetrics.get.resultSerializationTime))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.memoryBytesSpilled))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.diskBytesSpilled))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.peakExecutionMemory))
        || containsValue(Utils.bytesToString(task.taskMetrics.get.inputMetrics.bytesRead))
        || containsValue(task.taskMetrics.get.inputMetrics.recordsRead)
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.outputMetrics.bytesWritten))
        || containsValue(task.taskMetrics.get.outputMetrics.recordsWritten)
        || containsValue(UIUtils.formatDuration(
          task.taskMetrics.get.shuffleReadMetrics.fetchWaitTime))
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead))
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.shuffleReadMetrics.localBytesRead +
          task.taskMetrics.get.shuffleReadMetrics.remoteBytesRead))
        || containsValue(task.taskMetrics.get.shuffleReadMetrics.recordsRead)
        || containsValue(Utils.bytesToString(
          task.taskMetrics.get.shuffleWriteMetrics.bytesWritten))
        || containsValue(task.taskMetrics.get.shuffleWriteMetrics.recordsWritten)
        || containsValue(UIUtils.formatDuration(
          task.taskMetrics.get.shuffleWriteMetrics.writeTime / 1000000)))
    }
    // 过滤匹配任意字段或指标的任务
    val filteredTaskDataSequence: Seq[TaskData] = taskDataList.filter(f =>
      (containsValue(f.taskId) || containsValue(f.index) || containsValue(f.attempt)
        || containsValue(UIUtils.formatDate(f.launchTime))
        || containsValue(f.resultFetchStart.getOrElse(defaultOptionString))
        || containsValue(f.executorId) || containsValue(f.host) || containsValue(f.status)
        || containsValue(f.taskLocality) || containsValue(f.speculative)
        || containsValue(f.errorMessage.getOrElse(defaultOptionString))
        || taskMetricsContainsValue(f)
        || containsValue(UIUtils.formatDuration(f.schedulerDelay
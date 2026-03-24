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
 * Spark History Server REST API v1 应用列表查询资源
 * 提供按条件过滤查询已完成和运行中应用列表的能力
 */
package org.apache.spark.status.api.v1

import java.util.{List => JList}

import jakarta.ws.rs.{DefaultValue, GET, Produces, QueryParam}
import jakarta.ws.rs.core.MediaType

/**
 * 应用列表查询REST资源类
 * 负责处理应用列表的条件查询请求，根据状态、时间范围过滤返回符合条件的应用信息
 */
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ApplicationListResource extends ApiRequestContext {

  /**
   * 查询符合过滤条件的Spark应用列表
   * @param status 需要返回的应用状态列表（RUNNING/COMPLETED），空列表返回全部
   * @param minDate 应用开始时间最小日期过滤条件
   * @param maxDate 应用开始时间最大日期过滤条件
   * @param minEndDate 应用结束时间最小日期过滤条件
   * @param maxEndDate 应用结束时间最大日期过滤条件
   * @param limit 返回结果数量限制
   * @return 符合条件的应用信息迭代器
   */
  @GET
  def appList(
      @QueryParam("status") status: JList[ApplicationStatus],
      @DefaultValue("2010-01-01") @QueryParam("minDate") minDate: SimpleDateParam,
      @DefaultValue("3000-01-01") @QueryParam("maxDate") maxDate: SimpleDateParam,
      @DefaultValue("2010-01-01") @QueryParam("minEndDate") minEndDate: SimpleDateParam,
      @DefaultValue("3000-01-01") @QueryParam("maxEndDate") maxEndDate: SimpleDateParam,
      @QueryParam("limit") limit: Integer)
  : Iterator[ApplicationInfo] = {

    // 处理结果数量限制，未指定时返回全部
    val numApps = Option(limit).map(_.toInt).getOrElse(Integer.MAX_VALUE)
    // 判断是否需要包含已完成的应用
    val includeCompleted = status.isEmpty || status.contains(ApplicationStatus.COMPLETED)
    // 判断是否需要包含运行中的应用
    val includeRunning = status.isEmpty || status.contains(ApplicationStatus.RUNNING)

    // 从UI根对象获取应用列表并按条件过滤
    uiRoot.getApplicationInfoList(numApps) { app =>
      // 判断应用是否存在未完成的尝试，只要有一次尝试未完成，整个应用视为运行中
      val anyRunning = app.attempts.isEmpty || !app.attempts.head.completed
      // 状态匹配：要么匹配已完成状态要求，要么匹配运行中状态要求
      // 只要应用存在任何一次尝试落入时间窗口，就保留该应用
      ((!anyRunning && includeCompleted) || (anyRunning && includeRunning)) &&
      app.attempts.exists { attempt =>
        isAttemptInRange(attempt, minDate, maxDate, minEndDate, maxEndDate, anyRunning)
      }
    }
  }

  /**
   * 判断应用尝试是否落入指定的时间范围
   * @param attempt 应用尝试信息
   * @param minStartDate 最小开始时间
   * @param maxStartDate 最大开始时间
   * @param minEndDate 最小结束时间
   * @param maxEndDate 最大结束时间
   * @param anyRunning 是否包含运行中尝试
   * @return 尝试是否符合时间范围要求
   */
  private def isAttemptInRange(
      attempt: ApplicationAttemptInfo,
      minStartDate: SimpleDateParam,
      maxStartDate: SimpleDateParam,
      minEndDate: SimpleDateParam,
      maxEndDate: SimpleDateParam,
      anyRunning: Boolean): Boolean = {
    // 检查开始时间是否在范围内
    val startTimeOk = attempt.startTime.getTime >= minStartDate.timestamp &&
      attempt.startTime.getTime <= maxStartDate.timestamp
    // 运行中应用的结束时间检查：只有最大结束时间在当前时间之后才保留运行中应用
    val endTimeOkForRunning = anyRunning && (maxEndDate.timestamp > System.currentTimeMillis())
    // 已完成应用的结束时间检查：检查结束时间是否在范围内
    val endTimeOkForCompleted = !anyRunning && (attempt.endTime.getTime >= minEndDate.timestamp &&
      attempt.endTime.getTime <= maxEndDate.timestamp)
    // 合并结束时间检查结果
    val endTimeOk = endTimeOkForRunning || endTimeOkForCompleted
    startTimeOk && endTimeOk
  }
}
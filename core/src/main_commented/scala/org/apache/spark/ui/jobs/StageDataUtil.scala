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

import org.apache.spark.status.api.v1.StageData
import org.apache.spark.ui.UIUtils

/**
 * Stage数据工具类，为Spark Web UI提供Stage状态的格式化能力
 * 为作业页面提供Stage运行时间、提交时间的格式化展示功能
 */
object StageDataUtil {
  /**
   * 计算Stage已运行时长
   * @param stageData Stage状态数据
   * @return 运行时长（毫秒），未提交则返回None
   */
  def getDuration(stageData: StageData): Option[Long] = {
    stageData.submissionTime.map { start =>
      // 已完成使用完成时间，未完成使用当前时间
      val end = stageData.completionTime.map(_.getTime).getOrElse(System.currentTimeMillis())
      end - start.getTime
    }
  }

  /**
   * 获取格式化后的Stage运行时长字符串，用于Web UI展示
   * @param stageData Stage状态数据
   * @return 格式化后的时长字符串，未知则返回"Unknown"
   */
  def getFormattedDuration(stageData: StageData): String = {
    val duration = getDuration(stageData)
    duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
  }

  /**
   * 获取格式化后的Stage提交时间字符串，用于Web UI展示
   * @param stageData Stage状态数据
   * @return 格式化后的提交时间字符串，未提交则返回"Unknown"
   */
  def getFormattedSubmissionTime(stageData: StageData): String = {
    stageData.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
  }
}
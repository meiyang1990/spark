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
 * Spark Web UI 作业数据工具类模块
 * 提供作业时间相关计算和格式化功能，用于作业页面展示
 */
package org.apache.spark.ui.jobs

import org.apache.spark.status.api.v1.JobData
import org.apache.spark.ui.UIUtils

/**
 * 作业数据工具类
 * 为Spark Web UI提供作业时间信息的计算和格式化能力，供作业列表和详情页面使用
 */
private[ui] object JobDataUtil {

  /**
   * 计算作业的运行时长
   * @param jobData 作业元数据对象
   * @return 作业运行时长（毫秒），未提交则返回None
   */
  def getDuration(jobData: JobData): Option[Long] = {
    jobData.submissionTime.map { start =>
        val end = jobData.completionTime.map(_.getTime()).getOrElse(System.currentTimeMillis())
        end - start.getTime()
    }
  }

  /**
   * 获取格式化后的作业运行时长字符串
   * @param jobData 作业元数据对象
   * @return 格式化后的时长字符串，未知则返回"Unknown"
   */
  def getFormattedDuration(jobData: JobData): String = {
    val duration = getDuration(jobData)
    duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
  }

  /**
   * 获取格式化后的作业提交时间字符串
   * @param jobData 作业元数据对象
   * @return 格式化后的提交时间字符串，未知则返回"Unknown"
   */
  def getFormattedSubmissionTime(jobData: JobData): String = {
    jobData.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
  }
}
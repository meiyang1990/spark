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

package org.apache.spark.executor

import org.apache.spark.TaskCommitDenied

/**
 * 任务输出提交被Driver拒绝时抛出的异常
 * 
 * 当任务尝试将输出提交到存储系统但被 Driver 拒绝时抛出此异常。
 * 这种情况通常发生在推测执行场景中：多个相同任务的尝试同时运行，
 * 只有一个尝试被允许提交结果，其他尝试将收到拒绝并抛出此异常，
 * 用于Executor端识别并退出被淘汰的任务尝试。
 * 
 * @param msg 异常消息
 * @param jobID 所属作业的ID
 * @param splitID 分区/分片ID
 * @param attemptNumber 任务尝试次数编号
 */
private[spark] class CommitDeniedException(
    msg: String,
    jobID: Int,
    splitID: Int,
    attemptNumber: Int)
  extends Exception(msg) {

  // 将当前异常转换为TaskCommitDenied类型的任务失败原因，用于向Driver汇报提交被拒绝的结果
  def toTaskCommitDeniedReason: TaskCommitDenied = TaskCommitDenied(jobID, splitID, attemptNumber)
}
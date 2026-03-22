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

package org.apache.spark;

import java.io.Serializable;

/**
 * Spark阶段信息暴露接口，用于对外提供Spark作业执行阶段（Stage）的运行时统计信息。
 * 
 * 本接口不允许Spark外部代码自行实现，Spark核心可能会新增方法，破坏外部实现的二进制兼容性。
 * 核心职责：公开Stage的基本信息和任务执行进度统计，供监控、调度和WebUI等模块查询使用。
 */
public interface SparkStageInfo extends Serializable {
  /**
   * 获取当前Stage的ID
   * @return Stage唯一标识ID
   */
  int stageId();

  /**
   * 获取当前尝试执行的Attempt ID（Stage重试会递增该ID）
   * @return 当前尝试的编号
   */
  int currentAttemptId();

  /**
   * 获取Stage提交到调度队列的时间戳
   * @return 提交时间（毫秒时间戳）
   */
  long submissionTime();

  /**
   * 获取Stage的名称，通常包含Stage对应的操作名称描述
   * @return Stage名称字符串
   */
  String name();

  /**
   * 获取当前Stage总共包含的任务数量
   * @return 总任务数
   */
  int numTasks();

  /**
   * 获取当前处于活跃状态（正在执行）的任务数量
   * @return 活跃任务数
   */
  int numActiveTasks();

  /**
   * 获取已经成功完成的任务数量
   * @return 已完成任务数
   */
  int numCompletedTasks();

  /**
   * 获取执行失败的任务数量
   * @return 失败任务数
   */
  int numFailedTasks();
}
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

package org.apache.spark.ui

/**
 * Spark Web UI中各监控指标的提示文本定义，提供鼠标悬停时的帮助说明
 * 集中管理所有UI提示信息，方便维护和国际化修改
 */
private[spark] object ToolTips {
  /** 调度延迟指标提示文本，解释指标含义和调优建议 */
  val SCHEDULER_DELAY =
    """Scheduler delay includes time to ship the task from the scheduler to
       the executor, and time to send the task result from the executor to the scheduler. If
       scheduler delay is large, consider decreasing the size of tasks or decreasing the size
       of task results."""

  /** 任务反序列化时间指标提示文本 */
  val TASK_DESERIALIZATION_TIME =
    """Time spent deserializing the task closure on the executor, including the time to read the
       broadcasted task."""

  /** Shuffle读等待时间指标提示文本 */
  val SHUFFLE_READ_FETCH_WAIT_TIME =
    "Time that the task spent blocked waiting for shuffle data to be read from remote machines."

  /** 输入字节数指标提示文本 */
  val INPUT = "Bytes read from Hadoop or from Spark storage."

  /** 输出字节数指标提示文本 */
  val OUTPUT = "Bytes written to Hadoop."

  /** Shuffle写指标提示文本 */
  val SHUFFLE_WRITE =
    "Bytes and records written to disk in order to be read by a shuffle in a future stage."

  /** Shuffle读总指标提示文本 */
  val SHUFFLE_READ =
    """Total shuffle bytes and records read (includes both data read locally and data read from
       remote executors). """

  /** 远程Shuffle读字节数指标提示文本 */
  val SHUFFLE_READ_REMOTE_SIZE =
    """Total shuffle bytes read from remote executors. This is a subset of the shuffle
       read bytes; the remaining shuffle data is read locally. """

  /** 获取结果时间指标提示文本，解释指标含义和调优建议 */
  val GETTING_RESULT_TIME =
    """Time that the driver spends fetching task results from workers. If this is large, consider
       decreasing the amount of data returned from each task."""

  /** 结果序列化时间指标提示文本 */
  val RESULT_SERIALIZATION_TIME =
    """Time spent serializing the task result on the executor before sending it back to the
       driver."""

  /** GC时间指标提示文本 */
  val GC_TIME =
    """Time that the executor spent paused for Java garbage collection while the task was
       running."""

  /** 峰值执行内存指标提示文本，说明统计范围 */
  val PEAK_EXECUTION_MEMORY =
    """Execution memory refers to the memory used by internal data structures created during
       shuffles, aggregations and joins when Tungsten is enabled. The value of this accumulator
       should be approximately the sum of the peak sizes across all such data structures created
       in this task. For SQL jobs, this only tracks all unsafe operators, broadcast joins, and
       external sort."""

  /** 作业时间线页面操作提示 */
  val JOB_TIMELINE =
    """Shows when jobs started and ended and when executors joined or left. Drag to scroll.
       Click Enable Zooming and use mouse wheel to zoom in/out."""

  /** 阶段时间线页面操作提示 */
  val STAGE_TIMELINE =
    """Shows when stages started and ended and when executors joined or left. Drag to scroll.
       Click Enable Zooming and use mouse wheel to zoom in/out."""

  /** 作业DAG图展示说明 */
  val JOB_DAG =
    """Shows a graph of stages executed for this job, each of which can contain
       multiple RDD operations (e.g. map() and filter()), and of RDDs inside each operation
       (shown as dots)."""

  /** 阶段DAG图展示说明，包含缓存RDD颜色约定 */
  val STAGE_DAG =
    """Shows a graph of RDD operations in this stage, and RDDs inside each one. A stage can run
       multiple operations (e.g. two map() functions) if they can be pipelined. Some operations
       also create multiple RDDs internally. Cached RDDs are shown in green.
    """

  /** 应用执行器数量限制说明，解释动态分配场景下的行为 */
  val APPLICATION_EXECUTOR_LIMIT =
    """Maximum number of executors that this application will use. This limit is finite only when
       dynamic allocation is enabled. The number of granted executors may exceed the limit
       ephemerally when executors are being killed.
    """

  /** 阶段总执行时长定义说明 */
  val DURATION =
    """Elapsed time since the first task of the stage was launched until execution completion of
       all its tasks (Excluding the time of the stage waits to be launched after submitted).
    """
}
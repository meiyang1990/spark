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

package org.apache.spark.scheduler

import java.io._
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * 结果任务（ResultTask）：将计算结果发送回Driver应用的任务。
 * 参见 [[Task]] 了解更多信息。
 *
 * @param stageId 所属Stage的ID
 * @param stageAttemptId 所属Stage的尝试ID
 * @param taskBinary 序列化的RDD和函数的广播变量。反序列化后类型为(RDD[T], (TaskContext, Iterator[T]) => U)
 * @param partition 此任务关联的RDD分区
 * @param numPartitions 所属Stage中的总分区数
 * @param locs 用于本地性调度的首选任务执行位置
 * @param outputId 此任务在作业中的索引（作业可能只在输入RDD的部分分区上启动任务）
 * @param localProperties 用户在Driver端设置的线程本地属性副本
 * @param serializedTaskMetrics Driver端创建并序列化的TaskMetrics
 * @param jobId 所属作业ID（可选）
 * @param appId 所属应用ID（可选）
 * @param appAttemptId 所属应用尝试ID（可选）
 * @param isBarrier 是否属于Barrier Stage（可选）
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    numPartitions: Int,
    locs: Seq[TaskLocation],
    val outputId: Int,
    artifacts: JobArtifactSet,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[U](stageId, stageAttemptId, partition.index, numPartitions, artifacts,
    localProperties, serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.distinct
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    func(context, rdd.iterator(partition, context))
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}

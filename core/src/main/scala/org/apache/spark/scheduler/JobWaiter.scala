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
// 这个文件已经全部加上中文注释

package org.apache.spark.scheduler

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

/**
 * 等待DAGScheduler作业完成的对象。当任务完成时，将结果传递给指定的处理函数。
 * 实现了JobListener接口，作为作业提交者与DAGScheduler之间的桥梁。
 *
 * @param dagScheduler DAG调度器引用，用于取消作业
 * @param jobId 作业唯一ID
 * @param totalTasks 作业包含的总任务数
 * @param resultHandler 结果处理函数，接收分区索引和计算结果
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener with Logging {

  /** 已完成的任务计数器（原子操作保证线程安全） */
  private val finishedTasks = new AtomicInteger(0)
  // 作业的Promise，用于异步等待结果。
  // 对于0个任务的作业（如零分区RDD），直接设置为成功。
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()

  /** 作业是否已完成 */
  def jobFinished: Boolean = jobPromise.isCompleted

  /** 获取作业完成的Future，调用者可以通过它阻塞等待或注册回调 */
  def completionFuture: Future[Unit] = jobPromise.future

  /**
   * 向DAGScheduler发送取消作业的信号（可带取消原因）。
   * 取消操作本身是异步处理的。底层调度器取消该作业的所有任务后，
   * 会以SparkException使该作业失败。
   */
  def cancel(reason: Option[String]): Unit = {
    dagScheduler.cancelJob(jobId, reason)
  }

  /**
   * 向DAGScheduler发送取消作业的信号（无原因）。
   */
  def cancel(): Unit = cancel(None)

  override def taskSucceeded(index: Int, result: Any): Unit = {
    // 同步调用resultHandler，防止resultHandler本身非线程安全
    synchronized {
      resultHandler(index, result.asInstanceOf[T])
    }
    // 所有任务都完成时，将Promise标记为成功
    if (finishedTasks.incrementAndGet() == totalTasks) {
      jobPromise.success(())
    }
  }

  override def jobFailed(exception: Exception): Unit = {
    // 尝试将Promise标记为失败，如果已经完成则忽略
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

}

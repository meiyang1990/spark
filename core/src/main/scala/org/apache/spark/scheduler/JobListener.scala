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

/**
 * 向DAGScheduler提交作业后，用于监听作业完成或失败事件的接口。
 * 每当一个任务成功完成时，监听器会被通知；如果整个作业失败，
 * 也会收到通知（此后不会再有taskSucceeded事件）。
 */
private[spark] trait JobListener {
  /** 当第index个分区的任务成功完成时回调，result为该分区的计算结果 */
  def taskSucceeded(index: Int, result: Any): Unit
  /** 作业失败时回调，exception为失败原因 */
  def jobFailed(exception: Exception): Unit
}

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

package org.apache.spark.util

import java.util.EventListener

import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi

/**
 * 文件: taskListeners.scala
 * 所属模块: Spark Core 核心工具模块
 * 核心职责: 定义Spark任务执行生命周期相关的监听器接口和异常类，供开发者扩展任务生命周期事件处理能力
 */

/**
 * :: DeveloperApi ::
 * 
 * 任务完成监听器接口，定义任务执行完成后触发的回调方法
 * 用户可实现该接口添加自定义逻辑，在任务完成后执行清理、统计等操作
 */
@DeveloperApi
trait TaskCompletionListener extends EventListener {
  def onTaskCompletion(context: TaskContext): Unit
}


/**
 * :: DeveloperApi ::
 * 
 * 任务失败监听器接口，定义任务执行失败时触发的回调方法
 * 要求实现必须幂等，因为onTaskFailure可能会被多次调用
 */
@DeveloperApi
trait TaskFailureListener extends EventListener {
  def onTaskFailure(context: TaskContext, error: Throwable): Unit
}


/**
 * 任务完成监听器回调执行时发生异常，包装抛出该异常
 * 收集所有监听器执行的异常信息，统一对外抛出
 */
private[spark]
class TaskCompletionListenerException(
    errorMessages: Seq[String],
    val previousError: Option[Throwable] = None)
  extends RuntimeException {

  override def getMessage: String = {
    // 组装所有监听器异常的错误信息
    val listenerErrorMessage =
      if (errorMessages.size == 1) {
        errorMessages.head
      } else {
        errorMessages.zipWithIndex.map { case (msg, i) => s"Exception $i: $msg" }.mkString("\n")
      }
    // 拼接任务中原先已有的异常信息
    val previousErrorMessage = previousError.map { e =>
      "\n\nPrevious exception in task: " + e.getMessage + "\n" +
        e.getStackTrace.mkString("\t", "\n\t", "")
    }.getOrElse("")
    listenerErrorMessage + previousErrorMessage
  }
}
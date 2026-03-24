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

import scala.xml.{Node, Text}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.ui.{SparkUITab, UIUtils, WebUIPage}

/**
 * Spark Web UI中任务线程栈页面，用于展示指定任务阻塞线程的线程转储信息
 * 用于调试任务执行过程中的线程阻塞问题，提供线程状态和栈信息展示
 *
 * @param parent 父级UI标签，通常属于任务页面标签
 * @param 当前SparkContext实例
 */
private[spark] class TaskThreadDumpPage(
    parent: SparkUITab,
    sc: Option[SparkContext]) extends WebUIPage("taskThreadDump") {

  /**
   * 生成Executor页面中阻塞线程的线程转储跳转链接
   * @param request HTTP请求对象
   * @param executorId Executor ID
   * @param blockingThreadId 造成阻塞的线程ID
   * @return 跳转到Executor页面对应线程转储锚点的URL
   */
  private def executorThreadDumpUrl(
      request: HttpServletRequest,
      executorId: String,
      blockingThreadId: Long): String = {
    val uiRoot = UIUtils.prependBaseUri(request, parent.basePath)
    s"$uiRoot/executors/?executorId=$executorId#${blockingThreadId}_td_id"
  }

  /**
   * 渲染任务线程转储页面，生成页面内容
   * @param request HTTP请求对象，包含executorId和taskId参数
   * @return 渲染完成的HTML节点序列
   */
  override def render(request: HttpServletRequest): Seq[Node] = {
    // 获取并解码Executor ID参数，缺失则抛出异常
    val executorId = Option(request.getParameter("executorId")).map { executorId =>
      UIUtils.decodeURLParameter(executorId)
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }
    // 获取并解码任务ID参数，缺失则抛出异常
    val taskId = Option(request.getParameter("taskId")).map { taskId =>
      val decoded = UIUtils.decodeURLParameter(taskId)
      decoded.toLong
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing taskId parameter")
    }

    val time = System.currentTimeMillis()
    // 从SparkContext获取指定任务指定Executor上的线程转储
    val maybeThreadDump = sc.get.getTaskThreadDump(taskId, executorId)

    // 根据是否获取到线程转储生成页面内容
    val content = maybeThreadDump.map { thread =>
      val threadId = thread.threadId
      // 处理线程被其他线程阻塞的信息，生成跳转链接
      val blockedBy = thread.blockedByThreadId match {
        case Some(blockingThreadId) =>
          <div>
            Blocked by
            <a href={executorThreadDumpUrl(request, executorId, blockingThreadId)}>
              Thread
              {blockingThreadId}{thread.blockedByLock}
            </a>
          </div>
        case None => Text("")
      }
      // 格式化线程持有的同步锁和监视器信息
      val synchronizers = thread.synchronizers.map(l => s"Lock($l)")
      val monitors = thread.monitors.map(m => s"Monitor($m)")
      val heldLocks = (synchronizers ++ monitors).mkString(", ")

      // 生成HTML页面结构
      <div class="row">
        <div class="col-12">
          <p>Updated at {UIUtils.formatDate(time)}</p>
          <table class={UIUtils.TABLE_CLASS_NOT_STRIPED + " accordion-group"}>
            <thead>
              <th>Thread ID</th>
              <th>Thread Name</th>
              <th>Thread State</th>
              <th>
                {UIUtils.tooltipSpan(<xml:group>Thread Locks</xml:group>,
                  "Objects whose lock the thread currently holds")}
              </th>
            </thead>
            <tbody>
              <tr>
                <td>{threadId}</td>
                <td>{thread.threadName}</td>
                <td>{thread.threadState}</td>
                <td>{blockedBy}{heldLocks}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div>
          <div>
            <pre>{thread.toString}</pre>
          </div>
        </div>
      </div>
    }.getOrElse{
      // 未获取到线程转储，提示任务已结束或获取出错
      Text(s"Task $taskId finished or some error occurred during dumping thread")
    }
    // 使用Spark UI标准页面结构包装返回内容
    UIUtils.headerSparkPage(request, s"Thread dump for task $taskId", content, parent)
  }
}
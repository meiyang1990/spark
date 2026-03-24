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

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
 * 文件级注释：不可中断线程执行器，保证所有提交任务都运行在不可中断线程中
 * 典型场景如Kafka消费者调用，需要避免任务被线程中断机制影响正确性
 * 
 * [[UninterruptibleThreadRunner]] ensures that all tasks are running in an
 * [[UninterruptibleThread]]. A good example is Kafka consumer usage.
 */
private[spark] class UninterruptibleThreadRunner(threadName: String) {
  // 创建单线程线程池，使用自定义不可中断线程工厂
  private val thread = Executors.newSingleThreadExecutor((r: Runnable) => {
    val t = new UninterruptibleThread(threadName) {
      override def run(): Unit = {
        r.run()
      }
    }
    // 设置为守护线程，不阻止JVM退出
    t.setDaemon(true)
    t
  })
  // 基于该线程池创建Scala执行上下文
  private val execContext = ExecutionContext.fromExecutorService(thread)

  /**
   * 在不可中断线程中执行指定任务，如果当前已经是不可中断线程则直接执行
   * @param body 需要执行的任务逻辑
   * @tparam T 任务返回值类型
   * @return 任务执行结果
   */
  def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      // 当前不是不可中断线程，提交到内部不可中断线程池执行
      val future = Future {
        body
      }(execContext)
      // 阻塞等待执行完成，不设置超时
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      // 当前已经是不可中断线程，直接执行避免不必要的线程切换
      body
    }
  }

  /**
   * 关闭线程池，释放资源
   */
  def shutdown(): Unit = {
    thread.shutdown()
  }
}
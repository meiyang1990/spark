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

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.EVENT_LOOP

/**
 * 事件循环抽象基类，负责接收调用方投递的事件，并在独立专用线程中顺序处理所有事件。
 * 该类会启动一个独占的事件处理线程，持续从事件队列拉取事件并处理。
 * 
 * 注意：事件队列会无限增长，子类需要保证`onReceive`能及时处理事件，避免潜在的OOM问题。
 * 
 * @tparam E 事件类型泛型
 * @param name 事件循环线程名称，用于日志标识和调试
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {

  // 存放待处理事件的阻塞队列
  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  // 事件循环停止标志，原子变量保证线程安全
  private val stopped = new AtomicBoolean(false)

  // 暴露给测试使用的事件处理线程
  // 事件处理线程，持续从队列拉取事件处理
  private[spark] val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        // 未停止则持续循环处理事件
        while (!stopped.get) {
          // 阻塞获取队列中的下一个事件
          val event = eventQueue.take()
          try {
            // 处理事件，调用子类实现的业务逻辑
            onReceive(event)
          } catch {
            // 捕获非致命异常，调用错误处理方法
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                // 错误处理方法也抛出异常时，仅记录日志不终止循环
                case NonFatal(e) => logError(log"Unexpected error in ${MDC(EVENT_LOOP, name)}", e)
              }
          }
        }
      } catch {
        // 中断异常：直接退出循环，即使队列还有未处理事件
        case ie: InterruptedException => // exit even if eventQueue is not empty
        // 捕获其他非致命异常，记录日志后退出
        case NonFatal(e) => logError(log"Unexpected error in ${MDC(EVENT_LOOP, name)}", e)
      }
    }

  }

  /**
   * 启动事件循环，完成初始化后启动事件处理线程
   */
  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // 启动事件线程前先调用onStart，保证初始化完成后才开始处理事件
    onStart()
    eventThread.start()
  }

  /**
   * 停止事件循环，中断处理线程并清理资源
   */
  def stop(): Unit = {
    // CAS原子操作，确保只停止一次
    if (stopped.compareAndSet(false, true)) {
      // 中断事件处理线程
      eventThread.interrupt()
      var onStopCalled = false
      try {
        // 等待事件线程退出
        eventThread.join()
        // 线程退出后调用onStop，保证所有事件处理完才执行清理
        onStopCalled = true
        onStop()
      } catch {
        // 当前线程被中断，恢复中断状态
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          // 如果onStop还没调用，在此处调用保证资源清理
          if (!onStopCalled) {
            onStop()
          }
      }
    } else {
      // 允许多次调用stop，静默处理重复停止请求
    }
  }

  /**
   * 投递事件到事件队列，事件线程会异步处理该事件
   * @param event 待投递的事件
   */
  def post(event: E): Unit = {
    if (!stopped.get) {
      if (eventThread.isAlive) {
        eventQueue.put(event)
      } else {
        onError(new IllegalStateException(s"$name has already been stopped accidentally."))
      }
    }
  }

  /**
   * 判断事件循环是否处于活跃状态（已启动且未停止）
   * @return true表示活跃，false表示已停止或未启动
   */
  def isActive: Boolean = eventThread.isAlive

  /**
   * 在启动事件线程前调用，用于子类自定义初始化逻辑
   */
  protected def onStart(): Unit = {}

  /**
   * 在事件线程退出后调用，用于子类自定义清理逻辑
   */
  protected def onStop(): Unit = {}

  /**
   * 在事件线程中被调用，处理从队列拉取到的事件，需要子类实现具体业务逻辑
   * 
   * 注意：应避免在onReceive中执行阻塞操作，否则会阻塞事件线程导致后续事件无法及时处理。
   * 如果需要执行阻塞操作，请将其放到额外线程中执行。
   * 
   * @param event 待处理的事件
   */
  protected def onReceive(event: E): Unit

  /**
   * 当onReceive抛出非致命异常时调用，用于子类自定义错误处理逻辑。
   * onError本身抛出的非致命异常会被忽略，仅记录日志。
   * 
   * @param e 待处理的异常
   */
  protected def onError(e: Throwable): Unit

}
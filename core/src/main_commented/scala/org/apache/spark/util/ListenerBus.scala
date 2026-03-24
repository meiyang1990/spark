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

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.codahale.metrics.Timer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{EVENT, LISTENER, TOTAL_TIME}
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate

/**
 * 文件核心功能：实现Spark事件总线基础抽象，负责将事件分发到所有注册的监听器
 * 是Spark内部事件监听机制的核心基础设施，支持多种事件监听器的注册、移除和事件广播
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // 存储监听器及其对应性能统计定时器的列表，使用线程安全的CopyOnWriteArrayList实现
  private[this] val listenersPlusTimers = new CopyOnWriteArrayList[(L, Option[Timer])]

  // Marked `private[spark]` for access in tests.
  // 获取所有已注册的监听器列表，供测试和类型查找使用
  private[spark] def listeners = listenersPlusTimers.asScala.map(_._1).asJava

  // 延迟加载当前Spark运行环境实例
  private lazy val env = SparkEnv.get

  // 延迟加载慢事件日志记录配置开关，从Spark配置读取是否开启慢事件日志
  private lazy val logSlowEventEnabled = if (env != null) {
    env.conf.get(config.LISTENER_BUS_LOG_SLOW_EVENT_ENABLED)
  } else {
    false
  }

  // 延迟加载慢事件阈值配置，超过该阈值的事件处理会被记录日志
  private lazy val logSlowEventThreshold = if (env != null) {
    env.conf.get(config.LISTENER_BUS_LOG_SLOW_EVENT_TIME_THRESHOLD)
  } else {
    Long.MaxValue
  }

  /**
   * 获取度量监听器事件处理耗时的CodaHale指标定时器
   * 留给子类实现自定义的度量逻辑，默认不提供定时器
   * 
   * @param listener 目标监听器
   * @return 可选的Timer实例
   */
  protected def getTimer(listener: L): Option[Timer] = None

  /**
   * 添加新的事件监听器到总线，线程安全，可在任意线程调用
   * 
   * @param listener 要添加的监听器实例
   */
  final def addListener(listener: L): Unit = {
    listenersPlusTimers.add((listener, getTimer(listener)))
  }

  /**
   * 从总线移除监听器，移除后监听器不会再接收任何事件，线程安全，可在任意线程调用
   * 
   * @param listener 要移除的监听器实例
   */
  final def removeListener(listener: L): Unit = {
    listenersPlusTimers.asScala.find(_._1 eq listener).foreach { listenerAndTimer =>
      listenersPlusTimers.remove(listenerAndTimer)
    }
  }

  /**
   * 移除所有已注册的监听器，线程安全，可在任意线程调用
   */
  final def removeAllListeners(): Unit = {
    listenersPlusTimers.clear()
  }

  /**
   * 当监听器处理出错时执行移除操作，允许子类扩展额外清理逻辑
   * 例如AsyncEventQueue在LiveListenerBus中会清理对应的队列
   * 
   * @param listener 出错的监听器实例
   */
  def removeListenerOnError(listener: L): Unit = {
    removeListener(listener)
  }


  /**
   * 将事件广播分发到所有已注册的监听器
   * 调用方需要保证所有事件都在同一个线程调用postToAll，保证事件顺序
   * 
   * @param event 要分发的事件对象
   */
  def postToAll(event: E): Unit = {
    // 为了避免Scala集合包装器的性能开销，直接使用Java迭代器遍历
    val iter = listenersPlusTimers.iterator
    while (iter.hasNext) {
      val listenerAndMaybeTimer = iter.next()
      val listener = listenerAndMaybeTimer._1
      val maybeTimer = listenerAndMaybeTimer._2
      // 如果开启了性能度量，启动定时器上下文
      val maybeTimerContext = if (maybeTimer.isDefined) {
        maybeTimer.get.time()
      } else {
        null
      }
      // 延迟加载监听器格式化类名，仅在需要日志时生成
      lazy val listenerName = Utils.getFormattedClassName(listener)
      try {
        // 调用具体分发方法将事件派发给当前监听器
        doPostEvent(listener, event)
        // 如果线程被中断，立即抛出异常，将中断关联到当前监听器
        if (Thread.interrupted()) {
          throw new InterruptedException()
        }
      } catch {
        // 处理中断异常：记录日志并移除出错的监听器
        case ie: InterruptedException =>
          logError(log"Interrupted while posting to " +
            log"${MDC(LISTENER, listenerName)}. Removing that listener.", ie)
          removeListenerOnError(listener)
        // 处理非致命异常：如果不是可忽略异常，记录日志但不移除监听器
        case NonFatal(e) if !isIgnorableException(e) =>
          logError(log"Listener ${MDC(LISTENER, listenerName)} threw an exception", e)
      } finally {
        // 如果开启了性能度量，停止定时器并检查是否需要记录慢事件日志
        if (maybeTimerContext != null) {
          val elapsed = maybeTimerContext.stop()
          if (logSlowEventEnabled && elapsed > logSlowEventThreshold) {
            logInfo(log"Process of event ${MDC(EVENT, redactEvent(event))} by" +
              log"listener ${MDC(LISTENER, listenerName)} took " +
              log"${MDC(TOTAL_TIME, elapsed / 1000000d)}ms.")
          }
        }
      }
    }
  }

  /**
   * 将具体事件分发到指定监听器处理
   * 子类必须实现该方法定义具体的分发逻辑，所有事件保证在同一个线程处理
   * 
   * @param listener 目标监听器
   * @param event 要处理的事件
   */
  protected def doPostEvent(listener: L, event: E): Unit

  /**
   * 判断指定异常是否是可忽略的异常，用于过滤不需要记录日志的异常
   * 子类可以重写该方法自定义异常过滤逻辑，默认所有异常都不忽略
   * 
   * @param e 待判断的异常对象
   * @return 是否可忽略该异常
   */
  protected def isIgnorableException(e: Throwable): Boolean = false

  /**
   * 根据指定类型查找所有匹配类型的监听器实例
   * 
   * @tparam T 要查找的监听器类型
   * @return 匹配类型的监听器序列
   */
  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

  /**
   * 对敏感事件信息打码脱敏，避免日志泄露敏感配置信息
   * 当前仅对SparkListenerEnvironmentUpdate事件中的环境信息脱敏
   * 
   * @param e 原始事件对象
   * @return 脱敏后的事件对象
   */
  private def redactEvent(e: E): E = {
    e match {
      case event: SparkListenerEnvironmentUpdate =>
        EventLoggingListener.redactEvent(env.conf, event).asInstanceOf[E]
      case _ => e
    }
  }

}
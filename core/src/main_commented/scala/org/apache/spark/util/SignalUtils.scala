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

import java.util.Collections

import scala.jdk.CollectionConverters._

import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys._

/**
 * POSIX信号处理工具类，为Spark提供统一的进程信号注册和回调执行能力
 * 仅在Unix-like系统上生效，负责管理各类系统信号（如TERM、INT等）的自定义处理逻辑
 */
private[spark] object SignalUtils extends Logging {

  /** 标志位，确保日志处理器只注册一次，避免重复注册 */
  private var loggerRegistered = false

  /**
   * 注册信号日志处理器，在接收到常见终止信号时打印日志
   * 用于在Unix-like系统上记录进程接收到的终止信号，便于问题排查
   * @param log 用于输出日志的SLF4J logger实例
   */
  def registerLogger(log: Logger): Unit = synchronized {
    if (!loggerRegistered) {
      Seq("TERM", "HUP", "INT").foreach { sig =>
        SignalUtils.register(sig) {
          log.error("RECEIVED SIGNAL " + sig)
          false
        }
      }
      loggerRegistered = true
    }
  }

  /**
   * 注册指定信号的回调处理动作，默认使用通用失败日志
   * @param signal 要注册的信号名称（如TERM、INT等）
   * @param action 信号触发时执行的回调动作，返回true表示信号已处理无需传递，false表示需要继续传递给原有处理器
   */
  def register(signal: String)(action: => Boolean): Unit = {
    if (Utils.isUnix) {
      register(signal, log"Failed to register signal handler for ${MDC(SIGNAL, signal)}",
        logStackTrace = true)(action)
    }
  }

  /**
   * 注册指定信号的回调处理动作，支持自定义注册失败日志和栈跟踪配置
   * 所有对应信号的回调动作都会在单独线程中执行，仅支持Unix-like系统，不支持的场景仅输出警告不中断流程
   * @param signal 要注册的信号名称（如TERM、INT等）
   * @param failMessage 注册失败时输出的日志消息
   * @param logStackTrace 注册失败时是否打印异常栈跟踪，默认为true
   * @param action 信号触发时执行的回调动作，返回true表示信号已处理无需传递，false表示需要继续传递给原有处理器
   */
  def register(
      signal: String,
      failMessage: MessageWithContext,
      logStackTrace: Boolean = true)(
      action: => Boolean): Unit = synchronized {
    try {
      val handler = handlers.getOrElseUpdate(signal, {
        logInfo(log"Registering signal handler for ${MDC(SIGNAL, signal)}")
        new ActionHandler(new Signal(signal))
      })
      handler.register(action)
    } catch {
      case ex: Exception =>
        if (logStackTrace) {
          logWarning(failMessage, ex)
        } else {
          logWarning(failMessage)
        }
    }
  }

  /**
   * 单个信号处理器，维护该信号对应的所有回调动作列表，负责信号接收时分发执行所有回调
   * 保存原有处理器，在所有回调都未处理信号时将信号传递给原有处理器
   * @param signal 要处理的信号对象
   */
  private class ActionHandler(signal: Signal) extends SignalHandler {

    /**
     * 信号触发时要执行的动作列表，线程安全存储
     * 每个回调返回true表示信号已处理，不需要继续 escalate 到下一个回调和原有处理器
     */
    private val actions = Collections.synchronizedList(new java.util.LinkedList[() => Boolean])

    // 保存注册当前处理器之前的原有信号处理器，用于后续传递信号
    private val prevHandler: SignalHandler = Signal.handle(signal, this)

    /**
     * 信号接收入口方法，负责执行所有已注册回调，根据处理结果决定是否传递给原有处理器
     * 处理过程中会先恢复原有处理器，保证处理过程中收到新信号可被正常处理，处理完成后重新注册当前处理器
     * @param sig 接收到的信号对象
     */
    override def handle(sig: Signal): Unit = {
      // 先恢复原有处理器，保证当前处理器执行过程中收到的信号可被原有处理器处理
      Signal.handle(signal, prevHandler)

      // 执行所有已注册动作，所有动作都返回false才 escalate 给原有处理器
      // 使用map保证所有动作都执行完成，再判断是否需要 escalate
      val escalate = actions.asScala.map(action => action()).forall(_ == false)
      if (escalate) {
        prevHandler.handle(sig)
      }

      // 重新注册当前处理器，等待下一次信号
      Signal.handle(signal, this)
    }

    /**
     * 向当前信号处理器添加一个新的回调动作
     * @param action 信号触发时执行的回调动作，返回true表示信号已处理无需传递，false表示需要继续传递
     */
    def register(action: => Boolean): Unit = actions.add(() => action)
  }

  /** 信号名称到对应处理器的映射缓存，复用同一个信号的处理器实例 */
  private val handlers = new scala.collection.mutable.HashMap[String, ActionHandler]
}
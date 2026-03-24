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

import java.io.File
import java.util.PriorityQueue
import java.util.concurrent.TimeUnit

import scala.util.Try

import org.apache.hadoop.fs.FileSystem

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.internal.config.SPARK_SHUTDOWN_TIMEOUT_MS

/**
 * Spark 关闭钩子管理器，负责JVM关闭时按优先级执行清理任务，核心功能包括临时目录清理、自定义钩子执行
 */
private[spark] object ShutdownHookManager extends Logging {
  /** 默认关闭钩子优先级 */
  val DEFAULT_SHUTDOWN_PRIORITY = 100

  /**
   * SparkContext实例的关闭优先级，优先级低于默认值，保证其他用户钩子在SparkContext关闭前执行
   */
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50

  /**
   * 临时目录清理钩子的优先级，低于SparkContext关闭优先级，保证作业结束后再清理临时目录，避免清理正在使用的文件
   */
  val TEMP_DIR_SHUTDOWN_PRIORITY = 25

  /** 延迟初始化关闭钩子管理器实例 */
  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    manager.install()
    manager
  }

  /** 存储需要在关闭时删除的路径集合 */
  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  // 提前初始化日志组件，添加临时目录清理的关闭钩子
  logDebug("Adding shutdown hook") // force eager creation of logger
  addShutdownHook(TEMP_DIR_SHUTDOWN_PRIORITY) { () =>
    logDebug("Shutdown hook called")
    // 先转成数组遍历，避免删除过程中修改原集合导致并发问题
    shutdownDeletePaths.toArray.foreach { dirPath =>
      try {
        logDebug(log"Deleting directory ${MDC(LogKeys.PATH, dirPath)}")
        Utils.deleteRecursively(new File(dirPath))
      } catch {
        case e: Exception =>
          logError(log"Exception while deleting Spark temp dir: ${MDC(PATH, dirPath)}", e)
      }
    }
  }

  /**
   * 注册需要在JVM关闭时删除的目录
   * @param file 待删除的目录文件
   */
  def registerShutdownDeleteDir(file: File): Unit = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  /**
   * 取消注册需要在JVM关闭时删除的目录
   * @param file 待取消删除的目录文件
   */
  def removeShutdownDeleteDir(file: File): Unit = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.remove(absolutePath)
    }
  }

  /**
   * 判断路径是否已经注册为关闭删除
   * @param file 待检查的文件
   * @return 是否已注册
   */
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  /**
   * 检查当前文件是否已经被某个已注册的父目录包含在删除列表中，避免重复删除导致IO异常
   * @param file 待检查的文件
   * @return 如果当前文件是某个已注册删除目录的子文件且不相等，返回true
   */
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo(log"path = ${MDC(LogKeys.FILE_NAME, file)}, already present as root for deletion.")
    }
    retval
  }

  /**
   * 检测当前是否处于JVM关闭流程中
   * @return 如果正在关闭返回true，否则返回false，可能存在误判但不影响核心逻辑
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run(): Unit = {}
      }
      // scalastyle:off runtimeaddshutdownhook
      Runtime.getRuntime.addShutdownHook(hook)
      // scalastyle:on runtimeaddshutdownhook
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  /**
   * 添加默认优先级的关闭钩子
   * @param hook 关闭时执行的代码
   * @return 可用于取消注册的钩子句柄
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  /**
   * 添加指定优先级的关闭钩子，优先级数值越大越先执行
   * @param priority 钩子优先级
   * @param hook 关闭时执行的代码
   * @return 可用于取消注册的钩子句柄
   */
  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  /**
   * 移除已注册的关闭钩子
   * @param ref `addShutdownHook`返回的钩子句柄
   * @return 是否成功移除
   */
  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}

/**
 * Spark内部关闭钩子管理器实现，基于优先级队列管理钩子，集成Hadoop的ShutdownHookManager保证执行顺序
 */
private [util] class SparkShutdownHookManager {

  /** 基于优先级队列存储关闭钩子，按优先级从高到低排序 */
  private val hooks = new PriorityQueue[SparkShutdownHook]()
  /** 标记是否已经开始关闭流程，volatile保证多线程可见性 */
  @volatile private var shuttingDown = false

  /**
   * 安装全局关闭钩子，注册到Hadoop ShutdownHookManager，在JVM关闭时按顺序执行所有注册钩子
   */
  def install(): Unit = {
    val hookTask = new Runnable() {
      override def run(): Unit = runAll()
    }
    // 优先级高于Hadoop默认的关闭钩子，保证Spark清理在Hadoop文件系统关闭前完成
    val priority = FileSystem.SHUTDOWN_HOOK_PRIORITY + 30
    // 关闭超时需要从系统属性读取，因为此时Spark配置还未同步到系统属性
    val timeout = new SparkConf().get(SPARK_SHUTDOWN_TIMEOUT_MS)

    timeout.fold {
      org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
        hookTask, priority)
    } { t =>
      org.apache.hadoop.util.ShutdownHookManager.get().addShutdownHook(
        hookTask, priority, t, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * 按优先级从高到低依次执行所有注册的关闭钩子
   */
  def runAll(): Unit = {
    shuttingDown = true
    var nextHook: SparkShutdownHook = null
    while ({ nextHook = hooks.synchronized { hooks.poll() }; nextHook != null }) {
      Try(Utils.logUncaughtExceptions(nextHook.run()))
    }
  }

  /**
   * 添加新的关闭钩子
   * @param priority 钩子优先级
   * @param hook 钩子执行代码
   * @return 钩子句柄，用于后续取消注册
   */
  def add(priority: Int, hook: () => Unit): AnyRef = {
    hooks.synchronized {
      if (shuttingDown) {
        throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
      }
      val hookRef = new SparkShutdownHook(priority, hook)
      hooks.add(hookRef)
      hookRef
    }
  }

  /**
   * 移除已注册的关闭钩子
   * @param ref 钩子句柄
   * @return 是否成功移除
   */
  def remove(ref: AnyRef): Boolean = {
    hooks.synchronized { hooks.remove(ref) }
  }

}

/**
 * 单个关闭钩子实现，实现Comparable接口支持优先级队列排序
 * @param priority 钩子优先级
 * @param hook 钩子执行代码
 */
private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {

  /** 优先级大的排在前面，先执行 */
  override def compareTo(other: SparkShutdownHook): Int = other.priority.compareTo(priority)

  /** 执行钩子代码 */
  def run(): Unit = hook()

}
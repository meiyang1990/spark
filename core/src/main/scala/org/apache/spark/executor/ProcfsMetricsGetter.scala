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

package org.apache.spark.executor

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils


/**
 * 进程级资源指标数据类。
 *
 * 通过读取 /proc 文件系统收集 Executor 进程树的内存使用情况。
 * 按进程类型分类统计：Java 进程、Python 进程、其他进程。
 *
 * @param jvmVmemTotal JVM 进程的虚拟内存总量（字节）
 * @param jvmRSSTotal JVM 进程的常驻内存（RSS）总量（字节）
 * @param pythonVmemTotal Python 进程的虚拟内存总量（PySpark 场景）
 * @param pythonRSSTotal Python 进程的 RSS 总量
 * @param otherVmemTotal 其他进程的虚拟内存总量
 * @param otherRSSTotal 其他进程的 RSS 总量
 */
private[spark] case class ProcfsMetrics(
    jvmVmemTotal: Long,
    jvmRSSTotal: Long,
    pythonVmemTotal: Long,
    pythonRSSTotal: Long,
    otherVmemTotal: Long,
    otherRSSTotal: Long)

/**
 * 基于 /proc 文件系统的进程指标采集器。
 *
 * 【功能】
 * 通过读取 Linux /proc 文件系统中的进程状态文件，收集 Executor 进程及其子进程
 * （如 Python Worker）的内存使用指标。
 *
 * 【采集的指标】
 * - 虚拟内存（VSIZE）：进程可访问的虚拟地址空间大小
 * - 常驻内存（RSS）：实际占用的物理内存大小
 *
 * 【进程分类】
 * 根据 /proc/{pid}/stat 中的 comm 字段判断进程类型：
 * - 包含 "java" → JVM 进程
 * - 包含 "python" → Python 进程
 * - 其他 → 其他进程
 *
 * 【使用条件】
 * 仅在以下条件同时满足时可用：
 * 1. /proc 文件系统存在（即 Linux 系统）
 * 2. spark.executor.processTreeMetrics.enabled = true
 *
 * 部分设计思路参考了 Hadoop 的 ProcfsBasedProcessTree 类。
 *
 * @param procfsDir /proc 文件系统路径，默认为 "/proc/"
 */
private[spark] class ProcfsMetricsGetter(procfsDir: String = "/proc/") extends Logging {
  private val procfsStatFile = "stat"
  private val testing = Utils.isTesting
  // 内存页大小，RSS 值需要乘以页大小才能转换为字节
  private val pageSize = computePageSize()
  private var isAvailable: Boolean = isProcfsAvailable
  // 当前进程句柄，用于获取进程树
  private val currentProcessHandle = ProcessHandle.current()

  // 检查 /proc 文件系统是否可用
  private lazy val isProcfsAvailable: Boolean = {
    if (testing) {
       true
    }
    else {
      val procDirExists = Try(Files.exists(Paths.get(procfsDir))).recover {
        case ioe: IOException =>
          logWarning("Exception checking for procfs dir", ioe)
          false
      }
      val shouldPollProcessTreeMetrics =
        SparkEnv.get.conf.get(config.EXECUTOR_PROCESS_TREE_METRICS_ENABLED)
      procDirExists.get && shouldPollProcessTreeMetrics
    }
  }

  // 通过系统调用获取内存页大小（通常为 4096 字节）
  private def computePageSize(): Long = {
    if (testing) {
      return 4096;
    }
    try {
      val cmd = Array("getconf", "PAGESIZE")
      val out = Utils.executeAndGetOutput(cmd.toImmutableArraySeq)
      Integer.parseInt(out.split("\n")(0))
    } catch {
      case e: Exception =>
        logDebug("Exception when trying to compute pagesize, as a" +
          " result reporting of ProcessTree metrics is stopped")
        isAvailable = false
        0
    }
  }

  /**
   * 从单个进程的 /proc/{pid}/stat 文件中读取指标并累加到总指标中。
   *
   * stat 文件格式参考：http://man7.org/linux/man-pages/man5/proc.5.html
   * 关键字段：
   * - 字段 2（comm）：进程名，用于判断进程类型
   * - 字段 23（vsize）：虚拟内存大小（字节）
   * - 字段 24（rss）：常驻内存页数（需乘以 pageSize 转换为字节）
   */
  private[executor] def addProcfsMetricsFromOneProcess(
      allMetrics: ProcfsMetrics,
      pid: Long): ProcfsMetrics = {

    try {
      val pidDir = new File(procfsDir, pid.toString)
      def openReader(): BufferedReader = {
        val f = new File(pidDir, procfsStatFile)
        new BufferedReader(new InputStreamReader(new FileInputStream(f), UTF_8))
      }
      Utils.tryWithResource(openReader()) { in =>
        val procInfo = in.readLine
        // comm 字段在括号内，可能包含空格，需要特殊处理解析
        val commStartIndex = procInfo.indexOf('(')
        val commEndIndex = procInfo.lastIndexOf(')') + 1
        val pidArray = Array(procInfo.substring(0, commStartIndex).trim)
        val commArray = Array(procInfo.substring(commStartIndex, commEndIndex))
        val splitAfterComm = procInfo.substring(commEndIndex).trim.split(" ")
        val procInfoSplit = pidArray ++ commArray ++ splitAfterComm
        val vmem = procInfoSplit(22).toLong
        val rssMem = procInfoSplit(23).toLong * pageSize
        // 根据进程名判断类型并累加到对应字段
        if (procInfoSplit(1).toLowerCase(Locale.US).contains("java")) {
          allMetrics.copy(
            jvmVmemTotal = allMetrics.jvmVmemTotal + vmem,
            jvmRSSTotal = allMetrics.jvmRSSTotal + (rssMem)
          )
        }
        else if (procInfoSplit(1).toLowerCase(Locale.US).contains("python")) {
          allMetrics.copy(
            pythonVmemTotal = allMetrics.pythonVmemTotal + vmem,
            pythonRSSTotal = allMetrics.pythonRSSTotal + (rssMem)
          )
        }
        else {
          allMetrics.copy(
            otherVmemTotal = allMetrics.otherVmemTotal + vmem,
            otherRSSTotal = allMetrics.otherRSSTotal + (rssMem)
          )
        }
      }
    } catch {
      case f: IOException =>
        logDebug("There was a problem with reading" +
          " the stat file of the process. ", f)
        throw f
    }
  }

  /**
   * 计算当前 Executor 的进程树（当前进程及所有子孙进程）
   */
  private[executor] def computeProcessTree(): Set[Long] = {
    if (!isAvailable) {
      Set.empty
    } else {
      val children = currentProcessHandle.descendants().map(_.pid()).toList.asScala.toSet
      children + currentProcessHandle.pid()
    }
  }

  /**
   * 计算进程树中所有进程的资源指标总和。
   * 如果任何一个进程读取失败，返回全零指标（避免返回误导性的部分数据）。
   */
  private[spark] def computeAllMetrics(): ProcfsMetrics = {
    if (!isAvailable) {
      return ProcfsMetrics(0, 0, 0, 0, 0, 0)
    }
    val pids = computeProcessTree()
    var allMetrics = ProcfsMetrics(0, 0, 0, 0, 0, 0)
    for (p <- pids) {
      try {
        allMetrics = addProcfsMetricsFromOneProcess(allMetrics, p)
        if (!isAvailable) {
          return ProcfsMetrics(0, 0, 0, 0, 0, 0)
        }
      } catch {
        case _: IOException =>
          return ProcfsMetrics(0, 0, 0, 0, 0, 0)
      }
    }
    allMetrics
  }
}

/**
 * ProcfsMetricsGetter 伴生对象，提供全局单例实例
 */
private[spark] object ProcfsMetricsGetter {
  // 全局单例，供 ExecutorMetricType 使用
  final val pTreeInfo = new ProcfsMetricsGetter
}

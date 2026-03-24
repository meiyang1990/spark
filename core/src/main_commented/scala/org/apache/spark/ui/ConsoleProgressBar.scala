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

package org.apache.spark.ui

import java.util.concurrent.TimeUnit

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI._
import org.apache.spark.status.api.v1.StageData
import org.apache.spark.util.ThreadUtils

/**
 * 控制台进度条组件，在终端控制台实时展示Spark Stage的执行进度。
 * 定期从应用状态存储拉取活跃Stage的状态信息，Stage启动500ms后才会显示进度条。
 * 多个并发Stage会合并展示在同一行，自动更新进度不干扰用户日志输出。
 *
 * @param sc SparkContext上下文实例，用于获取配置和状态信息
 */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {
  // 回车符，用于回到行首覆盖输出
  private val CR = '\r'
  // 进度条更新间隔，单位毫秒
  private val updatePeriodMSec = sc.conf.get(UI_CONSOLE_PROGRESS_UPDATE_INTERVAL)
  // 进度条初始显示延迟，Stage启动后等待500ms才显示，避免快速完成的Stage闪烁
  private val firstDelayMSec = 500L
  // 获取标准错误输出作为控制台输出，在RedirectConsolePlugin安装前就是终端控制台，对应spark-shell场景
  private val console = System.err

  // 终端宽度，优先从环境变量COLUMNS获取，默认80列
  private val TerminalWidth = sys.env.getOrElse("COLUMNS", "80").toInt

  private var lastFinishTime = 0L
  private var lastUpdateTime = 0L
  private var lastProgressBar = ""

  // 创建后台定时刷新线程，定期更新进度条
  private val timer = ThreadUtils.newDaemonSingleThreadScheduledExecutor("refresh progress")
  private val timerFuture = timer.scheduleAtFixedRate(
    () => refresh(), firstDelayMSec, updatePeriodMSec, TimeUnit.MILLISECONDS)

  /**
   * 定时刷新进度条状态，拉取活跃Stage信息并更新控制台显示
   */
  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    // 所有Stage刚结束还没到延迟时间，不刷新
    if (now - lastFinishTime < firstDelayMSec) {
      return
    }
    // 获取所有活跃Stage，过滤掉启动未满500ms的Stage
    val stages = sc.statusStore.activeStages()
      .filter { s => now - s.submissionTime.get.getTime() > firstDelayMSec }
    // 存在需要显示的Stage，最多显示前3个Stage的进度
    if (stages.length > 0) {
      show(now, stages.take(3))
    }
  }

  /**
   * 在控制台渲染进度条，进度条固定占用一行，通过覆盖自身实现更新，不会干扰后续日志输出
   * @param now 当前时间戳
   * @param stages 需要显示进度的Stage列表
   */
  private def show(now: Long, stages: Seq[StageData]): Unit = {
    // 平均分配每个Stage占用的终端宽度
    val width = TerminalWidth / stages.length
    val bar = stages.map { s =>
      val total = s.numTasks
      // 进度条前缀：[Stage X:
      val header = s"[Stage ${s.stageId}:"
      // 进度条后缀：(已完成 + 运行中) / 总任务数]
      val tailer = s"(${s.numCompleteTasks} + ${s.numActiveTasks}) / $total]"
      // 计算进度条本体可用宽度
      val w = width - header.length - tailer.length
      // 生成进度条本体
      val bar = if (w > 0) {
        // 计算已完成比例对应的宽度
        val percent = w * s.numCompleteTasks / total
        // 生成进度条字符：已完成=，当前位置>，未完成空格
        (0 until w).map { i =>
          if (i < percent) "=" else if (i == percent) ">" else " "
        }.mkString("")
      } else {
        ""
      }
      header + bar + tailer
    }.mkString("")

    // 仅当进度变化或超过1分钟未更新时刷新，避免闲置时SSH连接断开
    if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
      console.print(s"$CR$bar$CR")
      lastUpdateTime = now
    }
    lastProgressBar = bar
  }

  /**
   * 清空控制台中已显示的进度条
   */
  private def clear(): Unit = {
    if (!lastProgressBar.isEmpty) {
      // 用空格覆盖整行，然后回到行首
      console.printf(s"$CR${" ".repeat(TerminalWidth)}$CR")
      lastProgressBar = ""
    }
  }

  /**
   * 标记所有Stage已完成，清空进度条，避免进度条和后续作业输出混排
   */
  def finishAll(): Unit = synchronized {
    clear()
    lastFinishTime = System.currentTimeMillis()
  }

  /**
   * 停止后台刷新定时器，释放资源避免SparkContext无法被GC回收
   */
  def stop(): Unit = {
    timerFuture.cancel(false)
    ThreadUtils.shutdown(timer)
  }
}
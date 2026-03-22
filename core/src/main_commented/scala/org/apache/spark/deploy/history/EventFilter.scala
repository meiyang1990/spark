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

package org.apache.spark.deploy.history

import scala.io.{Codec, Source}
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.deploy.history.EventFilter.FilterStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{LINE, LINE_NUM, PATH}
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * 事件过滤构建器，提供从Spark监听事件中收集过滤规则所需信息、并最终生成事件过滤器的接口
 * 用于历史服务器对事件日志进行压缩裁剪时，提前收集需要保留的事件信息
 */
private[spark] trait EventFilterBuilder extends SparkListenerInterface {
  /** 根据已收集的信息构建最终可用的事件过滤器 */
  def createFilter(): EventFilter
}

/** 
 * 事件过滤器接口，定义判断给定Spark事件是否应该被保留的方法
 * 用于历史事件日志压缩，过滤掉不需要保留的过期事件
 */
private[spark] trait EventFilter {
  /**
   * 获取过滤统计信息，用于评估压缩效果
   * 统计项包含作业、阶段、任务的总数量和保留数量，如果过滤器不统计作业相关事件则返回None
   * @return 过滤统计信息可选对象
   */
  def statistics(): Option[FilterStatistics]

  /**
   * 返回事件过滤偏函数，判断事件是否应该被保留
   * 偏函数只匹配过滤器能够处理决策的事件，无法决策的事件留空不匹配，将默认保留
   * @return 偏函数，输入事件返回是否接受（保留）该事件
   */
  def acceptFn(): PartialFunction[SparkListenerEvent, Boolean]
}

/**
 * 事件过滤工具类，提供对事件日志文件应用过滤规则的能力
 * 用于Spark历史服务器的事件日志压缩功能
 */
private[spark] object EventFilter extends Logging {
  /**
   * 过滤统计信息，记录过滤前后作业、阶段、任务的数量，用于评估压缩效果
   * @param totalJobs 原始总作业数
   * @param liveJobs 保留的作业数
   * @param totalStages 原始总阶段数
   * @param liveStages 保留的阶段数
   * @param totalTasks 原始总任务数
   * @param liveTasks 保留的任务数
   */
  case class FilterStatistics(
      totalJobs: Long,
      liveJobs: Long,
      totalStages: Long,
      liveStages: Long,
      totalTasks: Long,
      liveTasks: Long)

  /**
   * 对指定HDFS上的事件日志文件应用所有过滤规则，对不同结果的事件执行对应回调
   * @param fs Hadoop文件系统对象
   * @param filters 要应用的过滤器列表
   * @param path 事件日志文件路径
   * @param onAccepted 事件被保留时的回调
   * @param onRejected 事件被过滤时的回调
   * @param onUnidentified 无法解析的事件行回调
   */
  def applyFilterToFile(
      fs: FileSystem,
      filters: Seq[EventFilter],
      path: Path,
      onAccepted: (String, SparkListenerEvent) => Unit,
      onRejected: (String, SparkListenerEvent) => Unit,
      onUnidentified: String => Unit): Unit = {
    // 自动管理输入流资源，读取事件日志文件
    Utils.tryWithResource(EventLogFileReader.openEventLog(path, fs)) { in =>
      // 按UTF-8编码读取所有行
      val lines = Source.fromInputStream(in)(Codec.UTF8).getLines()

      // 遍历所有行，同时记录行号
      lines.zipWithIndex.foreach { case (line, lineNum) =>
        try {
          // 尝试解析JSON为Spark事件对象
          val event = try {
            Some(JsonProtocol.sparkEventFromJson(line))
          } catch {
            // 解析失败的行交给未识别回调处理，返回None
            case NonFatal(_) =>
              onUnidentified(line)
              None
          }

          // 如果成功解析出事件，应用所有过滤器
          event.foreach { e =>
            // 收集所有过滤器给出的决策结果，无法决策的过滤器返回空
            val results = filters.flatMap(_.acceptFn().lift.apply(e))
            // 只要有至少一个过滤器明确拒绝该事件，就过滤掉
            if (results.nonEmpty && results.forall(_ == false)) {
              onRejected(line, e)
            } else {
              // 否则保留该事件
              onAccepted(line, e)
            }
          }
        } catch {
          // 捕获解析过程中发生的异常，记录日志后重新抛出
          case e: Exception =>
            logError(log"Exception parsing Spark event log: ${MDC(PATH, path.getName)}", e)
            logError(log"Malformed line #${MDC(LINE_NUM, lineNum)}: ${MDC(LINE, line)}\n")
            throw e
        }
      }
    }
  }
}
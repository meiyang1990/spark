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

import java.io.IOException
import java.net.URI
import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.EventFilter.FilterStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.util.Utils

/**
 * 文件级注释：Spark历史服务器事件日志压缩工具，将多个旧事件日志文件合并压缩为单个文件，减少存储占用
 * 
 * 该类通过两阶段读取实现日志压缩：
 * 1) 初始化所有可用的[[EventFilterBuilder]]实例，重放旧事件日志收集过滤所需信息，用于创建过滤实例
 * 2) 从Builder生成[[EventFilter]]实例，再次重放旧事件日志，仅保留过滤器接受的事件，写入压缩文件
 * 
 * 压缩效果通过过滤器统计信息计算得分，表示可过滤掉事件的大致比例。基于启发式规则计算得分：任务事件通常占日志大部分体积。
 */
class EventLogFileCompactor(
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    fs: FileSystem,
    maxFilesToRetain,
    compactionThresholdScore: Double) extends Logging {

  require(maxFilesToRetain > 0, "Max event log files to retain should be higher than 0.")

  /**
   * 压缩多个旧事件日志为单个压缩文件，并清理已被压缩的旧日志文件
   * 
   * @param eventLogFiles 按索引排序的事件日志文件列表，如果已存在压缩文件，必须放在列表首位
   * @return 压缩结果，包含结果码和压缩文件索引（成功时返回）
   */
  def compact(eventLogFiles: Seq[FileStatus]): CompactionResult = {
    assertPrecondition(eventLogFiles)

    if (eventLogFiles.length < maxFilesToRetain) {
      return CompactionResult(CompactionResultCode.NOT_ENOUGH_FILES, None)
    }

    val filesToCompact = findFilesToCompact(eventLogFiles)
    if (filesToCompact.isEmpty) {
      CompactionResult(CompactionResultCode.NOT_ENOUGH_FILES, None)
    } else {
      val builders = initializeBuilders(fs, filesToCompact.map(_.getPath))

      val filters = builders.map(_.createFilter())
      val minScore = filters.flatMap(_.statistics()).map(calculateScore).min

      if (minScore < compactionThresholdScore) {
        CompactionResult(CompactionResultCode.LOW_SCORE_FOR_COMPACTION, None)
      } else {
        rewrite(filters, filesToCompact)
        cleanupCompactedFiles(filesToCompact)
        CompactionResult(CompactionResultCode.SUCCESS, Some(
          RollingEventLogFilesWriter.getEventLogFileIndex(filesToCompact.last.getPath.getName)))
      }
    }
  }

  // 检查输入文件列表是否符合前置条件：最多只能有一个已压缩文件，且必须放在列表首位
  private def assertPrecondition(eventLogFiles: Seq[FileStatus]): Unit = {
    val idxCompactedFiles = eventLogFiles.zipWithIndex.filter { case (file, _) =>
      EventLogFileWriter.isCompacted(file.getPath)
    }
    require(idxCompactedFiles.size < 2 && idxCompactedFiles.headOption.forall(_._2 == 0),
      "The number of compact files should be at most 1, and should be placed first if exists.")
  }

  /**
   * 通过ServiceLoader加载所有可用的EventFilterBuilder，并重放目标文件中的事件初始化Builder
   */
  private def initializeBuilders(fs: FileSystem, files: Seq[Path]): Seq[EventFilterBuilder] = {
    val bus = new ReplayListenerBus()

    val builders = ServiceLoader.load(classOf[EventFilterBuilder],
      Utils.getContextOrSparkClassLoader).asScala.toSeq
    builders.foreach(bus.addListener)

    files.foreach { log =>
      Utils.tryWithResource(EventLogFileReader.openEventLog(log, fs)) { in =>
        bus.replay(in, log.getName)
      }
    }

    builders
  }

  // 根据过滤统计信息计算压缩得分，得分越高表示可过滤掉的任务事件越多，压缩收益越大
  private def calculateScore(stats: FilterStatistics): Double = {
    // 当前简化计算：按被过滤的任务数占总任务数的比例计算得分
    // 后续可根据更多启发式信息优化该计算逻辑
    (stats.totalTasks - stats.liveTasks) * 1.0 / stats.totalTasks
  }

  /**
   * 根据过滤规则重写事件日志到单个压缩文件，仅保留所有过滤器接受或未识别的事件
   * 只有当所有过滤器都拒绝该事件时，才会丢弃该事件
   */
  private[history] def rewrite(
      filters: Seq[EventFilter],
      eventLogFiles: Seq[FileStatus]): String = {
    require(eventLogFiles.nonEmpty)

    val lastIndexEventLogPath = eventLogFiles.last.getPath
    val logWriter = new CompactedEventLogFileWriter(lastIndexEventLogPath, "dummy", None,
      lastIndexEventLogPath.getParent.toUri, sparkConf, hadoopConf)

    val startTime = System.currentTimeMillis()
    logWriter.start()
    eventLogFiles.foreach { file =>
      EventFilter.applyFilterToFile(fs, filters, file.getPath,
        onAccepted = (line, _) => logWriter.writeEvent(line, flushLogger = true),
        onRejected = (_, _) => {},
        onUnidentified = line => logWriter.writeEvent(line, flushLogger = true)
      )
    }
    logWriter.stop()
    val duration = System.currentTimeMillis() - startTime
    logInfo(log"Finished rewriting eventLog files to ${MDC(LogKeys.PATH, logWriter.logPath)}" +
      log" took ${MDC(LogKeys.TOTAL_TIME, duration)} ms.")

    logWriter.logPath
  }

  // 删除已被压缩的原始日志文件，删除失败记录警告并跳过
  private def cleanupCompactedFiles(files: Seq[FileStatus]): Unit = {
    files.foreach { file =>
      var deleted = false
      try {
        deleted = fs.delete(file.getPath, true)
      } catch {
        case _: IOException =>
      }
      if (!deleted) {
        logWarning(log"Failed to remove ${MDC(LogKeys.PATH, file.getPath)} / skip removing.")
      }
    }
  }

  // 确定需要压缩的文件范围：保留末尾maxFilesToRetain个文件，压缩前面所有的旧文件
  private def findFilesToCompact(eventLogFiles: Seq[FileStatus]): Seq[FileStatus] = {
    val numNormalEventLogFiles = {
      if (EventLogFileWriter.isCompacted(eventLogFiles.head.getPath)) {
        eventLogFiles.length - 1
      } else {
        eventLogFiles.length
      }
    }

    // 如果普通文件数量不超过保留数量，不需要压缩
    if (numNormalEventLogFiles > maxFilesToRetain) {
      eventLogFiles.dropRight(maxFilesToRetain)
    } else {
      Seq.empty
    }
  }
}

/**
 * 压缩结果封装类，存储压缩结果状态和压缩文件索引
 * 
 * @param code 压缩结果状态码
 * @param compactIndex 压缩成功时返回压缩文件索引，否则返回None
 */
case class CompactionResult(code: CompactionResultCode.Value, compactIndex: Option[Long])

/**
 * 压缩结果状态码枚举，定义所有可能的压缩结果状态
 */
object CompactionResultCode extends Enumeration {
  val SUCCESS, NOT_ENOUGH_FILES, LOW_SCORE_FOR_COMPACTION = Value
}

/**
 * 压缩文件写入器，继承SingleEventLogFileWriter复用现有写入逻辑，仅修改压缩文件路径生成规则
 */
private class CompactedEventLogFileWriter(
    originalFilePath: Path,
    appId: String,
    appAttemptId: Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SingleEventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf) {

  override val logPath: String = originalFilePath.toUri.toString + EventLogFileWriter.COMPACTED
}
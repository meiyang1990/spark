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

package org.apache.spark.scheduler

import java.io.{EOFException, InputStream, IOException}

import scala.io.{Codec, Source}

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.util.JsonProtocol

/**
 * 可用于从序列化的事件数据中回放事件的 SparkListenerBus。
 * 历史服务器使用此类从事件日志文件中重新播放 Spark 应用的执行历史。
 */
private[spark] class ReplayListenerBus extends SparkListenerBus with Logging {

  /**
   * 按给定流中维护的顺序回放每个事件。流中预期每行包含一个 JSON 编码的 SparkListenerEvent。
   *
   * 此方法可以多次调用，但在此方法抛出任何错误后，监听器的行为是未定义的。
   *
   * @param logData 包含事件日志数据的流
   * @param sourceName 读取 logData 的文件名（或其他来源标识）
   * @param maybeTruncated 指示日志文件是否可能被截断（某些异常情况下日志文件可能未完成写入）
   * @param eventsFilter 过滤函数，用于选择日志数据流中应被解析和回放的 JSON 事件字符串。
   *        未指定时，日志数据中的所有事件字符串都将被解析和回放。
   * @return 是否成功完整回放日志文件（无错误且无 HaltReplayException），否则返回 false
   */
  def replay(
      logData: InputStream,
      sourceName: String,
      maybeTruncated: Boolean = false,
      eventsFilter: ReplayEventsFilter = SELECT_ALL_FILTER): Boolean = {
    val lines = Source.fromInputStream(logData)(Codec.UTF8).getLines()
    replay(lines, sourceName, maybeTruncated, eventsFilter)
  }

  /**
   * Overloaded variant of [[replay()]] which accepts an iterator of lines instead of an
   * [[InputStream]]. Exposed for use by custom ApplicationHistoryProvider implementations.
   */
  def replay(
      lines: Iterator[String],
      sourceName: String,
      maybeTruncated: Boolean,
      eventsFilter: ReplayEventsFilter): Boolean = {
    var currentLine: String = null
    var lineNumber: Int = 0
    val unrecognizedEvents = new scala.collection.mutable.HashSet[String]
    val unrecognizedProperties = new scala.collection.mutable.HashSet[String]

    try {
      val lineEntries = lines
        .zipWithIndex
        .filter { case (line, _) => eventsFilter(line) }

      while (lineEntries.hasNext) {
        try {
          val entry = lineEntries.next()

          currentLine = entry._1
          lineNumber = entry._2 + 1

          postToAll(JsonProtocol.sparkEventFromJson(currentLine))
        } catch {
          case e: ClassNotFoundException =>
            // Ignore unknown events, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unknown event.
            if (!unrecognizedEvents.contains(e.getMessage)) {
              logWarning(log"Drop unrecognized event: ${MDC(ERROR, e.getMessage)}")
              unrecognizedEvents.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case e: UnrecognizedPropertyException =>
            // Ignore unrecognized properties, parse through the event log file.
            // To avoid spamming, warnings are only displayed once for each unrecognized property.
            if (!unrecognizedProperties.contains(e.getMessage)) {
              logWarning(log"Drop unrecognized property: ${MDC(ERROR, e.getMessage)}")
              unrecognizedProperties.add(e.getMessage)
            }
            logDebug(s"Drop incompatible event log: $currentLine")
          case jpe: JsonParseException =>
            // We can only ignore exception from last line of the file that might be truncated
            // the last entry may not be the very last line in the event log, but we treat it
            // as such in a best effort to replay the given input
            if (!maybeTruncated || lineEntries.hasNext) {
              throw jpe
            } else {
              logWarning(log"Got JsonParseException from log file ${MDC(FILE_NAME, sourceName)}" +
                log" at line ${MDC(LINE_NUM, lineNumber)}, " +
                log"the file might not have finished writing cleanly.")
            }
        }
      }
      true
    } catch {
      case e: HaltReplayException =>
        // Just stop replay.
        false
      case _: EOFException if maybeTruncated => false
      case ioe: IOException =>
        throw ioe
      case e: Exception =>
        logError(log"Exception parsing Spark event log: ${MDC(PATH, sourceName)}", e)
        logError(log"Malformed line #${MDC(LINE_NUM, lineNumber)}: ${MDC(LINE, currentLine)}\n")
        false
    }
  }

  override protected def isIgnorableException(e: Throwable): Boolean = {
    e.isInstanceOf[HaltReplayException]
  }

}

/**
 * Exception that can be thrown by listeners to halt replay. This is handled by ReplayListenerBus
 * only, and will cause errors if thrown when using other bus implementations.
 */
private[spark] class HaltReplayException extends RuntimeException

private[spark] object ReplayListenerBus {

  type ReplayEventsFilter = (String) => Boolean

  // utility filter that selects all event logs during replay
  val SELECT_ALL_FILTER: ReplayEventsFilter = { (eventString: String) => true }
}

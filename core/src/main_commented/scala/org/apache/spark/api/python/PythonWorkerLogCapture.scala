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

package org.apache.spark.api.python

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.storage.{PythonWorkerLogBlockIdGenerator, PythonWorkerLogLine, RollingLogWriter}

/**
 * 文件功能：PySpark Python工作进程日志捕获管理器，负责将Python UDF进程输出的日志捕获并写入存储，支持按进程轮转
 * 
 * 核心职责：解析Python工作进程输出流，根据进程PID路由日志到对应的轮转日志写入器，同时保留原输出流的接口行为，
 * 支持守护进程和非守护进程两种工作模式，将结构化日志存储到Spark块存储供后续查询使用。
 */
private[python] class PythonWorkerLogCapture(
    sessionId: String,
    logMarker: String = "PYTHON_WORKER_LOGGING") extends Logging {

  // 按工作进程ID存储日志写入器：key=workerId(PID), value=(轮转日志写入器, 行序列号计数器)
  private val workerLogWriters = new ConcurrentHashMap[String, (RollingLogWriter, AtomicLong)]()

  /**
   * 包装输入流，添加Python日志捕获能力
   * 
   * @param inputStream 原始输入流，通常是Python工作进程的标准输出流
   * @return 包装后带有日志捕获功能的输入流
   */
  def wrapInputStream(inputStream: InputStream): InputStream = {
    new CaptureWorkerLogsInputStream(inputStream)
  }

  /**
   * 移除并关闭指定工作进程的日志写入器
   * 
   * @param workerId 工作进程ID，通常是PID的字符串形式
   */
  def removeAndCloseWorkerLogWriter(workerId: String): Unit = {
    Option(workerLogWriters.remove(workerId)).foreach { case (writer, _) =>
      try {
        writer.close()
      } catch {
        case e: Exception =>
          logWarning(
            log"Failed to close log writer for worker ${MDC(LogKeys.PYTHON_WORKER_ID, workerId)}",
            e)
      }
    }
  }

  /**
   * 关闭所有活跃的工作进程日志写入器，清理资源
   */
  def closeAllWriters(): Unit = {
    workerLogWriters.asScala.foreach { case (workerId, (writer, _)) =>
      try {
        writer.close()
      } catch {
        case e: Exception =>
          logWarning(
            log"Failed to close log writer for worker ${MDC(LogKeys.PYTHON_WORKER_ID, workerId)}",
            e)
      }
    }
    workerLogWriters.clear()
  }

  /**
   * 获取或创建指定工作进程的日志写入器
   * 
   * @param workerId 工作进程唯一标识，通常是PID
   * @return (轮转日志写入器, 行序列号原子计数器)
   */
  private def getOrCreateLogWriter(workerId: String): (RollingLogWriter, AtomicLong) = {
    workerLogWriters.computeIfAbsent(workerId, _ => {
      val logWriter = SparkEnv.get.blockManager.getRollingLogWriter(
        new PythonWorkerLogBlockIdGenerator(sessionId, workerId)
      )
      (logWriter, new AtomicLong())
    })
  }

  /**
   * 处理从Python工作进程读取到的单行日志
   * 
   * @param line 包含日志标记和JSON内容的完整行
   * @return 需要透传给上层的非日志前缀内容
   */
  private def processLogLine(line: String): String = {
    val markerIndex = line.indexOf(s"$logMarker:")
    if (markerIndex >= 0) {
      val prefix = line.substring(0, markerIndex)
      val markerAndJson = line.substring(markerIndex)

      // 解析格式: "日志标记:工作进程PID:JSON日志内容"
      val parts = markerAndJson.split(":", 3)
      if (parts.length >= 3) {
        val workerId = parts(1) // Python工作进程PID
        val json = parts(2)

        try {
          if (json.isEmpty) {
            // 空JSON表示工作进程退出，清理对应日志写入器
            removeAndCloseWorkerLogWriter(workerId)
          } else {
            val (writer, seqId) = getOrCreateLogWriter(workerId)
            writer.writeLog(
              PythonWorkerLogLine(System.currentTimeMillis(), seqId.getAndIncrement(), json)
            )
          }
        } catch {
          case e: Exception =>
            logWarning(
              log"Failed to write log for worker ${MDC(LogKeys.PYTHON_WORKER_ID, workerId)}", e)
        }
      }
      prefix
    } else {
      // 不含日志标记，直接透传整行
      line + System.lineSeparator()
    }
  }

  /**
   * 输入流包装类，拦截读取过程并解析捕获Python工作进程输出的结构化日志
   */
  private class CaptureWorkerLogsInputStream(in: InputStream) extends InputStream {

    private[this] val reader = new BufferedReader(
      new InputStreamReader(in, StandardCharsets.ISO_8859_1))
    private[this] val temp = new Array[Byte](1)
    private[this] var buffer = ByteBuffer.allocate(0)

    override def read(): Int = {
      val n = read(temp)
      if (n <= 0) {
        -1
      } else {
        // 将有符号字节转换为无符号整数返回
        temp(0) & 0xff
      }
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      if (buffer.hasRemaining) {
        // 缓冲区还有剩余数据，填充到目标数组返回
        val buf = ByteBuffer.wrap(b, off, len)
        val remaining = Math.min(buffer.remaining(), buf.remaining())
        buf.put(buf.position(), buffer, buffer.position(), remaining)
        buffer.position(buffer.position() + remaining)
        remaining
      } else {
        // 缓冲区已空，读取一行新数据
        val line = reader.readLine()
        if (line == null) {
          // 流已结束，关闭所有日志写入器
          closeAllWriters()
          -1
        } else {
          val processedContent = if (line.contains(s"$logMarker:")) {
            // 包含日志标记，处理后提取前缀透传
            processLogLine(line)
          } else {
            // 无日志标记，直接透传整行
            line + System.lineSeparator()
          }

          // 将处理后内容写入缓冲区，递归调用读取填充目标数组
          buffer = ByteBuffer.wrap(processedContent.getBytes(StandardCharsets.ISO_8859_1))
          read(b, off, len)
        }
      }
    }

    override def close(): Unit = {
      try {
        reader.close()
      } finally {
        closeAllWriters()
      }
    }
  }
}
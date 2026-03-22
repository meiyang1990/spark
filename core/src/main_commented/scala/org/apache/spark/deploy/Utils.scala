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

package org.apache.spark.deploy

import java.io.File

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{LOG_TYPE, PATH}
import org.apache.spark.ui.JettyUtils.createServletHandler
import org.apache.spark.ui.WebUI
import org.apache.spark.util.Utils.{getFileLength, offsetBytes}
import org.apache.spark.util.logging.RollingFileAppender

/**
 * 文件级注释：Spark部署模块的工具类，提供WebUI日志查看相关的工具方法
 * 为Spark Standalone部署模式的WebUI提供日志文件读取和分页展示能力
 */
private[deploy] object Utils extends Logging {
  // 默认单次读取日志的字节大小，100KB
  val DEFAULT_BYTES = 100 * 1024
  // 支持查看的日志类型集合
  val SUPPORTED_LOG_TYPES = Set("stderr", "stdout", "out")

  /**
   * 为WebUI页面添加日志查看处理器，处理/log路径的日志请求
   * @param page 目标WebUI页面
   * @param conf Spark配置对象
   */
  def addRenderLogHandler(page: WebUI, conf: SparkConf): Unit = {
    page.attachHandler(createServletHandler("/log",
      (request: HttpServletRequest) => renderLog(request, conf),
      conf))
  }

  /**
   * 处理HTTP日志请求，读取指定范围的日志内容并返回响应文本
   * @param request HTTP请求对象
   * @param conf Spark配置对象
   * @return 格式化后的日志文本，包含范围信息和日志内容
   */
  private def renderLog(request: HttpServletRequest, conf: SparkConf): String = {
    // 从环境变量获取日志根目录，默认使用logs/
    val logDir = sys.env.getOrElse("SPARK_LOG_DIR", "logs/")
    // 从请求参数获取要查看的日志类型
    val logType = request.getParameter("logType")
    // 从请求参数获取起始偏移量
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    // 从请求参数获取读取长度，默认使用DEFAULT_BYTES
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt)
      .getOrElse(DEFAULT_BYTES)

    // 获取指定范围的日志内容及位置信息
    val (logText, startByte, endByte, logLength) = getLog(conf, logDir, logType, offset, byteLength)
    // 添加日志范围头部信息
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  /** Get the part of the log files given the offset and desired length of bytes */
  /**
   * 根据偏移量和字节长度读取日志文件的指定部分，支持滚动日志文件
   * @param conf Spark配置对象
   * @param logDirectory 日志所在目录
   * @param logType 要读取的日志类型
   * @param offsetOption 起始字节偏移量，None则读取末尾
   * @param byteLength 要读取的字节长度
   * @return 元组(日志内容, 起始偏移, 结束偏移, 总日志长度)，错误时返回错误信息
   */
  def getLog(
      conf: SparkConf,
      logDirectory: String,
      logType: String,
      offsetOption: Option[Long],
      byteLength: Int): (String, Long, Long, Long) = {
    // 检查日志类型是否支持
    if (!SUPPORTED_LOG_TYPES.contains(logType)) {
      return ("Error: Log type must be one of " + SUPPORTED_LOG_TYPES.mkString(", "), 0, 0, 0)
    }
    try {
      // 定位日志文件
      val fileName = if (logType.equals("out")) {
        // out类型日志需要自动匹配目录下的.out文件
        val normalizedUri = new File(logDirectory).toURI.normalize()
        val normalizedLogDir = new File(normalizedUri.getPath)
        normalizedLogDir.listFiles.map(_.getName).filter(_.endsWith(".out"))
          .headOption.getOrElse(logType)
      } else {
        logType
      }
      // 获取排序后的滚动日志文件列表（旧文件在前，新文件在后）
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, fileName)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      // 计算每个日志文件的长度
      val fileLengths: Seq[Long] = files.map(getFileLength(_, conf))
      // 计算所有滚动日志的总长度
      val totalLength = fileLengths.sum
      // 确定起始偏移量，未指定则从末尾往前读取
      val offset = offsetOption.getOrElse(totalLength - byteLength)
      // 边界检查，限制起始偏移在合法范围内
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      // 计算结束偏移，不超过总长度
      val endIndex = math.min(startIndex + byteLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      // 从多个滚动日志中读取指定范围的字节内容
      val logText = offsetBytes(files, fileLengths, startIndex, endIndex)
      logDebug(s"Got log of length ${logText.length} bytes")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        // 捕获异常，记录错误日志并返回错误信息
        logError(log"Error getting ${MDC(LOG_TYPE, logType)} logs from " +
          log"directory ${MDC(PATH, logDirectory)}", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}
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

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.matching.Regex

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys

/**
 * Executor 日志 URL 处理器，支持自定义日志 URL 模式。
 *
 * 【背景】
 * 在 YARN、Kubernetes 等不同集群管理器上，Executor 日志的访问方式各不相同。
 * 为了统一日志访问体验，Spark 支持通过自定义 URL 模式重写日志链接。
 *
 * 【URL 模式语法】
 * 使用 {{ATTRIBUTE_NAME}} 作为占位符，支持的属性包括：
 * - {{FILE_NAME}}：日志文件名（如 stdout、stderr）
 * - {{APP_ID}}：应用 ID
 * - {{CONTAINER_ID}}：容器 ID（YARN 环境）
 * - {{CLUSTER_ID}}：集群 ID
 * - 其他由资源管理器提供的自定义属性
 *
 * 【使用示例】
 * 配置：spark.history.custom.executor.log.url = http://logs.example.com/{{APP_ID}}/{{FILE_NAME}}
 * 输入属性：{APP_ID -> "app-123", LOG_FILES -> "stdout,stderr"}
 * 输出：{stdout -> "http://logs.example.com/app-123/stdout",
 *        stderr -> "http://logs.example.com/app-123/stderr"}
 *
 * @param logUrlPattern 自定义日志 URL 模式，None 表示使用原始 URL
 */
private[spark] class ExecutorLogUrlHandler(logUrlPattern: Option[String]) extends Logging {
  import ExecutorLogUrlHandler._

  // 确保缺失属性的警告只打印一次
  private val informedForMissingAttributes = new AtomicBoolean(false)

  /**
   * 应用 URL 模式生成最终的日志 URL 映射。
   *
   * @param logUrls 原始日志 URL 映射（文件名 -> URL）
   * @param attributes 可用的属性映射（属性名 -> 值）
   * @return 处理后的日志 URL 映射
   */
  def applyPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String]): Map[String, String] = {
    logUrlPattern match {
      case Some(pattern) => doApplyPattern(logUrls, attributes, pattern)
      case None => logUrls
    }
  }

  /**
   * 执行实际的模式替换逻辑。
   * 1. 提取模式中的所有占位符
   * 2. 检查必需的属性是否存在
   * 3. 用属性值替换占位符
   * 4. 如果包含 FILE_NAME，为每个日志文件生成独立的 URL
   */
  private def doApplyPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String],
      urlPattern: String): Map[String, String] = {
    // FILE_NAME 和 LOG_FILES 的特殊关系：
    // - FILE_NAME 是 URL 模式中的占位符
    // - LOG_FILES 是属性中提供的可用日志文件列表（逗号分隔）
    val allPatterns = CUSTOM_URL_PATTERN_REGEX.findAllMatchIn(urlPattern).map(_.group(1)).toSet
    val allPatternsExceptFileName = allPatterns.filter(_ != "FILE_NAME")
    val allAttributeKeys = attributes.keySet
    val allAttributeKeysExceptLogFiles = allAttributeKeys.filter(_ != "LOG_FILES")

    // 检查是否所有必需的属性都存在
    if (allPatternsExceptFileName.diff(allAttributeKeysExceptLogFiles).nonEmpty) {
      logFailToRenewLogUrls("some of required attributes are missing in app's event log.",
        allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else if (allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES")) {
      logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
        "missing in app's event log.", allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else {
      // 先替换除 FILE_NAME 外的所有占位符
      val updatedUrl = allPatternsExceptFileName.foldLeft(urlPattern) { case (orig, patt) =>
        orig.replace(s"{{$patt}}", attributes(patt))
      }

      // 如果包含 FILE_NAME，为每个日志文件生成独立的 URL
      if (allPatterns.contains("FILE_NAME")) {
        attributes("LOG_FILES").split(",").map { file =>
          file -> updatedUrl.replace("{{FILE_NAME}}", file)
        }.toMap
      } else {
        Map("log" -> updatedUrl)
      }
    }
  }

  // 记录 URL 重写失败的原因（只记录一次，避免日志刷屏）
  private def logFailToRenewLogUrls(
      reason: String,
      allPatterns: Set[String],
      allAttributes: Set[String]): Unit = {
    if (informedForMissingAttributes.compareAndSet(false, true)) {
      logInfo(log"Fail to renew executor log urls: ${MDC(LogKeys.REASON, reason)}." +
        log" Required: ${MDC(LogKeys.REGEX, allPatterns)} / " +
        log"available: ${MDC(LogKeys.ATTRIBUTE_MAP, allAttributes)}." +
        log" Falling back to show app's original log urls.")
    }
  }
}

/**
 * ExecutorLogUrlHandler 伴生对象
 */
private[spark] object ExecutorLogUrlHandler {
  // 匹配 {{ATTRIBUTE_NAME}} 格式的正则表达式
  val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
}

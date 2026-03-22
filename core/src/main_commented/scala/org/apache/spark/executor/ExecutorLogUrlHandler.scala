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

package org.apache.spark.executor

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.matching.Regex

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys

/**
 * 文件级注释：Executor 日志URL处理器，核心功能是根据用户自定义的URL模式生成标准化的Executor日志访问链接，
 * 适配YARN、Kubernetes等不同集群管理器的日志访问路径，统一Spark历史服务器的日志展示体验。
 *
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

  // 确保缺失属性的警告只打印一次，避免重复刷屏
  private val informedForMissingAttributes = new AtomicBoolean(false)

  /**
   * 应用自定义URL模式生成最终的日志URL映射，对外暴露的入口方法。
   *
   * @param logUrls 原始日志URL映射（键为日志文件名，值为原始URL）
   * @param attributes 用于替换占位符的属性键值对
   * @return 处理后的日志URL映射
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
   * 执行实际的URL模式替换与生成逻辑，核心业务方法。
   * 执行流程：提取占位符 -> 检查属性完整性 -> 替换公共占位符 -> 按日志文件生成最终URL
   *
   * @param logUrls 原始日志URL映射
   * @param attributes 占位符属性键值对
   * @param urlPattern 用户配置的自定义URL模式
   * @return 处理后的日志URL映射
   */
  private def doApplyPattern(
      logUrls: Map[String, String],
      attributes: Map[String, String],
      urlPattern: String): Map[String, String] = {
    // 提取URL模式中所有占位符
    val allPatterns = CUSTOM_URL_PATTERN_REGEX.findAllMatchIn(urlPattern).map(_.group(1)).toSet
    // 过滤出除FILE_NAME外的所有占位符（FILE_NAME需单独处理）
    val allPatternsExceptFileName = allPatterns.filter(_ != "FILE_NAME")
    // 获取所有可用属性键
    val allAttributeKeys = attributes.keySet
    // 过滤出除LOG_FILES外的所有属性键（LOG_FILES仅配合FILE_NAME使用）
    val allAttributeKeysExceptLogFiles = allAttributeKeys.filter(_ != "LOG_FILES")

    // 检查是否所有非FILE_NAME的必需属性都存在
    if (allPatternsExceptFileName.diff(allAttributeKeysExceptLogFiles).nonEmpty) {
      logFailToRenewLogUrls("some of required attributes are missing in app's event log.",
        allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else if (allPatterns.contains("FILE_NAME") && !allAttributeKeys.contains("LOG_FILES")) {
      // 模式包含FILE_NAME但属性中没有提供日志文件列表，失败回退
      logFailToRenewLogUrls("'FILE_NAME' parameter is provided, but file information is " +
        "missing in app's event log.", allPatternsExceptFileName, allAttributeKeys)
      logUrls
    } else {
      // 折叠替换所有非FILE_NAME占位符，生成基础URL
      val updatedUrl = allPatternsExceptFileName.foldLeft(urlPattern) { case (orig, patt) =>
        orig.replace(s"{{$patt}}", attributes(patt))
      }

      // 如果模式包含FILE_NAME占位符，为每个日志文件生成独立URL
      if (allPatterns.contains("FILE_NAME")) {
        attributes("LOG_FILES").split(",").map { file =>
          file -> updatedUrl.replace("{{FILE_NAME}}", file)
        }.toMap
      } else {
        // 不包含FILE_NAME，直接返回单条统一URL
        Map("log" -> updatedUrl)
      }
    }
  }

  // 记录URL重写失败日志，通过AtomicBoolean保证同一次应用生命周期只打印一次警告，避免日志污染
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
 * 伴生对象，存放正则表达式等公共静态常量
 */
private[spark] object ExecutorLogUrlHandler {
  // 匹配 {{ATTRIBUTE_NAME}} 格式占位符的正则表达式，捕获属性名分组
  val CUSTOM_URL_PATTERN_REGEX: Regex = "\\{\\{([A-Za-z0-9_\\-]+)\\}\\}".r
}
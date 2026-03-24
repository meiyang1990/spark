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

/**
 * 文件: CausedBy.scala
 * 所属模块: Spark core 公共基础工具模块
 * 核心职责: 提供异常根原因提取的模式匹配提取器，简化在catch语句中匹配异常根本原因的代码
 *
 * Extractor Object for pulling out the root cause of an error.
 * If the error contains no cause, it will return the error itself.
 *
 * Usage:
 * try {
 *   ...
 * } catch {
 *   case CausedBy(ex: CommitDeniedException) => ...
 * }
 */
private[spark] object CausedBy {

  /**
   * 模式匹配提取方法，递归提取异常链最底层的根异常
   * @param e 输入的待提取根原因的异常对象
   * @return 包含根异常的Option，如果存在根异常则返回根异常，否则返回原始异常
   */
  def unapply(e: Throwable): Option[Throwable] = {
    Option(e.getCause).flatMap(cause => unapply(cause)).orElse(Some(e))
  }
}
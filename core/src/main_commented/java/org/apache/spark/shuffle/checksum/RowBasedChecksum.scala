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

package org.apache.spark.shuffle.checksum

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * 基于行的Shuffle数据校验和计算器，计算结果与输入(key, value)对的顺序无关
 * 计算流程：先计算每一行的校验和，再对所有行校验和执行异或与累加操作，最后混合两个结果得到最终校验和
 */
abstract class RowBasedChecksum() extends Serializable with Logging {
  // 循环左移位数，用于混合异或和累加结果
  private val ROTATE_POSITIONS = 27
  // 标记计算过程是否发生错误
  private var hasError: Boolean = false
  // 所有行校验和的异或结果
  private var checksumXor: Long = 0
  // 所有行校验和的累加结果
  private var checksumSum: Long = 0

  /**
   * 获取最终计算得到的校验和
   * 如果计算过程发生错误，返回默认值0
   * @return 混合后的最终校验和
   */
  def getValue: Long = {
    if (!hasError) {
      // 对累加结果循环左移后与异或结果再次异或，充分混合两个值得到最终校验和
      checksumXor ^ rotateLeft(checksumSum)
    } else {
      0
    }
  }

  /**
   * 使用新的(key, value)对更新基于行的校验和，非线程安全
   * @param key 输入键
   * @param value 输入值
   */
  def update(key: Any, value: Any): Unit = {
    if (!hasError) {
      try {
        // 计算当前行的校验和
        val rowChecksumValue = calculateRowChecksum(key, value)
        // 更新全局异或结果
        checksumXor = checksumXor ^ rowChecksumValue
        // 更新全局累加结果
        checksumSum += rowChecksumValue
      } catch {
        case NonFatal(e) =>
          logError("Checksum computation encountered error: ", e)
          hasError = true
      }
    }
  }

  /**
   * 计算单个(key, value)对的行校验和，由子类实现具体计算逻辑
   * @param key 输入键
   * @param value 输入值
   * @return 当前行的校验和
   */
  protected def calculateRowChecksum(key: Any, value: Any): Long

  /**
   * 对64位长整型执行循环左移操作
   * @param value 输入长整型值
   * @return 循环左移后的结果
   */
  private def rotateLeft(value: Long): Long = {
    (value << ROTATE_POSITIONS) | (value >>> (64 - ROTATE_POSITIONS))
  }
}

/**
 * RowBasedChecksum工具类，提供多个分区校验和聚合计算能力
 */
object RowBasedChecksum {
  /**
   * 聚合多个分区的行校验和得到全局校验和
   * @param rowBasedChecksums 所有分区的行校验和数组
   * @return 聚合后的全局校验和
   */
  def getAggregatedChecksumValue(rowBasedChecksums: Array[RowBasedChecksum]): Long = {
    Option(rowBasedChecksums)
      .map(_.foldLeft(0L)((acc, c) => acc * 31L + c.getValue))
      .getOrElse(0L)
  }
}
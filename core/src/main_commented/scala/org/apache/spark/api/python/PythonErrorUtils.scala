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

import java.util

import org.apache.spark.{BreakingChangeInfo, QueryContext, SparkThrowable}

/**
 * 从 SparkThrowable 实例提取结构化错误信息的工具类
 * 
 * 该工具类专为 PySpark 设计，用于获取错误的结构化元数据，
 * 解决了 Py4J 无法处理接口默认方法的兼容性问题
 */
private[spark] object PythonErrorUtils {
  /** 获取错误条件码 */
  def getCondition(e: SparkThrowable): String = e.getCondition
  /** 获取错误类标识（与getCondition语义一致，兼容旧版本） */
  def getErrorClass(e: SparkThrowable): String = e.getCondition
  /** 获取SQL标准错误状态码 */
  def getSqlState(e: SparkThrowable): String = e.getSqlState
  /** 判断是否为内部系统错误 */
  def isInternalError(e: SparkThrowable): Boolean = e.isInternalError
  /** 获取破坏性变更信息 */
  def getBreakingChangeInfo(e: SparkThrowable): BreakingChangeInfo = e.getBreakingChangeInfo
  /** 获取错误消息占位符参数映射 */
  def getMessageParameters(e: SparkThrowable): util.Map[String, String] = e.getMessageParameters
  /** 获取默认错误消息模板 */
  def getDefaultMessageTemplate(e: SparkThrowable): String = e.getDefaultMessageTemplate
  /** 获取查询上下文信息数组 */
  def getQueryContext(e: SparkThrowable): Array[QueryContext] = e.getQueryContext
}
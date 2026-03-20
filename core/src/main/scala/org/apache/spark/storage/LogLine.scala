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

package org.apache.spark.storage

import scala.reflect.ClassTag
import scala.reflect.classTag

/**
 * 日志行的基类
 *
 * @param eventTime 日志写入时的时间戳（毫秒）
 * @param sequenceId 日志行的序列 ID
 * @param message 日志消息内容
 */
trait LogLine {
  val eventTime: Long
  val sequenceId: Long
  val message: String
}

object LogLine {
  // 根据日志块类型获取对应的 ClassTag
  def getClassTag(logBlockType: LogBlockType.LogBlockType): ClassTag[_<:LogLine] =
    logBlockType match {
      case LogBlockType.TEST =>
        classTag[TestLogLine]
      case LogBlockType.PYTHON_WORKER =>
        classTag[PythonWorkerLogLine]
      case unsupportedLogBlockType =>
        throw new RuntimeException("Not supported log type " + unsupportedLogBlockType)
    }
}

// 测试日志行
case class TestLogLine(eventTime: Long, sequenceId: Long, message: String)
  extends LogLine {
}

// Python Worker 日志行
case class PythonWorkerLogLine(eventTime: Long, sequenceId: Long, message: String)
  extends LogLine

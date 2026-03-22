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

package org.apache.spark.deploy.rest

import scala.util.Try

import org.apache.spark.internal.config
import org.apache.spark.util.Utils

/**
 * 文件说明: 定义REST应用提交协议中所有请求的基类和具体实现，处理提交请求的参数解析与验证
 */

/**
 * REST应用提交协议中客户端请求的抽象基类，定义所有请求的公共结构与验证逻辑
 */
private[rest] abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  var clientSparkVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(clientSparkVersion, "clientSparkVersion")
  }
}

/**
 * REST应用提交协议中启动新应用的请求，封装提交应用所需的所有参数，并提供参数类型验证逻辑
 */
private[rest] class CreateSubmissionRequest extends SubmitRestProtocolRequest {
  var appResource: String = null
  var mainClass: String = null
  var appArgs: Array[String] = null
  var sparkProperties: Map[String, String] = null
  var environmentVariables: Map[String, String] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assert(sparkProperties != null, "No Spark properties set!")
    assertFieldIsSet(appResource, "appResource")
    assertPropertyIsBoolean(config.DRIVER_SUPERVISE.key)
    assertPropertyIsNumeric(config.DRIVER_CORES.key)
    assertPropertyIsNumeric(config.CORES_MAX.key)
    assertPropertyIsMemory(config.DRIVER_MEMORY.key)
    assertPropertyIsMemory(config.EXECUTOR_MEMORY.key)
  }

  /** 验证指定配置属性是否为布尔类型 */
  private def assertPropertyIsBoolean(key: String): Unit =
    assertProperty[Boolean](key, "boolean", _.toBoolean)

  /** 验证指定配置属性是否为数值类型 */
  private def assertPropertyIsNumeric(key: String): Unit =
    assertProperty[Double](key, "numeric", _.toDouble)

  /** 验证指定内存配置属性是否为合法内存格式，转换为MB单位 */
  private def assertPropertyIsMemory(key: String): Unit =
    assertProperty[Int](key, "memory", Utils.memoryStringToMb)

  /** 
   * 通用验证方法：验证指定Spark配置属性可以转换为目标类型
   * @param key 配置属性键名
   * @param valueType 期望的值类型描述
   * @param convert 类型转换函数
   * @tparam T 目标类型
   */
  private def assertProperty[T](key: String, valueType: String, convert: (String => T)): Unit = {
    sparkProperties.get(key).foreach { value =>
      Try(convert(value)).getOrElse {
        throw new SubmitRestProtocolException(
          s"Property '$key' expected $valueType value: actual was '$value'.")
      }
    }
  }
}
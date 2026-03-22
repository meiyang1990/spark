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

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.util.Utils

/**
 * 文件级别注释：REST应用提交协议消息抽象定义
 * 本文件定义了Spark Standalone REST提交协议中交互消息的基础抽象类和工具方法，
 * 负责消息的JSON序列化、反序列化和合法性校验，是所有提交请求/响应消息的基类
 */

/**
 * REST应用提交协议中交换消息的抽象基类
 * 所有协议消息都会被序列化为JSON在客户端和服务器之间传输，
 * 包含三个通用字段：动作类型、Spark版本、可选描述信息，
 * 提供消息序列化和合法性校验能力
 */
@JsonInclude(Include.NON_ABSENT)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
private[rest] abstract class SubmitRestProtocolMessage {
  @JsonIgnore
  val messageType = Utils.getFormattedClassName(this)

  val action: String = messageType
  var message: String = null

  // 供JSON反序列化使用的空实现方法
  private def setAction(a: String): Unit = { }

  /**
   * 将当前消息序列化为JSON字符串
   * 序列化前会先对消息字段进行合法性校验
   * @return 序列化后的JSON字符串
   */
  def toJson: String = {
    validate()
    SubmitRestProtocolMessage.mapper.writeValueAsString(this)
  }

  /**
   * 执行消息合法性校验，捕获校验过程中的异常并包装为协议异常
   * 校验失败会抛出SubmitRestProtocolException
   */
  final def validate(): Unit = {
    try {
      doValidate()
    } catch {
      case e: Exception =>
        throw new SubmitRestProtocolException(s"Validation of message $messageType failed!", e)
    }
  }

  /**
   * 消息合法性校验的具体实现
   * 子类可以覆盖此方法扩展自定义校验逻辑
   */
  protected def doValidate(): Unit = {
    if (action == null) {
      throw new SubmitRestMissingFieldException(s"The action field is missing in $messageType")
    }
  }

  /**
   * 校验指定字段在消息中已被设置
   * @param value 字段值
   * @param name 字段名称，用于异常描述
   */
  protected def assertFieldIsSet[T](value: T, name: String): Unit = {
    if (value == null) {
      throw new SubmitRestMissingFieldException(s"'$name' is missing in message $messageType.")
    }
  }

  /**
   * 校验自定义条件，校验失败抛出协议异常
   * @param condition 需要满足的条件
   * @param failMessage 校验失败的描述信息
   */
  protected def assert(condition: Boolean, failMessage: String): Unit = {
    if (!condition) { throw new SubmitRestProtocolException(failMessage) }
  }
}

/**
 * 处理SubmitRestProtocolMessage序列化与反序列化的工具对象
 * 提供从JSON解析消息、获取动作类型等能力，是REST提交协议的消息处理入口
 */
private[spark] object SubmitRestProtocolMessage {
  private val packagePrefix = this.getClass.getPackage.getName
  // 初始化Jackson JSON对象映射器，配置反序列化忽略未知属性、输出格式化JSON，注册Scala模块支持Scala类型
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  /**
   * 从给定JSON中解析出action字段的值
   * 如果找不到action字段则抛出缺失字段异常
   * @param json 输入JSON字符串
   * @return 解析得到的action值
   */
  def parseAction(json: String): String = {
    val value: Option[String] = parse(json) match {
      case JObject(fields) =>
        fields.collectFirst { case ("action", v) => v }.collect { case JString(s) => s }
      case _ => None
    }
    value.getOrElse {
      throw new SubmitRestMissingFieldException(s"Action field not found in JSON:\n$json")
    }
  }

  /**
   * 从JSON字符串构造消息对象
   * 先解析action字段推断消息类型，要求action对应本包下已定义的消息类，否则抛出类找不到异常
   * @param json 输入JSON字符串
   * @return 反序列化得到的消息对象
   */
  def fromJson(json: String): SubmitRestProtocolMessage = {
    val className = parseAction(json)
    val clazz = Utils.classForName(packagePrefix + "." + className)
      .asSubclass[SubmitRestProtocolMessage](classOf[SubmitRestProtocolMessage])
    fromJson(json, clazz)
  }

  /**
   * 根据指定消息类型从JSON构造消息对象
   * 不需要通过action推断类型，适用于自定义用户消息的反序列化场景
   * @param json 输入JSON字符串
   * @param clazz 目标消息类的Class对象
   * @return 反序列化得到的消息对象
   */
  def fromJson[T <: SubmitRestProtocolMessage](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz)
  }
}
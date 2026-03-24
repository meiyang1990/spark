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
package org.apache.spark.status.api.v1

import java.io.OutputStream
import java.lang.annotation.Annotation
import java.lang.reflect.Type
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, SimpleTimeZone}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.{MediaType, MultivaluedMap}
import jakarta.ws.rs.ext.{MessageBodyWriter, Provider}

/**
 * 文件说明：Spark REST API v1 模块的 JSON 序列化实现，基于 Jackson 库将POJO指标响应对象序列化为JSON格式
 *
 * 设计原因：不使用标准 jersey-jackson 插件，避免引入额外依赖，兼容 YARN 自带的旧版本 Jersey
 * 自动发现：Jersey 会基于包路径和注解自动发现并注册这个类
 */

/**
 * 基于Jackson实现的Jersey消息体编写器，用于将Spark状态API响应对象序列化为JSON
 * 负责将API返回的POJO指标数据转换为JSON格式输出
 */
@Provider
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class JacksonMessageWriter extends MessageBodyWriter[Object]{

  // 创建定制化Jackson对象映射器，支持Scala类型序列化
  val mapper = new ObjectMapper() {
    override def writeValueAsString(t: Any): String = {
      super.writeValueAsString(t)
    }
  }
  // 注册Scala模块，支持Scala集合等数据类型序列化
  mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
  // 开启输出缩进格式化，便于阅读API返回结果
  mapper.enable(SerializationFeature.INDENT_OUTPUT)
  // 排除值为absent的属性不输出
  mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_ABSENT)
  // 设置ISO标准日期格式，统一日期序列化格式
  mapper.setDateFormat(JacksonMessageWriter.makeISODateFormat)

  /**
   * 判断当前类型是否可由该编写器处理，所有响应对象都允许处理
   * @return 始终返回true，表示所有类型都可处理
   */
  override def isWriteable(
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Boolean = {
      true
  }

  /**
   * 将对象序列化为JSON并写入输出流
   * @param t 待序列化的响应对象
   * @param outputStream 响应输出流
   */
  override def writeTo(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType,
      multivaluedMap: MultivaluedMap[String, AnyRef],
      outputStream: OutputStream): Unit = {
    mapper.writeValue(outputStream, t)
  }

  /**
   * 获取内容长度，返回-1表示不预先不知道长度，由底层自动处理
   * @return 始终返回-1
   */
  override def getSize(
      t: Object,
      aClass: Class[_],
      `type`: Type,
      annotations: Array[Annotation],
      mediaType: MediaType): Long = {
    -1L
  }
}

/**
 * JacksonMessageWriter 的工具对象，提供日期格式化工具
 */
private[spark] object JacksonMessageWriter {
  /**
   * 创建符合ISO8601标准的GMT时区日期格式化器，用于日期序列化
   * @return 配置好的ISO8601日期格式化实例
   */
  def makeISODateFormat: SimpleDateFormat = {
    val iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'", Locale.US)
    val cal = Calendar.getInstance(new SimpleTimeZone(0, "GMT"))
    iso8601.setCalendar(cal)
    iso8601
  }
}
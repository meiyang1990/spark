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

package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.http.HttpServletRequest
import org.eclipse.jetty.ee10.servlet.ServletContextHandler

import org.apache.spark.SparkConf
import org.apache.spark.ui.JettyUtils._

/**
 * 指标HTTP服务Sink，提供基于Jetty的HTTP接口查询Spark全量指标数据
 * 作为Spark监控指标的输出方式，允许外部系统通过HTTP接口拉取当前节点的所有指标
 * @param property 配置属性，包含Servlet路径和采样配置
 * @param registry 指标注册表，包含所有需要暴露的Spark指标
 */
private[spark] class MetricsServlet(
    val property: Properties, val registry: MetricRegistry) extends Sink {

  // 配置项键：Servlet访问路径
  val SERVLET_KEY_PATH = "path"
  // 配置项键：是否开启指标采样展示
  val SERVLET_KEY_SAMPLE = "sample"
  // 采样配置默认值：不开启采样
  val SERVLET_DEFAULT_SAMPLE = false
  // 从配置中读取Servlet访问路径
  val servletPath = property.getProperty(SERVLET_KEY_PATH)
  // 解析采样配置，默认不开启
  val servletShowSample = Option(property.getProperty(SERVLET_KEY_SAMPLE)).map(_.toBoolean)
    .getOrElse(SERVLET_DEFAULT_SAMPLE)
  // 初始化JSON序列化器，注册Metrics模块用于指标序列化
  val mapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, servletShowSample))

  /**
   * 获取Jetty Servlet处理器，注册到Jetty服务提供HTTP访问入口
   * @param conf Spark配置
   * @return 包装好的Servlet上下文处理器数组
   */
  def getHandlers(conf: SparkConf): Array[ServletContextHandler] = {
    Array[ServletContextHandler](
      createServletHandler(servletPath,
        new ServletParams(request => getMetricsSnapshot(request), "text/json"), conf)
    )
  }

  /**
   * 获取当前所有指标的JSON序列化结果，处理HTTP请求返回指标数据
   * @param request HTTP请求对象
   * @return 全量指标的JSON字符串
   */
  def getMetricsSnapshot(request: HttpServletRequest): String = {
    mapper.writeValueAsString(registry)
  }

  override def start(): Unit = { }

  override def stop(): Unit = { }

  override def report(): Unit = { }
}
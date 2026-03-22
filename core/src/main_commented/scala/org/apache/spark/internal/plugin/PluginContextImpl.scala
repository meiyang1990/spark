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

/**
 * Spark核心插件模块，提供插件上下文接口的实现类
 * 负责为自定义插件提供执行上下文，包括配置获取、指标注册、Driver通信能力
 */
package org.apache.spark.internal.plugin

import java.util

import com.codahale.metrics.MetricRegistry

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.RpcUtils

/**
 * 插件上下文实现类，为Executor端运行的自定义插件提供执行上下文环境
 * 提供配置访问、主机名获取、指标注册、与Driver端插件通信等核心能力
 * @param pluginName 插件名称
 * @param rpcEnv Rpc环境，用于节点间通信
 * @param metricsSystem 指标系统，用于注册插件自定义指标
 * @param conf Spark配置对象
 * @param executorID 当前Executor编号，Driver端运行时该值为空字符串
 * @param resources 当前Executor分配的资源信息映射
 */
private class PluginContextImpl(
    pluginName: String,
    rpcEnv: RpcEnv,
    metricsSystem: MetricsSystem,
    override val conf: SparkConf,
    override val executorID: String,
    override val resources: util.Map[String, ResourceInformation])
  extends PluginContext with Logging {

  override def hostname(): String = rpcEnv.address.hostPort.split(":")(0)

  // 插件专属的指标注册表，用于存放插件自定义指标
  private val registry = new MetricRegistry()

  // 延迟初始化Driver端插件端点引用，创建失败返回null
  private lazy val driverEndpoint = try {
    RpcUtils.makeDriverRef(classOf[PluginEndpoint].getName(), conf, rpcEnv)
  } catch {
    case e: Exception =>
      logWarning(s"Failed to create driver plugin endpoint ref.", e)
      null
  }

  override def metricRegistry(): MetricRegistry = registry

  override def send(message: AnyRef): Unit = {
    if (driverEndpoint == null) {
      throw new IllegalStateException("Driver endpoint is not known.")
    }
    // 发送单向消息给Driver端的插件，不等待回复
    driverEndpoint.send(PluginMessage(pluginName, message))
  }

  override def ask(message: AnyRef): AnyRef = {
    try {
      if (driverEndpoint != null) {
        // 发送同步请求消息给Driver端插件，等待返回结果
        driverEndpoint.askSync[AnyRef](PluginMessage(pluginName, message))
      } else {
        throw new IllegalStateException("Driver endpoint is not known.")
      }
    } catch {
      // 解包SparkException，抛出原始异常给调用者
      case e: SparkException if e.getCause() != null =>
        throw e.getCause()
    }
  }

  /**
   * 将插件的自定义指标注册到Spark指标系统中
   * 仅当插件存在自定义指标时才会注册
   */
  def registerMetrics(): Unit = {
    if (!registry.getMetrics().isEmpty()) {
      val src = new PluginMetricsSource(s"plugin.$pluginName", registry)
      metricsSystem.registerSource(src)
    }
  }

  /**
   * 插件指标源实现，封装插件指标注册表，统一接入Spark指标系统
   * @param sourceName 指标源名称，格式为plugin.插件名
   * @param metricRegistry 插件专属指标注册表
   */
  class PluginMetricsSource(
      override val sourceName: String,
      override val metricRegistry: MetricRegistry)
    extends Source

}
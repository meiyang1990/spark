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

package org.apache.spark.metrics

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.internal.config.METRICS_CONF
import org.apache.spark.util.Utils

/**
 * 文件级注释：Spark指标系统配置加载与解析类，负责从配置文件和Spark配置中加载指标系统配置，
 * 按指标实例分组管理配置，并支持默认配置 fallback 机制。
 */
private[spark] class MetricsConfig(conf: SparkConf) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"

  private[metrics] val properties = new Properties()
  private[metrics] var perInstanceSubProperties: mutable.HashMap[String, Properties] = null

  /**
   * 设置指标系统默认配置，默认开启MetricsServlet端点用于指标访问
   * @param prop 要设置默认配置的Properties对象
   */
  private def setDefaultProperties(prop: Properties): Unit = {
    prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    prop.setProperty("*.sink.servlet.path", "/metrics/json")
    prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
  }

  /**
   * 初始化指标配置，按优先级加载配置：默认配置 < 配置文件 < Spark配置中带spark.metrics.conf前缀的参数
   * 加载完成后按实例分组整理配置，并为特定实例缺失的配置 fallback 到默认(*)配置
   */
  def initialize(): Unit = {
    // 先加载默认配置，作为兜底
    setDefaultProperties(properties)

    // 从配置文件加载配置，覆盖默认配置
    loadPropertiesFromFile(conf.get(METRICS_CONF))

    // 从SparkConf中加载前缀为spark.metrics.conf.的配置，覆盖已有配置
    val prefix = "spark.metrics.conf."
    conf.getAll.foreach {
      case (k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }

    // 按实例前缀分组，整理出每个实例的子配置
    perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
    // 如果存在默认配置，将默认配置中实例未定义的属性补充到实例配置中
    if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
      val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
      for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
           (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
  }

  /**
   * 将平铺的属性配置按前缀（第一个点之前的部分）分组，提取前缀后的后缀作为子配置的键
   * 不符合正则表达式格式的属性会被忽略
   * @param prop 平铺的原始属性配置
   * @param regex 用于匹配前缀和后缀的正则表达式
   * @return 分组后的配置，键为实例前缀，值为该前缀下的子配置
   */
  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      if (regex.findPrefixOf(kv._1).isDefined) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }
    subProperties
  }

  /**
   * 获取指定实例的配置，如果实例不存在则返回默认(*)配置，如果默认配置也不存在则返回空Properties
   * @param inst 指标实例名称（如driver、master、executor等）
   * @return 对应实例的配置对象
   */
  def getInstance(inst: String): Properties = {
    perInstanceSubProperties.get(inst) match {
      case Some(s) => s
      case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }

  /**
   * 从指定路径或类路径加载指标配置文件
   * 如果用户指定了配置文件路径则加载用户指定文件，否则从类路径加载默认的metrics.properties
   * @param path 用户指定的配置文件路径，None表示使用默认路径
   */
  private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
    var is: InputStream = null
    try {
      // 打开配置文件输入流
      is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
      }

      // 如果成功找到文件，加载配置到properties
      if (is != null) {
        properties.load(is)
      }
    } catch {
      // 加载失败记录错误日志
      case e: Exception =>
        val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
        logError(log"Error loading configuration file ${MDC(PATH, file)}", e)
    } finally {
      // 确保输入流关闭
      if (is != null) {
        is.close()
      }
    }
  }

}
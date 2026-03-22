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

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.codahale.metrics.{Metric, MetricRegistry}
import org.eclipse.jetty.ee10.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.metrics.sink.{MetricsServlet, PrometheusServlet, Sink}
import org.apache.spark.metrics.source.{Source, StaticSources}
import org.apache.spark.util.Utils

/**
 * Spark 度量系统，负责从源采集指标并定期输出到目标sink
 * 
 * "instance"指定使用度量系统的角色，Spark中有master、worker、executor、driver等多种角色，每个角色创建独立的度量系统实例用于监控
 * "source"指定指标采集来源，分为两类：
 *   1. Spark内部源：如MasterSource、WorkerSource，采集Spark组件内部状态，在度量系统创建后添加
 *   2. 公共源：如JvmSource，采集底层系统状态，通过配置加载反射实例化
 * "sink"指定指标输出目的地，支持多个sink同时存在，指标会输出到所有配置的sink
 * 
 * 度量配置格式：[instance].[sink|source].[name].[options] = xxxx
 * - [instance]：可以是master/worker/executor/driver/applications，表示仅对指定实例生效，通配符*表示所有实例生效
 * - [sink|source]：标识该配置属于sink还是source，只能是source或sink
 * - [name]：指定sink或source的名称
 * - [options]：source或sink的具体属性配置
 */
private[spark] class MetricsSystem private (
    val instance: String, conf: SparkConf) extends Logging {

  private[this] val metricsConfig = new MetricsConfig(conf)

  private val sinks = new mutable.ArrayBuffer[Sink]
  private val sources = new mutable.ArrayBuffer[Source]
  private val registry = new MetricRegistry()

  private var running: Boolean = false

  // 将MetricsServlet视为特殊sink，需要暴露给Web UI添加处理器
  private var metricsServlet: Option[MetricsServlet] = None
  private var prometheusServlet: Option[PrometheusServlet] = None

  /**
   * 获取度量系统注册的所有UI处理器，只能在start()之后调用
   * @return 度量Servlet和Prometheus Servlet的处理器数组
   */
  def getServletHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    metricsServlet.map(_.getHandlers(conf)).getOrElse(Array()) ++
      prometheusServlet.map(_.getHandlers(conf)).getOrElse(Array())
  }

  // 初始化度量配置
  metricsConfig.initialize()

  /**
   * 启动度量系统，加载并注册所有源和sink，启动所有sink
   * @param registerStaticSources 是否注册Spark内置静态源，默认为true
   */
  def start(registerStaticSources: Boolean = true): Unit = {
    require(!running, "Attempting to start a MetricsSystem that is already running")
    running = true
    if (registerStaticSources) {
      StaticSources.allSources.foreach(registerSource)
      registerSources()
    }
    registerSinks()
    sinks.foreach(_.start())
  }

  /**
   * 停止度量系统，停止所有sink并清空指标注册表
   */
  def stop(): Unit = {
    if (running) {
      sinks.foreach(_.stop())
      registry.removeMatching((_: String, _: Metric) => true)
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  /**
   * 触发所有sink主动上报指标
   */
  def report(): Unit = {
    sinks.foreach(_.report())
  }

  /**
   * 构建指标源在注册表中的唯一名称
   * 命名格式为：<app ID>.<executor ID/driver>.<source name>，如果ID不可用则退化为<source name>
   * @param source 需要命名的指标源
   * @return 指标源在注册表中的唯一标识名称
   */
  private[spark] def buildRegistryName(source: Source): String = {
    val metricsNamespace = conf.get(METRICS_NAMESPACE).orElse(conf.getOption("spark.app.id"))

    val executorId = conf.get(EXECUTOR_ID)
    val defaultName = MetricRegistry.name(source.sourceName)

    if (instance == "driver" || instance == "executor") {
      if (metricsNamespace.isDefined && executorId.isDefined) {
        MetricRegistry.name(metricsNamespace.get, executorId.get, source.sourceName)
      } else {
        // 只有Driver和Executor会设置spark.app.id和spark.executor.id，Master/Worker等其他实例不关联特定应用
        if (metricsNamespace.isEmpty) {
          logWarning(log"Using default name ${MDC(LogKeys.DEFAULT_NAME, defaultName)} " +
            log"for source because neither " +
            log"${MDC(LogKeys.CONFIG, METRICS_NAMESPACE.key)} nor spark.app.id is set.")
        }
        if (executorId.isEmpty) {
          logWarning(log"Using default name ${MDC(LogKeys.DEFAULT_NAME, defaultName)} " +
            log"for source because spark.executor.id is not set.")
        }
        defaultName
      }
    } else { defaultName }
  }

  /**
   * 根据名称查找匹配的所有指标源
   * @param sourceName 要查找的源名称
   * @return 匹配的源列表
   */
  def getSourcesByName(sourceName: String): Seq[Source] = sources.synchronized {
    sources.filter(_.sourceName == sourceName).toSeq
  }

  /**
   * 注册新的指标源到度量系统
   * @param source 要注册的指标源
   */
  def registerSource(source: Source): Unit = {
    sources.synchronized {
      sources += source
    }
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  /**
   * 从度量系统移除指定指标源
   * @param source 要移除的指标源
   */
  def removeSource(source: Source): Unit = {
    sources.synchronized {
      sources -= source
    }
    val regName = buildRegistryName(source)
    registry.removeMatching((name: String, _: Metric) => name.startsWith(regName))
  }

  /**
   * 根据配置注册所有动态加载的指标源
   */
  private def registerSources(): Unit = {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // 注册所有当前实例关联的源
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Utils.classForName[Source](classPath).getConstructor().newInstance()
        registerSource(source)
      } catch {
        case e: Exception =>
          logError(log"Source class ${MDC(LogKeys.CLASS_NAME, classPath)} " +
            log"cannot be instantiated", e)
      }
    }
  }

  /**
   * 根据配置注册所有输出sink，特殊处理两个HTTP指标Servlet
   */
  private def registerSinks(): Unit = {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          // 处理Metrics Servlet特殊sink
          if (kv._1 == "servlet") {
            val servlet = Utils.classForName[MetricsServlet](classPath)
              .getConstructor(classOf[Properties], classOf[MetricRegistry])
              .newInstance(kv._2, registry)
            metricsServlet = Some(servlet)
          } else if (kv._1 == "prometheusServlet") {
            // 处理Prometheus Servlet特殊sink
            val servlet = Utils.classForName[PrometheusServlet](classPath)
              .getConstructor(classOf[Properties], classOf[MetricRegistry])
              .newInstance(kv._2, registry)
            prometheusServlet = Some(servlet)
          } else {
            // 实例化普通sink，兼容两种构造器签名兼容旧实现
            val sink = try {
              Utils.classForName[Sink](classPath)
                .getConstructor(classOf[Properties], classOf[MetricRegistry])
                .newInstance(kv._2, registry)
            } catch {
              case _: NoSuchMethodException =>
                // 回退到包含SecurityManager参数的三参数构造器
                Utils.classForName[Sink](classPath)
                  .getConstructor(
                    classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
                  .newInstance(kv._2, registry, null)
            }
            sinks += sink
          }
        } catch {
          case e: Exception =>
            logError(log"Sink class ${MDC(LogKeys.CLASS_NAME, classPath)} " +
              log"cannot be instantiated")
            throw e
        }
      }
    }
  }

  /**
   * 获取度量系统加载的原始配置属性
   * @return 度量配置属性对象
   */
  def metricsProperties(): Properties = metricsConfig.properties
}

/**
 * 度量系统的伴生对象，提供正则表达式匹配、轮询周期检查和度量系统创建工厂方法
 */
private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  /**
   * 检查sink轮询周期不低于最小值（1秒）
   * @param pollUnit 配置的时间单位
   * @param pollPeriod 配置的周期长度
   */
  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int): Unit = {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  /**
   * 创建指定实例的度量系统实例
   * @param instance 度量系统所属角色实例名称
   * @param conf Spark配置
   * @return 创建好的度量系统对象
   */
  def createMetricsSystem(instance: String, conf: SparkConf): MetricsSystem = {
    new MetricsSystem(instance, conf)
  }
}

/**
 * 所有预定义的度量系统实例名称常量定义
 */
private[spark] object MetricsSystemInstances {
  // Spark Standalone集群Master节点
  val MASTER = "master"

  // Master下汇报各应用状态的组件
  val APPLICATIONS = "applications"

  // Spark Standalone集群Worker节点
  val WORKER = "worker"

  // Spark执行器Executor
  val EXECUTOR = "executor"

  // Spark驱动程序Driver（创建SparkContext的进程）
  val DRIVER = "driver"

  // Spark Shuffle外部服务
  val SHUFFLE_SERVICE = "shuffleService"

  // YARN运行模式下的ApplicationMaster
  val APPLICATION_MASTER = "applicationMaster"
}
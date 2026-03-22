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

package org.apache.spark.internal.plugin

import scala.jdk.CollectionConverters._
import scala.util.{Either, Left, Right}

import org.apache.spark.{SparkContext, SparkEnv, TaskFailedReason}
import org.apache.spark.api.plugin._
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config._
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.Utils

/**
 * 插件容器抽象基类，定义插件生命周期和事件回调的统一接口
 */
sealed abstract class PluginContainer {

  /** 关闭容器，释放所有插件资源 */
  def shutdown(): Unit
  /** 注册插件指标，绑定到应用ID */
  def registerMetrics(appId: String): Unit
  /** 任务开始执行事件回调 */
  def onTaskStart(): Unit
  /** 任务执行成功事件回调 */
  def onTaskSucceeded(): Unit
  /** 任务执行失败事件回调 */
  def onTaskFailed(failureReason: TaskFailedReason): Unit

}

/**
 * Driver端插件容器，负责管理Driver端所有自定义插件的初始化、生命周期和事件分发
 * @param sc Spark上下文对象
 * @param resources 集群资源信息
 * @param plugins 已加载的插件列表
 */
private class DriverPluginContainer(
    sc: SparkContext,
    resources: java.util.Map[String, ResourceInformation],
    plugins: Seq[SparkPlugin])
  extends PluginContainer with Logging {

  // 提取并初始化所有Driver端插件组件
  private val driverPlugins: Seq[(String, DriverPlugin, PluginContextImpl)] = plugins.flatMap { p =>
    val driverPlugin = p.driverPlugin()
    if (driverPlugin != null) {
      val name = p.getClass().getName()
      val ctx = new PluginContextImpl(name, sc.env.rpcEnv, sc.env.metricsSystem, sc.conf,
        sc.env.executorId, resources)

      // 初始化插件，获取插件返回的额外配置
      val extraConf = driverPlugin.init(sc, ctx)
      if (extraConf != null) {
        extraConf.asScala.foreach { case (k, v) =>
          sc.conf.set(s"${PluginContainer.EXTRA_CONF_PREFIX}$name.$k", v)
        }
      }
      logInfo(log"Initialized driver component for plugin ${MDC(LogKeys.CLASS_NAME, name)}.")
      Some((p.getClass().getName(), driverPlugin, ctx))
    } else {
      None
    }
  }

  // 如果有已初始化的Driver插件，注册RPC端点用于和Executor通信
  if (driverPlugins.nonEmpty) {
    val pluginsByName = driverPlugins.map { case (name, plugin, _) => (name, plugin) }.toMap
    sc.env.rpcEnv.setupEndpoint(classOf[PluginEndpoint].getName(),
      new PluginEndpoint(pluginsByName, sc.env.rpcEnv))
  }

  override def registerMetrics(appId: String): Unit = {
    driverPlugins.foreach { case (_, plugin, ctx) =>
      plugin.registerMetrics(appId, ctx)
      ctx.registerMetrics()
    }
  }

  override def shutdown(): Unit = {
    driverPlugins.foreach { case (name, plugin, _) =>
      try {
        logDebug(s"Stopping plugin $name.")
        plugin.shutdown()
      } catch {
        case t: Throwable =>
          logInfo(log"Exception while shutting down plugin ${MDC(LogKeys.CLASS_NAME, name)}.", t)
      }
    }
  }

  override def onTaskStart(): Unit = {
    throw new IllegalStateException("Should not be called for the driver container.")
  }

  override def onTaskSucceeded(): Unit = {
    throw new IllegalStateException("Should not be called for the driver container.")
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    throw new IllegalStateException("Should not be called for the driver container.")
  }
}

/**
 * Executor端插件容器，负责管理Executor端所有自定义插件的初始化、生命周期和事件分发
 * @param env 当前Executor的Spark环境对象
 * @param resources 分配给该Executor的资源信息
 * @param plugins 已加载的插件列表
 */
private class ExecutorPluginContainer(
    env: SparkEnv,
    resources: java.util.Map[String, ResourceInformation],
    plugins: Seq[SparkPlugin])
  extends PluginContainer with Logging {

  // 提取并初始化所有Executor端插件组件，提取Driver返回的额外配置
  private val executorPlugins: Seq[(String, ExecutorPlugin)] = {
    val allExtraConf = env.conf.getAllWithPrefix(PluginContainer.EXTRA_CONF_PREFIX)

    plugins.flatMap { p =>
      val executorPlugin = p.executorPlugin()
      if (executorPlugin != null) {
        val name = p.getClass().getName()
        val prefix = name + "."
        // 提取当前插件对应的额外配置
        val extraConf = allExtraConf
          .filter { case (k, v) => k.startsWith(prefix) }
          .map { case (k, v) => k.substring(prefix.length()) -> v }
          .toMap
          .asJava
        val ctx = new PluginContextImpl(name, env.rpcEnv, env.metricsSystem, env.conf,
          env.executorId, resources)
        executorPlugin.init(ctx, extraConf)
        ctx.registerMetrics()

        logInfo(log"Initialized executor component for plugin ${MDC(LogKeys.CLASS_NAME, name)}.")
        Some(p.getClass().getName() -> executorPlugin)
      } else {
        None
      }
    }
  }

  override def registerMetrics(appId: String): Unit = {
    throw new IllegalStateException("Should not be called for the executor container.")
  }

  override def shutdown(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        logDebug(s"Stopping plugin $name.")
        plugin.shutdown()
      } catch {
        case t: Throwable =>
          logInfo(log"Exception while shutting down plugin ${MDC(LogKeys.CLASS_NAME, name)}.", t)
      }
    }
  }

  override def onTaskStart(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        plugin.onTaskStart()
      } catch {
        case t: Throwable =>
          logInfo(log"Exception while calling onTaskStart on" +
            log" plugin ${MDC(LogKeys.CLASS_NAME, name)}.", t)
      }
    }
  }

  override def onTaskSucceeded(): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        plugin.onTaskSucceeded()
      } catch {
        case t: Throwable =>
          logInfo(log"Exception while calling onTaskSucceeded on" +
            log" plugin ${MDC(LogKeys.CLASS_NAME, name)}.", t)
      }
    }
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    executorPlugins.foreach { case (name, plugin) =>
      try {
        plugin.onTaskFailed(failureReason)
      } catch {
        case t: Throwable =>
          logInfo(log"Exception while calling onTaskFailed on" +
            log" plugin ${MDC(LogKeys.CLASS_NAME, name)}.", t)
      }
    }
  }
}

/**
 * 插件容器单例工厂，负责从配置加载用户自定义插件，并根据运行端(Driver/Executor)创建对应容器
 */
object PluginContainer {

  // Driver传递额外配置给Executor的配置前缀
  val EXTRA_CONF_PREFIX = "spark.plugins.internal.conf."

  /**
   * 为Driver端创建插件容器
   * @param sc Spark上下文
   * @param resources 集群资源信息
   * @return 插件容器实例（无插件时返回None）
   */
  def apply(
      sc: SparkContext,
      resources: java.util.Map[String, ResourceInformation]): Option[PluginContainer] = {
    PluginContainer(Left(sc), resources)
  }

  /**
   * 为Executor端创建插件容器
   * @param env Spark运行环境
   * @param resources 分配给Executor的资源信息
   * @return 插件容器实例（无插件时返回None）
   */
  def apply(
      env: SparkEnv,
      resources: java.util.Map[String, ResourceInformation]): Option[PluginContainer] = {
    PluginContainer(Right(env), resources)
  }


  /**
   * 内部工厂方法，根据上下文类型创建对应容器
   * @param ctx 上下文标识，Left为Driver，Right为Executor
   * @param resources 资源信息
   * @return 插件容器实例（无插件时返回None）
   */
  private def apply(
      ctx: Either[SparkContext, SparkEnv],
      resources: java.util.Map[String, ResourceInformation]): Option[PluginContainer] = {
    // 获取配置对象
    val conf = ctx.fold(_.conf, _.conf)
    // 从配置加载所有用户自定义插件扩展
    val plugins = Utils.loadExtensions(classOf[SparkPlugin], conf.get(PLUGINS).distinct, conf)
    if (plugins.nonEmpty) {
      ctx match {
        case Left(sc) => Some(new DriverPluginContainer(sc, resources, plugins))
        case Right(env) => Some(new ExecutorPluginContainer(env, resources, plugins))
      }
    } else {
      None
    }
  }
}
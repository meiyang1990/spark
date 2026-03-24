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

import jakarta.ws.rs._
import jakarta.ws.rs.core.MediaType
import org.eclipse.jetty.ee10.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.annotation.Experimental
import org.apache.spark.ui.SparkUI

/**
 * 文件说明: 提供Prometheus格式的Executor指标暴露接口，适配Prometheus拉模型的监控采集
 * 核心功能: 将Spark Executor运行指标转换为Prometheus文本格式，供Prometheus监控系统采集
 */

/**
 * :: Experimental ::
 * 暴露Executor指标的Prometheus格式REST资源端点，提供Prometheus兼容的监控指标接口
 * 基于Executor摘要信息生成指标，区别于ExecutorSource的实时指标采集
 * 
 * @experimental 实验性API，可能在未来版本变更
 */
@Experimental
@Path("/executors")
private[v1] class PrometheusResource extends ApiRequestContext {
  @GET
  @Path("prometheus")
  @Produces(Array(MediaType.TEXT_PLAIN))
  /**
   * 生成所有Executor的Prometheus格式指标数据
   * @return 符合Prometheus文本格式的指标字符串
   */
  def executors(): String = {
    val sb = new StringBuilder
    // 添加Spark版本信息基础指标
    sb.append(s"""spark_info{version="$SPARK_VERSION_SHORT", revision="$SPARK_REVISION"} 1.0\n""")
    // 获取Spark UI存储，读取应用和Executor信息
    val store = uiRoot.asInstanceOf[SparkUI].store
    // 遍历所有Executor生成对应指标
    store.executorList(true).foreach { executor =>
      val prefix = "metrics_executor_"
      // 构造Prometheus标签，包含应用ID、应用名称、Executor ID
      val labels = Seq(
        "application_id" -> store.applicationInfo().id,
        "application_name" -> store.applicationInfo().name,
        "executor_id" -> executor.id
      ).map { case (k, v) => s"""$k="$v"""" }.mkString("{", ", ", "}")
      // 输出Executor基本运行指标
      sb.append(s"${prefix}rddBlocks$labels ${executor.rddBlocks}\n")
      sb.append(s"${prefix}memoryUsed_bytes$labels ${executor.memoryUsed}\n")
      sb.append(s"${prefix}diskUsed_bytes$labels ${executor.diskUsed}\n")
      sb.append(s"${prefix}totalCores$labels ${executor.totalCores}\n")
      sb.append(s"${prefix}maxTasks$labels ${executor.maxTasks}\n")
      sb.append(s"${prefix}activeTasks$labels ${executor.activeTasks}\n")
      sb.append(s"${prefix}failedTasks_total$labels ${executor.failedTasks}\n")
      sb.append(s"${prefix}completedTasks_total$labels ${executor.completedTasks}\n")
      sb.append(s"${prefix}totalTasks_total$labels ${executor.totalTasks}\n")
      // 转换毫秒为秒输出总任务执行时间
      sb.append(s"${prefix}totalDuration_seconds_total$labels ${executor.totalDuration * 0.001}\n")
      // 转换毫秒为秒输出总GC时间
      sb.append(s"${prefix}totalGCTime_seconds_total$labels ${executor.totalGCTime * 0.001}\n")
      sb.append(s"${prefix}totalInputBytes_bytes_total$labels ${executor.totalInputBytes}\n")
      sb.append(s"${prefix}totalShuffleRead_bytes_total$labels ${executor.totalShuffleRead}\n")
      sb.append(s"${prefix}totalShuffleWrite_bytes_total$labels ${executor.totalShuffleWrite}\n")
      sb.append(s"${prefix}maxMemory_bytes$labels ${executor.maxMemory}\n")
      executor.executorLogs.foreach { case (k, v) => }
      // 输出内存存储指标
      executor.memoryMetrics.foreach { m =>
        sb.append(s"${prefix}usedOnHeapStorageMemory_bytes$labels ${m.usedOnHeapStorageMemory}\n")
        sb.append(s"${prefix}usedOffHeapStorageMemory_bytes$labels ${m.usedOffHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOnHeapStorageMemory_bytes$labels ${m.totalOnHeapStorageMemory}\n")
        sb.append(s"${prefix}totalOffHeapStorageMemory_bytes$labels " +
          s"${m.totalOffHeapStorageMemory}\n")
      }
      // 输出峰值内存指标和GC统计指标
      executor.peakMemoryMetrics.foreach { m =>
        val names = Array(
          "JVMHeapMemory",
          "JVMOffHeapMemory",
          "OnHeapExecutionMemory",
          "OffHeapExecutionMemory",
          "OnHeapStorageMemory",
          "OffHeapStorageMemory",
          "OnHeapUnifiedMemory",
          "OffHeapUnifiedMemory",
          "DirectPoolMemory",
          "MappedPoolMemory",
          "ProcessTreeJVMVMemory",
          "ProcessTreeJVMRSSMemory",
          "ProcessTreePythonVMemory",
          "ProcessTreePythonRSSMemory",
          "ProcessTreeOtherVMemory",
          "ProcessTreeOtherRSSMemory"
        )
        // 输出各类内存峰值指标
        names.foreach { name =>
          sb.append(s"$prefix${name}_bytes$labels ${m.getMetricValue(name)}\n")
        }
        // 输出GC次数指标
        Seq("MinorGCCount", "MajorGCCount", "ConcurrentGCCount").foreach { name =>
          sb.append(s"$prefix${name}_total$labels ${m.getMetricValue(name)}\n")
        }
        // 输出GC时间指标，转换毫秒为秒
        Seq("MinorGCTime", "MajorGCTime", "ConcurrentGCTime").foreach { name =>
          sb.append(s"$prefix${name}_seconds_total$labels ${m.getMetricValue(name) * 0.001}\n")
        }
      }
    }
    sb.toString
  }
}

/**
 * Prometheus指标端点工具类，负责创建并配置Prometheus指标服务的Jetty Servlet上下文
 */
private[spark] object PrometheusResource {
  /**
   * 创建并配置Prometheus指标端点的Servlet上下文处理器
   * @param uiRoot Spark UI根上下文，用于获取应用状态信息
   * @return 配置完成的Servlet上下文处理器，可直接挂载到Jetty服务器
   */
  def getServletHandler(uiRoot: UIRoot): ServletContextHandler = {
    // 创建无会话的Servlet上下文
    val jerseyContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    // 设置指标端点上下文路径
    jerseyContext.setContextPath("/metrics")
    // 创建Jersey Servlet容器
    val holder: ServletHolder = new ServletHolder(classOf[ServletContainer])
    // 配置Jersey扫描的API包路径
    holder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.spark.status.api.v1")
    // 将UIRoot存入Servlet上下文供API端点使用
    UIRootFromServletContext.setUiRoot(jerseyContext, uiRoot)
    // 挂载Servlet到所有子路径
    jerseyContext.addServlet(holder, "/*")
    jerseyContext
  }
}
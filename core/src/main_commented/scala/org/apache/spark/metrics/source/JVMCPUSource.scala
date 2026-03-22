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
 * JVM CPU 指标源，实现 Spark 指标收集接口，收集当前进程CPU使用时间指标
 * 属于 Spark 核心指标系统模块，提供JVM进程CPU使用率相关监控数据
 */
package org.apache.spark.metrics.source

import java.lang.management.ManagementFactory
import javax.management.{MBeanServer, ObjectName}

import scala.util.control.NonFatal

import com.codahale.metrics.{Gauge, MetricRegistry}

/**
 * JVM CPU 指标源，负责收集并导出JVM进程CPU时间监控指标
 * 实现Source接口，供Spark指标系统注册和拉取指标数据
 */
private[spark] class JVMCPUSource extends Source {

  override val metricRegistry = new MetricRegistry()
  override val sourceName = "JVMCPU"

  // Dropwizard/Codahale metrics gauge measuring the JVM process CPU time.
  // This Gauge will try to get and return the JVM Process CPU time or return -1 otherwise.
  // The CPU time value is returned in nanoseconds.
  // It will use proprietary extensions such as com.sun.management.OperatingSystemMXBean or
  // com.ibm.lang.management.OperatingSystemMXBean, if available.
  /** 注册JVM CPU时间指标到指标注册表 */
  metricRegistry.register(MetricRegistry.name("jvmCpuTime"), new Gauge[Long] {
    /** 获取平台MBean服务器，用于查询JVM管理接口 */
    val mBean: MBeanServer = ManagementFactory.getPlatformMBeanServer
    /** 构造操作系统MBean的ObjectName */
    val name = new ObjectName("java.lang", "type", "OperatingSystem")
    override def getValue: Long = {
      try {
        // return JVM process CPU time if the ProcessCpuTime method is available
        /** 从MBean获取进程CPU时间，单位为纳秒 */
        mBean.getAttribute(name, "ProcessCpuTime").asInstanceOf[Long]
      } catch {
        /** 获取失败时返回-1，表示指标不可用 */
        case NonFatal(_) => -1L
      }
    }
  })
}
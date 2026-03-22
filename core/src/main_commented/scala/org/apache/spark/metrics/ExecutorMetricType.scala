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

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import javax.management.ObjectName

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ProcfsMetricsGetter
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.memory.MemoryManager

/**
 * 文件说明：定义Executor节点可监控的所有度量指标类型，提供统一的指标值获取接口
 * 核心职责：枚举不同类型的Executor资源度量（内存、GC等，提供统一的名称定义和值获取方法
 */

/**
 * Executor度量指标类型的公共接口，定义所有指标都需要实现的能力
 */
sealed trait ExecutorMetricType {
  private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long]
  private[spark] def names: Seq[String]
}

/**
 * 单值类型Executor度量指标的抽象基类，处理通用逻辑
 */
sealed trait SingleValueExecutorMetricType extends ExecutorMetricType {
  override private[spark] def names = {
    Seq(getClass().getName().
      stripSuffix("$").split("""\.""").last)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    val metrics = new Array[Long](1)
    metrics(0) = getMetricValue(memoryManager)
    metrics
  }

  private[spark] def getMetricValue(memoryManager: MemoryManager): Long
}

/**
 * 基于MemoryManager获取的单值度量指标实现
 * @param f 从MemoryManager中提取指标值的函数
 */
private[spark] abstract class MemoryManagerExecutorMetricType(
    f: MemoryManager => Long) extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

/**
 * 基于JMX MBean获取的单值度量指标实现
 * @param mBeanName JMX MBean的对象名
 */
private[spark] abstract class MBeanExecutorMetricType(mBeanName: String)
  extends SingleValueExecutorMetricType {
  // 获取对应MBean代理
  private val bean = ManagementFactory.newPlatformMXBeanProxy(
    ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    // 返回当前已使用内存大小
    bean.getMemoryUsed
  }
}

/**
 * JVM堆内存已使用量指标
 */
case object JVMHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

/**
 * JVM非堆内存已使用量指标
 */
case object JVMOffHeapMemory extends SingleValueExecutorMetricType {
  override private[spark] def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

/**
 * 进程树内存度量指标，包含JVM、Python和其他子进程的虚拟内存和RSS内存
 */
case object ProcessTreeMetrics extends ExecutorMetricType {
  // 所有子进程分组后6个指标名称
  override val names = Seq(
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory")

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    // 从/proc文件系统计算所有子进程内存指标
    val allMetrics = ProcfsMetricsGetter.pTreeInfo.computeAllMetrics()
    val processTreeMetrics = new Array[Long](names.length)
    // 赋值JVM虚拟内存总量
    processTreeMetrics(0) = allMetrics.jvmVmemTotal
    // 赋值JVM RSS内存总量
    processTreeMetrics(1) = allMetrics.jvmRSSTotal
    // 赋值Python虚拟内存总量
    processTreeMetrics(2) = allMetrics.pythonVmemTotal
    // 赋值Python RSS内存总量
    processTreeMetrics(3) = allMetrics.pythonRSSTotal
    // 赋值其他子进程虚拟内存总量
    processTreeMetrics(4) = allMetrics.otherVmemTotal
    // 赋值其他子进程RSS内存总量
    processTreeMetrics(5) = allMetrics.otherRSSTotal
    processTreeMetrics
  }
}

/**
 * 垃圾收集GC度量指标，包含分代统计GC次数和时间
 */
case object GarbageCollectionMetrics extends ExecutorMetricType with Logging {
  // 存储未内置支持的GC收集器列表，用于日志提示用户配置
  private var nonBuiltInCollectors: Seq[String] = Nil

  // 所有GC指标名称，按顺序定义
  override val names = Seq(
    "MinorGCCount",
    "MinorGCTime",
    "MajorGCCount",
    "MajorGCTime",
    "TotalGCTime",
    "ConcurrentGCCount",
    "ConcurrentGCTime"
  )

  /* 内置支持的常见年轻代GC收集器列表 */
  private[spark] val YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "Copy",
    "PS Scavenge",
    "ParNew",
    "G1 Young Generation"
  )

  // 内置支持的常见老年代GC收集器列表
  private[spark] val OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS = Seq(
    "MarkSweepCompact",
    "PS MarkSweep",
    "ConcurrentMarkSweep",
    "G1 Old Generation"
  )

  // 内置支持的并发GC收集器名称
  private[spark] val BUILTIN_CONCURRENT_GARBAGE_COLLECTOR = "G1 Concurrent GC"

  // 延迟初始化年轻代GC收集器列表，从配置读取配置项加载
  private lazy val youngGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS)
  }

  // 延迟初始化老年代GC收集器列表，从配置读取配置项加载
  private lazy val oldGenerationGarbageCollector: Seq[String] = {
    SparkEnv.get.conf.get(config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS)
  }

  override private[spark] def getMetricValues(memoryManager: MemoryManager): Array[Long] = {
    // 初始化7个指标的数组
    val gcMetrics = new Array[Long](names.length)
    // 获取所有GC收集器MXBean
    val mxBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    // 计算所有GC总时间
    gcMetrics(4) = mxBeans.map(_.getCollectionTime).sum
    // 遍历每个GC收集器分类统计
    mxBeans.foreach { mxBean =>
      if (youngGenerationGarbageCollector.contains(mxBean.getName)) {
        // 归为年轻代GC，统计次数和时间
        gcMetrics(0) = mxBean.getCollectionCount
        gcMetrics(1) = mxBean.getCollectionTime
      } else if (oldGenerationGarbageCollector.contains(mxBean.getName)) {
        // 归为老年代GC，统计次数和时间
        gcMetrics(2) = mxBean.getCollectionCount
        gcMetrics(3) = mxBean.getCollectionTime
      } else if (BUILTIN_CONCURRENT_GARBAGE_COLLECTOR.equals(mxBean.getName)) {
        // 归为并发GC，统计次数和时间
        gcMetrics(5) = mxBean.getCollectionCount
        gcMetrics(6) = mxBean.getCollectionTime
      } else if (!nonBuiltInCollectors.contains(mxBean.getName)) {
        // 首次发现未内置识别的GC收集器，添加到列表并日志提示用户配置
        nonBuiltInCollectors = mxBean.getName +: nonBuiltInCollectors
        // 构造MDC日志上下文
        val youngGenerationGc = MDC(YOUNG_GENERATION_GC,
          config.EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS.key)
        val oldGenerationGc = MDC(OLD_GENERATION_GC,
          config.EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS.key)
        // 输出警告日志提示用户配置自定义GC收集器
        logWarning(log"To enable non-built-in garbage collector(s) " +
          log"${MDC(NON_BUILT_IN_CONNECTORS, nonBuiltInCollectors)}, " +
          log"users should configure it(them) to $youngGenerationGc or $oldGenerationGc")
      } else {
        // 已记录过的非内置收集器，不处理
      }
    }
    gcMetrics
  }
}

/**
 * 堆内执行内存使用量指标
 */
case object OnHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.onHeapExecutionMemoryUsed)

/**
 * 堆外执行内存使用量指标
 */
case object OffHeapExecutionMemory extends MemoryManagerExecutorMetricType(
  _.offHeapExecutionMemoryUsed)

/**
 * 堆内存储内存使用量指标
 */
case object OnHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.onHeapStorageMemoryUsed)

/**
 * 堆外存储内存使用量指标
 */
case object OffHeapStorageMemory extends MemoryManagerExecutorMetricType(
  _.offHeapStorageMemoryUsed)

/**
 * 堆内统一内存总使用量（执行+存储合并指标
 */
case object OnHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

/**
 * 堆外统一内存总使用量（执行+存储合并指标
 */
case object OffHeapUnifiedMemory extends MemoryManagerExecutorMetricType(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

/**
 * 直接缓冲区内存使用量指标（NIO direct buffer池指标
 */
case object DirectPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=direct")

/**
 * 内存映射缓冲区内存使用量指标
 */
case object MappedPoolMemory extends MBeanExecutorMetricType(
  "java.nio:type=BufferPool,name=mapped")

/**
 * 全局对象，管理所有已注册的Executor度量指标，构建指标偏移索引
 */
private[spark] object ExecutorMetricType {

  // 所有已注册的度量指标获取器列表，按顺序排列
  val metricGetters = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory,
    ProcessTreeMetrics,
    GarbageCollectionMetrics
  )

  // 预计算每个指标名称对应的全局偏移，和总指标数量
  val (metricToOffset, numMetrics) = {
    var numberOfMetrics = 0
    val definedMetricsAndOffset = mutable.LinkedHashMap.empty[String, Int]
    // 遍历每个指标分配偏移
    metricGetters.foreach { m =>
      m.names.indices.foreach { idx =>
        definedMetricsAndOffset += (m.names(idx) -> (idx + numberOfMetrics))
      }
      numberOfMetrics += m.names.length
    }
    (definedMetricsAndOffset, numberOfMetrics)
  }
}
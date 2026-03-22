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

package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

/**
 * 文件级说明：Spark核心模块静态指标源集合，提供无需依赖SparkEnv即可使用的全局静态指标
 * 包含代码生成指标和Hive元数据访问指标两类静态指标源
 */
private[spark] object StaticSources {
  /**
   * 所有静态指标源的集合，这些指标源可被任意类（包括静态类）上报，不需要持有SparkEnv引用
   */
  val allSources = Seq(CodegenMetrics, HiveCatalogMetrics)
}

/**
 * 代码生成模块指标源，统计整个Spark代码生成过程的各项性能指标
 * 继承Source接口，提供给Metrics系统统一采集上报
 */
object CodegenMetrics extends Source {
  override val sourceName: String = "CodeGenerator"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * 生成源码长度直方图统计，单位：字符
   */
  val METRIC_SOURCE_CODE_SIZE = metricRegistry.histogram(MetricRegistry.name("sourceCodeSize"))

  /**
   * 源码编译耗时直方图统计，单位：毫秒
   */
  val METRIC_COMPILATION_TIME = metricRegistry.histogram(MetricRegistry.name("compilationTime"))

  /**
   * 生成类字节码大小直方图统计
   */
  val METRIC_GENERATED_CLASS_BYTECODE_SIZE =
    metricRegistry.histogram(MetricRegistry.name("generatedClassSize"))

  /**
   * 生成类中每个方法字节码大小直方图统计
   */
  val METRIC_GENERATED_METHOD_BYTECODE_SIZE =
    metricRegistry.histogram(MetricRegistry.name("generatedMethodSize"))
}

/**
 * Hive外部元数据 catalog 访问指标源，统计元数据访问和文件发现过程的各项指标
 * 继承Source接口，提供给Metrics系统统一采集上报
 */
object HiveCatalogMetrics extends Source {
  override val sourceName: String = "HiveExternalCatalog"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  /**
   * 通过客户端API获取分区元数据的总条目数计数器
   */
  val METRIC_PARTITIONS_FETCHED = metricRegistry.counter(MetricRegistry.name("partitionsFetched"))

  /**
   * InMemoryFileIndex从文件系统发现的文件总数计数器
   */
  val METRIC_FILES_DISCOVERED = metricRegistry.counter(MetricRegistry.name("filesDiscovered"))

  /**
   * 从文件状态缓存命中的文件总数计数器（无需重新扫描文件系统）
   */
  val METRIC_FILE_CACHE_HITS = metricRegistry.counter(MetricRegistry.name("fileCacheHits"))

  /**
   * Hive客户端调用总次数计数器（例如表元数据查询）
   */
  val METRIC_HIVE_CLIENT_CALLS = metricRegistry.counter(MetricRegistry.name("hiveClientCalls"))

  /**
   * 并行文件列表扫描启动的Spark作业总数计数器
   */
  val METRIC_PARALLEL_LISTING_JOB_COUNT = metricRegistry.counter(
    MetricRegistry.name("parallelListingJobCount"))

  /**
   * 重置所有指标计数为0，主要用于测试场景
   */
  def reset(): Unit = {
    METRIC_PARTITIONS_FETCHED.dec(METRIC_PARTITIONS_FETCHED.getCount())
    METRIC_FILES_DISCOVERED.dec(METRIC_FILES_DISCOVERED.getCount())
    METRIC_FILE_CACHE_HITS.dec(METRIC_FILE_CACHE_HITS.getCount())
    METRIC_HIVE_CLIENT_CALLS.dec(METRIC_HIVE_CLIENT_CALLS.getCount())
    METRIC_PARALLEL_LISTING_JOB_COUNT.dec(METRIC_PARALLEL_LISTING_JOB_COUNT.getCount())
  }

  /**
   * 增加获取分区数计数，避免客户端直接依赖Codahale类导致类加载问题
   * @param n 增加的数量
   */
  def incrementFetchedPartitions(n: Int): Unit = METRIC_PARTITIONS_FETCHED.inc(n)

  /**
   * 增加发现文件数计数，避免客户端直接依赖Codahale类导致类加载问题
   * @param n 增加的数量
   */
  def incrementFilesDiscovered(n: Int): Unit = METRIC_FILES_DISCOVERED.inc(n)

  /**
   * 增加文件缓存命中数计数，避免客户端直接依赖Codahale类导致类加载问题
   * @param n 增加的数量
   */
  def incrementFileCacheHits(n: Int): Unit = METRIC_FILE_CACHE_HITS.inc(n)

  /**
   * 增加Hive客户端调用数计数，避免客户端直接依赖Codahale类导致类加载问题
   * @param n 增加的数量
   */
  def incrementHiveClientCalls(n: Int): Unit = METRIC_HIVE_CLIENT_CALLS.inc(n)

  /**
   * 增加并行列表扫描作业数计数，避免客户端直接依赖Codahale类导致类加载问题
   * @param n 增加的数量
   */
  def incrementParallelListingJobCount(n: Int): Unit = METRIC_PARALLEL_LISTING_JOB_COUNT.inc(n)
}
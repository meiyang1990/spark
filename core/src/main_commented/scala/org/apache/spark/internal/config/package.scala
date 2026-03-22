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

package org.apache.spark.internal

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.io.CompressionCodec
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.metrics.GarbageCollectionMetrics
import org.apache.spark.network.shuffle.Constants
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.{EventLoggingListener, SchedulingMode}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO
import org.apache.spark.storage.{DefaultTopologyMapper, RandomBlockReplicationPolicy}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.{MavenUtils, Utils}
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillReader.MAX_BUFFER_SIZE_BYTES

/**
 * Spark核心配置项统一定义包对象，集中管理Spark内核所有内置配置参数
 * 所有配置项通过ConfigBuilder构建，提供类型安全的配置读取与校验
 */
package object config {

  // 配置键前缀常量，用于分类不同角色的配置
  private[spark] val SPARK_DRIVER_PREFIX = "spark.driver"
  private[spark] val SPARK_EXECUTOR_PREFIX = "spark.executor"
  private[spark] val SPARK_TASK_PREFIX = "spark.task"
  private[spark] val LISTENER_BUS_EVENT_QUEUE_PREFIX = "spark.scheduler.listenerbus.eventqueue"

  /** 默认并行度配置：用户未指定时，转换操作生成的RDD默认分区数 */
  private[spark] val DEFAULT_PARALLELISM =
    ConfigBuilder("spark.default.parallelism")
      .doc("Default number of partitions in RDDs returned by transformations like " +
        "join, reduceByKey, and parallelize when not set by user. " +
        "For distributed shuffle operations like reduceByKey and join, the largest number of " +
        "partitions in a parent RDD. For operations like parallelize with no parent RDDs, " +
        "it depends on the cluster manager. For example in Local mode, it defaults to the " +
        "number of cores on the local machine")
      .version("0.5.0")
      .intConf
      .createOptional

  /** 资源发现插件配置：自定义资源发现插件类名列表 */
  private[spark] val RESOURCES_DISCOVERY_PLUGIN =
    ConfigBuilder("spark.resources.discoveryPlugin")
      .doc("Comma-separated list of class names implementing" +
        "org.apache.spark.api.resource.ResourceDiscoveryPlugin to load into the application." +
        "This is for advanced users to replace the resource discovery class with a " +
        "custom implementation. Spark will try each class specified until one of them " +
        "returns the resource information for that resource. It tries the discovery " +
        "script last if none of the plugins return information for that resource.")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  /** Driver资源分配文件配置：Standalone模式下Driver资源分配JSON文件路径，内部使用 */
  private[spark] val DRIVER_RESOURCES_FILE =
    ConfigBuilder("spark.driver.resourcesFile")
      .internal()
      .doc("Path to a file containing the resources allocated to the driver. " +
        "The file should be formatted as a JSON array of ResourceAllocation objects. " +
        "Only used internally in standalone mode.")
      .version("3.0.0")
      .stringConf
      .createOptional

  /** Driver默认额外类路径配置：内部使用，保存默认值 */
  private[spark] val DRIVER_DEFAULT_EXTRA_CLASS_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_DEFAULT_EXTRA_CLASS_PATH)
      .internal()
      .version("4.0.0")
      .stringConf
      .createWithDefault(SparkLauncher.DRIVER_DEFAULT_EXTRA_CLASS_PATH_VALUE)

  /** Driver额外类路径配置：用户自定义的Driver额外类路径 */
  private[spark] val DRIVER_CLASS_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH)
      .withPrepended(DRIVER_DEFAULT_EXTRA_CLASS_PATH.key, File.pathSeparator)
      .version("1.0.0")
      .stringConf
      .createOptional

  /** Driver额外Java选项配置：Driver进程的额外JVM参数 */
  private[spark] val DRIVER_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
      .withPrepended(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)
      .version("1.0.0")
      .stringConf
      .createOptional

  /** Driver额外库路径配置：Driver进程的额外本地库路径 */
  private[spark] val DRIVER_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH)
      .version("1.0.0")
      .stringConf
      .createOptional

  /** Driver优先用户类路径配置：是否优先加载用户类路径中的类 */
  private[spark] val DRIVER_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.driver.userClassPathFirst")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(false)

  /** Driver核心数配置：集群模式下Driver进程使用的CPU核心数 */
  private[spark] val DRIVER_CORES = ConfigBuilder("spark.driver.cores")
    .doc("Number of cores to use for the driver process, only in cluster mode.")
    .version("1.3.0")
    .intConf
    .createWithDefault(1)

  /** Driver内存配置：Driver进程使用的内存大小 */
  private[spark] val DRIVER_MEMORY = ConfigBuilder(SparkLauncher.DRIVER_MEMORY)
    .doc("Amount of memory to use for the driver process, in MiB unless otherwise specified.")
    .version("1.1.1")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  /** Driver内存Overhead配置：集群模式下Driver进程的非堆内存大小 */
  private[spark] val DRIVER_MEMORY_OVERHEAD = ConfigBuilder("spark.driver.memoryOverhead")
    .doc("The amount of non-heap memory to be allocated per driver in cluster mode, " +
      "in MiB unless otherwise specified.")
    .version("2.3.0")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  /** Driver最小内存Overhead配置：集群模式下Driver进程非堆内存最小值 */
  private[spark] val DRIVER_MIN_MEMORY_OVERHEAD = ConfigBuilder("spark.driver.minMemoryOverhead")
    .doc("The minimum amount of non-heap memory to be allocated per driver in cluster mode, " +
      "in MiB unless otherwise specified. This value is ignored if " +
      "spark.driver.memoryOverhead is set directly.")
    .version("4.0.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("384m")

  /** Driver内存Overhead系数：非堆内存占Driver内存的比例 */
  private[spark] val DRIVER_MEMORY_OVERHEAD_FACTOR =
    ConfigBuilder("spark.driver.memoryOverheadFactor")
      .doc("Fraction of driver memory to be allocated as additional non-heap memory per driver " +
        "process in cluster mode. This is memory that accounts for things like VM overheads, " +
        "interned strings, other native overheads, etc. This tends to grow with the container " +
        "size. This value defaults to 0.10 except for Kubernetes non-JVM jobs, which defaults to " +
        "0.40. This is done as non-JVM tasks need more non-JVM heap space and such tasks " +
        "commonly fail with \"Memory Overhead Exceeded\" errors. This preempts this error " +
        "with a higher default. This value is ignored if spark.driver.memoryOverhead is set " +
        "directly.")
      .version("3.3.0")
      .doubleConf
      .checkValue(factor => factor > 0,
        "Ensure that memory overhead is a double greater than 0")
      .createWithDefault(0.1)

  /** 结构化日志配置：是否启用JSON格式结构化日志输出 */
  private[spark] val STRUCTURED_LOGGING_ENABLED =
    ConfigBuilder("spark.log.structuredLogging.enabled")
      .doc("When true, Spark logs are output as structured JSON lines with added Spark " +
        "Mapped Diagnostic Context (MDC), facilitating easier integration with log aggregation " +
        "and analysis tools. When false, logs are plain text without MDC. This configuration " +
        "does not apply to interactive environments such as spark-shell, spark-sql, and " +
        "PySpark shell.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  /** 旧版任务名MDC键名兼容配置：是否兼容旧版`mdc.taskName`键名 */
  private[spark] val LEGACY_TASK_NAME_MDC_ENABLED =
    ConfigBuilder("spark.log.legacyTaskNameMdc.enabled")
      .doc("When true, the MDC (Mapped Diagnostic Context) key `mdc.taskName` will be set in the " +
        "log output, which is the behavior of Spark version 3.1 through Spark 3.5 releases. " +
        "When false, the logging framework will use `task_name` as the MDC key, " +
        "aligning it with the naming convention of newer MDC keys introduced in Spark 4.0 release.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(false)

  /** Driver日志本地目录配置：Driver日志本地存储目录，启用UI日志展示 */
  private[spark] val DRIVER_LOG_LOCAL_DIR =
    ConfigBuilder("spark.driver.log.localDir")
      .doc("Specifies a local directory to write driver logs and enable Driver Log UI Tab.")
      .version("4.0.0")
      .stringConf
      .createOptional

  private[spark] val DRIVER_LOG_DFS_DIR =
    ConfigBuilder("spark.driver.log.dfsDir").version("3.0.0").stringConf.createOptional

  private[spark] val DRIVER_LOG_LAYOUT =
    ConfigBuilder("spark.driver.log.layout")
      .version("3.0.0")
      .stringConf
      .createOptional

  /** Driver日志持久化到DFS配置：是否将Driver日志持久化到DFS */
  private[spark] val DRIVER_LOG_PERSISTTODFS =
    ConfigBuilder("spark.driver.log.persistToDfs.enabled")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  /** Driver日志允许擦除编码配置：是否允许在擦除编码目录存储Driver日志 */
  private[spark] val DRIVER_LOG_ALLOW_EC =
    ConfigBuilder("spark.driver.log.allowErasureCoding")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  /** 事件日志配置：是否启用事件日志 */
  private[spark] val EVENT_LOG_ENABLED = ConfigBuilder("spark.eventLog.enabled")
    .version("1.0.0")
    .booleanConf
    .createWithDefault(false)

  /** 事件日志目录配置：存储事件日志的目录 */
  private[spark] val EVENT_LOG_DIR = ConfigBuilder("spark.eventLog.dir")
    .version("1.0.0")
    .stringConf
    .createWithDefault(EventLoggingListener.DEFAULT_LOG_DIR)

  /** 事件日志压缩配置：是否压缩事件日志 */
  private[spark] val EVENT_LOG_COMPRESS =
    ConfigBuilder("spark.eventLog.compress")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  /** 事件日志块更新记录配置：是否记录块更新事件到日志 */
  private[spark] val EVENT_LOG_BLOCK_UPDATES =
    ConfigBuilder("spark.eventLog.logBlockUpdates.enabled")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  /** 事件日志排除事件配置：逗号分隔的需要排除的事件名称列表 */
  private[spark] val EVENT_LOG_EXCLUDED_PATTERNS =
    ConfigBuilder("spark.eventLog.excludedPatterns")
      .doc("Specifies comma-separated event names to be excluded from the event logs.")
      .version("4.1.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  /** 事件日志允许擦除编码配置：是否允许在擦除编码目录存储事件日志 */
  private[spark] val EVENT_LOG_ALLOW_EC =
    ConfigBuilder("spark.eventLog.erasureCoding.enabled")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  /** 事件日志测试模式配置：内部使用，是否开启测试模式 */
  private[spark] val EVENT_LOG_TESTING =
    ConfigBuilder("spark.eventLog.testing")
      .internal()
      .version("1.0.1")
      .booleanConf
      .createWithDefault(false)

  /** 事件日志输出缓冲区大小配置：写入事件日志流的缓冲区大小 */
  private[spark] val EVENT_LOG_OUTPUT_BUFFER_SIZE = ConfigBuilder("spark.eventLog.buffer.kb")
    .doc("Buffer size to use when writing to output streams, in KiB unless otherwise specified.")
    .version("1.0.0")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("100k")

  /** 事件日志阶段执行器指标记录配置：是否记录每个阶段每个执行器的指标峰值 */
  private[spark] val EVENT_LOG_STAGE_EXECUTOR_METRICS =
    ConfigBuilder("spark.eventLog.logStageExecutorMetrics")
      .doc("Whether to write per-stage peaks of executor metrics (for each executor) " +
        "to the event log.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  /** 年轻代垃圾收集器名称配置：事件日志中记录GC指标支持的年轻代收集器列表 */
  private[spark] val EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS =
    ConfigBuilder("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors")
      .doc("Names of supported young generation garbage collector. A name usually is " +
        " the return of GarbageCollectorMXBean.getName. The built-in young generation garbage " +
        s"collectors are ${GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS}")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS)

  /** 老年代垃圾收集器名称配置：事件日志中记录GC指标支持的老年代收集器列表 */
  private[spark] val EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS =
    ConfigBuilder("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors")
      .doc("Names of supported old generation garbage collector. A name usually is " +
        "the return of GarbageCollectorMXBean.getName. The built-in old generation garbage " +
        s"collectors are ${GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS}")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS)

  /** 事件日志包含任务指标累加器配置：是否在事件日志中包含任务指标底层累加器值 */
  private[spark] val EVENT_LOG_INCLUDE_TASK_METRICS_ACCUMULATORS =
    ConfigBuilder("spark.eventLog.includeTaskMetricsAccumulators")
      .doc("Whether to include TaskMetrics' underlying accumulator values in the event log " +
        "(as part of the Task/Stage/Job metrics' 'Accumulables' fields. The TaskMetrics " +
        "values are already logged in the 'Task Metrics' fields (so the accumulator updates " +
        "are redundant). This flag defaults to true for behavioral backwards compatibility " +
        "for applications that might rely on the redundant logging. " +
        "See SPARK-42204 for details.")
      .version("4.0.0")
      .booleanConf
      .createWithDefault(true)

  /**
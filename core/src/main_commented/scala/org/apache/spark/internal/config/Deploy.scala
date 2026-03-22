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

package org.apache.spark.internal.config

import java.util.Locale
import java.util.concurrent.TimeUnit

/**
 * Spark Standalone部署模式相关配置项定义
 * 包含Master高可用恢复、应用状态保留、Worker调度、ID生成等部署相关配置
 */
private[spark] object Deploy {
  /** 集群恢复模式配置，支持NONE/ZOOKEEPER/FILESYSTEM等模式 */
  val RECOVERY_MODE = ConfigBuilder("spark.deploy.recoveryMode")
    .version("0.8.1")
    .stringConf
    .createWithDefault("NONE")

  /** 持久化引擎压缩编码配置，仅支持FILESYSTEM恢复模式 */
  val RECOVERY_COMPRESSION_CODEC = ConfigBuilder("spark.deploy.recoveryCompressionCodec")
    .doc("A compression codec for persistence engines. none (default), lz4, lzf, snappy, and " +
      "zstd. Currently, only FILESYSTEM mode supports this configuration.")
    .version("4.0.0")
    .stringConf
    .createOptional

  /** 恢复模式工厂类配置，用于自定义恢复实现 */
  val RECOVERY_MODE_FACTORY = ConfigBuilder("spark.deploy.recoveryMode.factory")
    .version("1.2.0")
    .stringConf
    .createWithDefault("")

  /** 文件系统恢复模式下的持久化目录配置 */
  val RECOVERY_DIRECTORY = ConfigBuilder("spark.deploy.recoveryDirectory")
    .version("0.8.1")
    .stringConf
    .createWithDefault("")

  /** 集群恢复过程超时时间配置，单位秒 */
  val RECOVERY_TIMEOUT = ConfigBuilder("spark.deploy.recoveryTimeout")
    .doc("Configures the timeout for recovery process. The default value is the same " +
      s"with ${Worker.WORKER_TIMEOUT.key}.")
    .version("4.0.0")
    .timeConf(TimeUnit.SECONDS)
    .checkValue(_ > 0, "spark.deploy.recoveryTimeout must be positive.")
    .createOptional

  /** Zookeeper恢复模式下的Zookeeper连接地址配置 */
  val ZOOKEEPER_URL = ConfigBuilder("spark.deploy.zookeeper.url")
    .doc(s"When `${RECOVERY_MODE.key}` is set to ZOOKEEPER, this " +
      "configuration is used to set the zookeeper URL to connect to.")
    .version("0.8.1")
    .stringConf
    .createOptional

  /** Zookeeper恢复模式下的存储节点路径配置 */
  val ZOOKEEPER_DIRECTORY = ConfigBuilder("spark.deploy.zookeeper.dir")
    .version("0.8.1")
    .stringConf
    .createOptional

  /** Master内存中保留的已完成应用数量上限 */
  val RETAINED_APPLICATIONS = ConfigBuilder("spark.deploy.retainedApplications")
    .version("0.8.0")
    .intConf
    .createWithDefault(200)

  /** Master内存中保留的已完成Driver数量上限 */
  val RETAINED_DRIVERS = ConfigBuilder("spark.deploy.retainedDrivers")
    .version("1.1.0")
    .intConf
    .createWithDefault(200)

  /** 死亡Worker持久化数据保留的清理轮次 */
  val REAPER_ITERATIONS = ConfigBuilder("spark.dead.worker.persistence")
    .version("0.8.0")
    .intConf
    .createWithDefault(15)

  /** Executor启动失败后的最大重试次数 */
  val MAX_EXECUTOR_RETRIES = ConfigBuilder("spark.deploy.maxExecutorRetries")
    .version("1.6.3")
    .intConf
    .createWithDefault(10)

  /** 是否开启Driver跨Worker分散调度，开启后尽量将Driver调度到不同Worker节点 */
  val SPREAD_OUT_DRIVERS = ConfigBuilder("spark.deploy.spreadOutDrivers")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(true)

  /** 是否开启应用Executor跨Worker分散调度，开启后尽量将Executor分布到不同Worker节点 */
  val SPREAD_OUT_APPS = ConfigBuilder("spark.deploy.spreadOutApps")
    .version("0.6.1")
    .withAlternative("spark.deploy.spreadOut")
    .booleanConf
    .createWithDefault(true)

  /**
   * Worker选择策略枚举，定义Master分配Executor时可用的Worker排序规则
   */
  object WorkerSelectionPolicy extends Enumeration {
    val CORES_FREE_ASC, CORES_FREE_DESC, MEMORY_FREE_ASC, MEMORY_FREE_DESC, WORKER_ID = Value
  }

  /** Worker选择策略配置，决定Master分配Executor时选择Worker的规则 */
  val WORKER_SELECTION_POLICY = ConfigBuilder("spark.deploy.workerSelectionPolicy")
    .doc("A policy to assign executors on one of the assignable workers; " +
      "CORES_FREE_ASC to choose a worker with the least free cores, " +
      "CORES_FREE_DESC to choose a worker with the most free cores, " +
      "MEMORY_FREE_ASC to choose a worker with the least free memory, " +
      "MEMORY_FREE_DESC to choose a worker with the most free memory, " +
      "WORKER_ID to choose a worker with the smallest worker id. " +
      "CORES_FREE_DESC is the default behavior.")
    .version("4.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(WorkerSelectionPolicy.values.map(_.toString))
    .createWithDefault(WorkerSelectionPolicy.CORES_FREE_DESC.toString)

  /** 每个应用默认申请的Core数量，默认不限制使用所有可用Core */
  val DEFAULT_CORES = ConfigBuilder("spark.deploy.defaultCores")
    .version("0.9.0")
    .intConf
    .checkValue(_ > 0, "spark.deploy.defaultCores must be positive.")
    .createWithDefault(Int.MaxValue)

  /** 集群中同时运行的Driver最大数量 */
  val MAX_DRIVERS = ConfigBuilder("spark.deploy.maxDrivers")
    .doc("The maximum number of running drivers.")
    .version("4.0.0")
    .intConf
    .checkValue(_ > 0, "The maximum number of running drivers should be positive.")
    .createWithDefault(Int.MaxValue)

  /** 应用ID自增部分的模值，用于控制ID分段位数 */
  val APP_NUMBER_MODULO = ConfigBuilder("spark.deploy.appNumberModulo")
    .doc("The modulo for app number. By default, the next of `app-yyyyMMddHHmmss-9999` is " +
      "`app-yyyyMMddHHmmss-10000`. If we have 10000 as modulo, it will be " +
      "`app-yyyyMMddHHmmss-0000`. In most cases, the prefix `app-yyyyMMddHHmmss` is increased " +
      "already during creating 10000 applications.")
    .version("4.0.0")
    .intConf
    .checkValue(_ >= 1000, "The modulo for app number should be greater than or equal to 1000.")
    .createOptional

  /** Driver ID生成格式，基于Java String.format规则定义 */
  val DRIVER_ID_PATTERN = ConfigBuilder("spark.deploy.driverIdPattern")
    .doc("The pattern for driver ID generation based on Java `String.format` method. " +
      "The default value is `driver-%s-%04d` which represents the existing driver id string " +
      ", e.g., `driver-20231031224459-0019`. Please be careful to generate unique IDs")
    .version("4.0.0")
    .stringConf
    .checkValue(!_.format("20231101000000", 0).exists(_.isWhitespace), "Whitespace is not allowed.")
    .createWithDefault("driver-%s-%04d")

  /** 应用ID生成格式，基于Java String.format规则定义 */
  val APP_ID_PATTERN = ConfigBuilder("spark.deploy.appIdPattern")
    .doc("The pattern for app ID generation based on Java `String.format` method.. " +
      "The default value is `app-%s-%04d` which represents the existing app id string, " +
      "e.g., `app-20231031224509-0008`. Plesae be careful to generate unique IDs.")
    .version("4.0.0")
    .stringConf
    .checkValue(!_.format("20231101000000", 0).exists(_.isWhitespace), "Whitespace is not allowed.")
    .createWithDefault("app-%s-%04d")
}
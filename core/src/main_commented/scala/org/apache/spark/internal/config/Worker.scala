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

import java.util.concurrent.TimeUnit

/**
 * Standalone集群Worker节点相关配置项定义
 * 存储所有Worker节点可配置参数的配置构建器
 */
private[spark] object Worker {
  val SPARK_WORKER_PREFIX = "spark.worker"

  /** Worker分配资源文件路径配置，仅Standalone模式内部使用 */
  val SPARK_WORKER_RESOURCE_FILE =
    ConfigBuilder("spark.worker.resourcesFile")
    .internal()
    .doc("Path to a file containing the resources allocated to the worker. " +
      "The file should be formatted as a JSON array of ResourceAllocation objects. " +
      "Only used internally in standalone mode.")
    .version("3.0.0")
    .stringConf
    .createOptional

  /** Master检测Worker超时的时间，单位秒 */
  val WORKER_TIMEOUT = ConfigBuilder("spark.worker.timeout")
    .version("0.6.2")
    .longConf
    .createWithDefault(60)

  /** Worker向Master注册时，短间隔重试的次数 */
  val WORKER_INITIAL_REGISTRATION_RETRIES = ConfigBuilder("spark.worker.initialRegistrationRetries")
    .version("4.0.0")
    .internal()
    .doc("The number of retries to reconnect in short intervals (between 5 and 15 seconds).")
    .intConf
    .checkValue(_ > 0, "The number of initial registration retries should be positive")
    .createWithDefault(6)

  /** Worker向Master注册的最大重试次数 */
  val WORKER_MAX_REGISTRATION_RETRIES = ConfigBuilder("spark.worker.maxRegistrationRetries")
    .version("4.0.0")
    .internal()
    .doc("The max number of retries to reconnect. After spark.worker.initialRegistrationRetries " +
      "attempts, the interval is between 30 and 90 seconds.")
    .intConf
    .checkValue(_ > 0, "The max number of registration retries should be positive")
    .createWithDefault(16)

  /** Worker终止Driver进程的超时时间 */
  val WORKER_DRIVER_TERMINATE_TIMEOUT = ConfigBuilder("spark.worker.driverTerminateTimeout")
    .version("2.1.2")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("10s")

  /** 是否启用Worker自动清理过期应用数据 */
  val WORKER_CLEANUP_ENABLED = ConfigBuilder("spark.worker.cleanup.enabled")
    .version("1.0.0")
    .booleanConf
    .createWithDefault(true)

  /** Worker自动清理过期应用数据的间隔，单位秒 */
  val WORKER_CLEANUP_INTERVAL = ConfigBuilder("spark.worker.cleanup.interval")
    .version("1.0.0")
    .longConf
    .createWithDefault(60 * 30)

  /** 已完成应用数据的保留时长，单位秒 */
  val APP_DATA_RETENTION = ConfigBuilder("spark.worker.cleanup.appDataTtl")
    .version("1.0.0")
    .longConf
    .createWithDefault(7 * 24 * 3600)

  /** 是否优先使用配置文件中指定的Master地址用于注册 */
  val PREFER_CONFIGURED_MASTER_ADDRESS = ConfigBuilder("spark.worker.preferConfiguredMasterAddress")
    .version("2.2.1")
    .booleanConf
    .createWithDefault(false)

  /** Worker UI的监听端口 */
  val WORKER_UI_PORT = ConfigBuilder("spark.worker.ui.port")
    .version("1.1.0")
    .intConf
    .createOptional

  /** Worker UI页面保留的已完成Executor数量上限 */
  val WORKER_UI_RETAINED_EXECUTORS = ConfigBuilder("spark.worker.ui.retainedExecutors")
    .version("1.5.0")
    .intConf
    .createWithDefault(1000)

  /** Worker UI页面保留的已完成Driver数量上限 */
  val WORKER_UI_RETAINED_DRIVERS = ConfigBuilder("spark.worker.ui.retainedDrivers")
    .version("1.5.0")
    .intConf
    .createWithDefault(1000)

  /** 压缩日志文件长度缓存的大小，用于Worker UI显示 */
  val UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF =
    ConfigBuilder("spark.worker.ui.compressedLogFileLengthCacheSize")
      .version("2.0.2")
      .intConf
      .createWithDefault(100)

  /** 触发Worker停服下线的信号量 */
  val WORKER_DECOMMISSION_SIGNAL =
    ConfigBuilder("spark.worker.decommission.signal")
      .doc("The signal that used to trigger the worker to start decommission.")
      .version("3.2.0")
      .stringConf
      .createWithDefaultString("PWR")

  /** Worker ID生成格式模板，内部配置 */
  val WORKER_ID_PATTERN = ConfigBuilder("spark.worker.idPattern")
    .internal()
    .doc("The pattern for worker ID generation based on Java `String.format` method. The " +
      "default value is `worker-%s-%s-%d` which represents the existing worker id string, e.g.," +
      " `worker-20231109183042-[fe80::1%lo0]-39729`. Please be careful to generate unique IDs")
    .version("4.0.0")
    .stringConf
    .checkValue(!_.format("20231109000000", "host", 0).exists(_.isWhitespace),
      "Whitespace is not allowed.")
    .createWithDefaultString("worker-%s-%s-%d")
}
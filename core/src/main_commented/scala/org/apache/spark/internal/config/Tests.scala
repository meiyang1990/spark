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

/**
 * 测试相关配置项集合，存放Spark单元测试和集成测试专用的配置定义
 */
private[spark] object Tests {

  val TEST_USE_COMPRESSED_OOPS_KEY = "spark.test.useCompressedOops"

  /** 测试环境可用内存配置，默认取当前JVM最大内存 */
  val TEST_MEMORY = ConfigBuilder("spark.testing.memory")
    .version("1.6.0")
    .longConf
    .createWithDefault(Runtime.getRuntime.maxMemory)

  /** 测试环境动态分配调度开关配置，默认开启 */
  val TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED =
    ConfigBuilder("spark.testing.dynamicAllocation.schedule.enabled")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  /** 是否处于测试模式的配置，可选配置 */
  val IS_TESTING = ConfigBuilder("spark.testing")
    .version("1.0.1")
    .booleanConf
    .createOptional

  /** 测试环境是否禁用阶段重试配置，默认不关闭 */
  val TEST_NO_STAGE_RETRY = ConfigBuilder("spark.test.noStageRetry")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(false)

  /** 测试环境预留内存配置，可选配置 */
  val TEST_RESERVED_MEMORY = ConfigBuilder("spark.testing.reservedMemory")
    .version("1.6.0")
    .longConf
    .createOptional

  /** 测试集群主机数量配置，默认值为5 */
  val TEST_N_HOSTS = ConfigBuilder("spark.testing.nHosts")
    .version("3.0.0")
    .intConf
    .createWithDefault(5)

  /** 单台测试主机上执行器数量配置，默认值为4 */
  val TEST_N_EXECUTORS_HOST = ConfigBuilder("spark.testing.nExecutorsPerHost")
    .version("3.0.0")
    .intConf
    .createWithDefault(4)

  /** 单个测试执行器核心数配置，默认值为2 */
  val TEST_N_CORES_EXECUTOR = ConfigBuilder("spark.testing.nCoresPerExecutor")
    .version("3.0.0")
    .intConf
    .createWithDefault(2)

  /** 资源警告测试开关配置，默认关闭 */
  val RESOURCES_WARNING_TESTING = ConfigBuilder("spark.resources.warnings.testing")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)

  /** 资源配置文件管理器测试开关配置，默认关闭 */
  val RESOURCE_PROFILE_MANAGER_TESTING =
    ConfigBuilder("spark.testing.resourceProfileManager")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  // This configuration is used for unit tests to allow skipping the task cpus to cores validation
  // to allow emulating standalone mode behavior while running in local mode. Standalone mode
  // by default doesn't specify a number of executor cores, it just uses all the ones available
  // on the host.
  /** 测试环境跳过核心数验证开关，用于在本地模式模拟Standalone模式行为，默认关闭 */
  val SKIP_VALIDATE_CORES_TESTING =
    ConfigBuilder("spark.testing.skipValidateCores")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  /** 测试环境跳过外部排序 shuffle服务注册开关，用于测试启用shuffle服务但不需要真实服务端的场景，默认关闭 */
  val TEST_SKIP_ESS_REGISTER = ConfigBuilder("spark.testing.skipESSRegister")
    .version("4.0.0")
    .doc("None of Spark testing modes (local, local-cluster) enables shuffle service. So it is " +
      s"hard to test ${SHUFFLE_SERVICE_ENABLED.key} when you only want to test this flag but " +
      s"without the real server. This config provides a way to allow tests run with " +
      s"${SHUFFLE_SERVICE_ENABLED.key} enabled without registration failures.")
    .booleanConf
    .createWithDefault(false)

}
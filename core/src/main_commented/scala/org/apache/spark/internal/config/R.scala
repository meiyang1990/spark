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
 * SparkR相关配置项定义，包含R语言后端连接、线程、命令路径等配置参数
 */
private[spark] object R {

  /** R后端连接超时时间配置，单位毫秒 */
  val R_BACKEND_CONNECTION_TIMEOUT = ConfigBuilder("spark.r.backendConnectionTimeout")
    .version("2.1.0")
    .intConf
    .createWithDefault(6000)

  /** R后端线程池线程数量配置 */
  val R_NUM_BACKEND_THREADS = ConfigBuilder("spark.r.numRBackendThreads")
    .version("1.4.0")
    .intConf
    .createWithDefault(2)

  /** R后端心跳发送间隔配置，单位毫秒 */
  val R_HEARTBEAT_INTERVAL = ConfigBuilder("spark.r.heartBeatInterval")
    .version("2.1.0")
    .intConf
    .createWithDefault(100)

  /** SparkR使用的Rscript命令路径配置 */
  val SPARKR_COMMAND = ConfigBuilder("spark.sparkr.r.command")
    .version("1.5.3")
    .stringConf
    .createWithDefault("Rscript")

  /** 自定义Rscript命令路径配置，可选配置 */
  val R_COMMAND = ConfigBuilder("spark.r.command")
    .version("1.5.3")
    .stringConf
    .createOptional
}
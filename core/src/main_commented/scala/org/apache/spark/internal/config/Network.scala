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
 * 网络模块配置项定义对象，集中定义Spark所有网络通信相关的配置参数
 * 包括加密配置、RPC通信配置、超时配置等核心网络参数
 */
private[spark] object Network {

  /** 网络加密SASL降级开关，开启后加密失败时允许降级到SASL认证 */
  private[spark] val NETWORK_CRYPTO_SASL_FALLBACK =
    ConfigBuilder("spark.network.crypto.saslFallback")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(true)

  /** 是否开启网络传输加密功能 */
  private[spark] val NETWORK_CRYPTO_ENABLED =
    ConfigBuilder("spark.network.crypto.enabled")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  /** 是否开启远程读取NIO缓冲区转换，解决堆外内存读取问题 */
  private[spark] val NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION =
    ConfigBuilder("spark.network.remoteReadNioBufferConversion")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(false)

  /** 全局网络超时时间，默认120秒，用于所有网络连接的空闲超时 */
  private[spark] val NETWORK_TIMEOUT =
    ConfigBuilder("spark.network.timeout")
      .version("1.3.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  /** 网络超时检查间隔时间，单位毫秒，默认继承存储块管理器超时间隔配置 */
  private[spark] val NETWORK_TIMEOUT_INTERVAL =
    ConfigBuilder("spark.network.timeoutInterval")
      .version("1.3.2")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString(STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString)

  /** RPC请求ask操作的超时时间，可选配置，未设置时使用全局网络超时 */
  private[spark] val RPC_ASK_TIMEOUT =
    ConfigBuilder("spark.rpc.askTimeout")
      .version("1.4.0")
      .stringConf
      .createOptional

  /** RPC连接建立专用线程池大小，控制并发连接数 */
  private[spark] val RPC_CONNECT_THREADS =
    ConfigBuilder("spark.rpc.connect.threads")
      .version("1.6.0")
      .intConf
      .createWithDefault(64)

  /** 每个对等节点间的RPC IO连接数，控制长连接复用数量 */
  private[spark] val RPC_IO_NUM_CONNECTIONS_PER_PEER =
    ConfigBuilder("spark.rpc.io.numConnectionsPerPeer")
      .version("1.6.0")
      .intConf
      .createWithDefault(1)

  /** RPC IO处理线程数，可选配置，未设置时根据核心数自动计算 */
  private[spark] val RPC_IO_THREADS =
    ConfigBuilder("spark.rpc.io.threads")
      .version("1.6.0")
      .intConf
      .createOptional

  /** RPC端点查找超时时间，可选配置，未设置时使用全局网络超时 */
  private[spark] val RPC_LOOKUP_TIMEOUT =
    ConfigBuilder("spark.rpc.lookupTimeout")
      .version("1.4.0")
      .stringConf
      .createOptional

  /** RPC消息最大允许大小，单位MB，超过会被截断，防止OOM */
  private[spark] val RPC_MESSAGE_MAX_SIZE =
    ConfigBuilder("spark.rpc.message.maxSize")
      .version("2.0.0")
      .intConf
      .createWithDefault(128)

  /** Netty RPC消息分发器线程池大小，控制消息处理并发度，可选配置 */
  private[spark] val RPC_NETTY_DISPATCHER_NUM_THREADS =
    ConfigBuilder("spark.rpc.netty.dispatcher.numThreads")
      .version("1.6.0")
      .intConf
      .createOptional
}
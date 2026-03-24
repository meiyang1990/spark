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

package org.apache.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Network._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

/**
 * RPC远程调用工具类，提供Driver端点引用获取、超时配置、最大消息大小计算等公共能力
 * 为Spark内部RPC通信提供统一的配置和工具方法
 */
private[spark] object RpcUtils {

  /**
   * 根据名称获取Driver端注册的RPC端点引用，从配置中读取Driver地址信息
   * 
   * @param name 要获取的RPC端点名称
   * @param conf Spark配置对象，用于读取Driver主机和端口配置
   * @param rpcEnv 当前节点的RPC环境
   * @return Driver端对应RPC端点的引用
   */
  def makeDriverRef(name: String, conf: SparkConf, rpcEnv: RpcEnv): RpcEndpointRef = {
    val driverHost: String = conf.get(config.DRIVER_HOST_ADDRESS.key, "localhost")
    val driverPort: Int = conf.getInt(config.DRIVER_PORT.key, 7077)
    Utils.checkHost(driverHost)
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
  }

  /**
   * 根据传入的Driver地址信息获取Driver端注册的RPC端点引用
   * 
   * @param name 要获取的RPC端点名称
   * @param driverHost Driver主机地址
   * @param driverPort Driver服务端口
   * @param rpcEnv 当前节点的RPC环境
   * @return Driver端对应RPC端点的引用
   */
  def makeDriverRef(
      name: String,
      driverHost: String,
      driverPort: Int,
      rpcEnv: RpcEnv): RpcEndpointRef = {
    Utils.checkHost(driverHost)
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
  }

  /**
   * 获取RPC ask请求操作默认超时时间，从配置读取回退到通用网络超时
   * 
   * @param conf Spark配置对象
   * @return 封装后的RPC超时对象
   */
  def askRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq(RPC_ASK_TIMEOUT.key, NETWORK_TIMEOUT.key), "120s")
  }

  /**
   * 获取RPC端点查找操作默认超时时间，从配置读取回退到通用网络超时
   * 
   * @param conf Spark配置对象
   * @return 封装后的RPC超时对象
   */
  def lookupRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq(RPC_LOOKUP_TIMEOUT.key, NETWORK_TIMEOUT.key), "120s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /**
   * 根据配置计算RPC消息最大允许大小，转换为字节单位并做上限校验
   * 
   * @param conf Spark配置对象
   * @return 最大消息大小，单位为字节
   */
  def maxMessageSizeBytes(conf: SparkConf): Int = {
    val maxSizeInMB = conf.get(RPC_MESSAGE_MAX_SIZE)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"${RPC_MESSAGE_MAX_SIZE.key} should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }
}
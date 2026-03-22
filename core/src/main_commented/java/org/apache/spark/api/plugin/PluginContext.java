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

package org.apache.spark.api.plugin;

import java.io.IOException;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;

import org.apache.spark.SparkConf;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.resource.ResourceInformation;

/**
 * :: DeveloperApi ::
 * 文件: PluginContext.java
 * 所属模块: Spark核心模块(org.apache.spark.api.plugin)
 * 核心职责: 定义Spark插件上下文接口，为第三方自定义插件提供运行时环境信息和交互能力
 * 
 * 该接口提供了Spark加载的插件所需的上下文信息和操作能力。
 * 插件初始化时会获得该接口的实例，插件可以持有该实例用于后续与Driver端组件交互。
 * 每个插件拥有独立的上下文实例，指标和消息隔离，插件之间无法直接交互。
 * 
 * @since 3.0.0
 */
@DeveloperApi
public interface PluginContext {

  /**
   * 获取当前插件对应的指标注册中心，用于注册插件发布的自定义指标
   * @return 插件专属的MetricRegistry实例
   */
  MetricRegistry metricRegistry();

  /**
   * 获取当前Spark应用的配置对象
   * @return Spark应用配置实例
   */
  SparkConf conf();

  /**
   * 获取当前进程的Executor ID，Driver端该值标识Driver本身
   * @return Executor/Driver标识ID
   */
  String executorID();

  /**
   * 获取当前Spark进程用于网络通信的主机名
   * @return 主机名
   */
  String hostname();

  /**
   * 获取分配给当前Driver或Executor的自定义资源信息(GPU、FPGA等)
   * @return 资源名称到资源信息的映射表
   */
  Map<String, ResourceInformation> resources();

  /**
   * 发送异步消息到插件的Driver端组件
   * <p>
   * 发送后不等待回复，消息入队后立即返回，消息必须可序列化
   *
   * @param message 待发送的可序列化消息
   * @throws IOException 消息发送失败时抛出异常
   */
  void send(Object message) throws IOException;

  /**
   * 发送同步RPC请求到插件的Driver端组件
   * <p>
   * 发送后阻塞等待Driver端回复，或直到超时（由spark.rpc.askTimeout配置控制）
   * 如果Driver返回错误，会抛出对应异常，消息必须可序列化
   *
   * @param message 待发送的可序列化消息
   * @return Driver端组件返回的回复
   * @throws Exception 超时或Driver返回错误时抛出异常
   */
  Object ask(Object message) throws Exception;

}
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

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Spark驱动端插件接口，定义SparkPlugin在Driver端的扩展能力
 * 
 * 为第三方开发者提供Driver端插件扩展点，用于在Driver生命周期中插入自定义逻辑
 * 配合{@link SparkPlugin}和{@link ExecutorPlugin}实现全链路自定义扩展
 * 
 * @since 3.0.0
 */
@DeveloperApi
public interface DriverPlugin {

  /**
   * 初始化驱动端插件
   * <p>
   * 该方法在Driver初始化的早期阶段调用，早于任务调度器初始化，此时大部分Spark子系统尚未完成初始化
   * 调用会阻塞Driver初始化流程，因此建议避免在此执行 heavy 操作，可放到异步线程或延后到应用完全启动后执行
   *
   * @param sc 加载当前插件的SparkContext上下文对象
   * @param pluginContext 插件运行上下文，提供Spark应用相关信息和工具能力
   * @return 插件自定义配置信息，会传递给所有Executor端的{@link ExecutorPlugin#init(PluginContext,Map)}方法
   */
  default Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    return Collections.emptyMap();
  }

  /**
   * 向Spark指标系统注册插件自定义指标
   * <p>
   * 该方法在Driver初始化后期调用，此时大部分子系统已启动，应用ID已经生成
   * 插件注册到{@link PluginContext#metricRegistry()}中的指标会自动创建对应名称的指标源
   * <p>
   * 注意：此方法调用后再新增指标可能无法被正确采集，建议在此方法完成所有指标注册
   *
   * @param appId 集群管理器分配的当前Spark应用ID
   * @param pluginContext 插件运行上下文，提供Spark应用相关信息和工具能力
   */
  default void registerMetrics(String appId, PluginContext pluginContext) {}

  /**
   * 处理来自Executor端的RPC消息
   * <p>
   * 插件可以通过Spark内置RPC系统从Executor向Driver发送消息（当前不支持反向发送）
   * Executor端插件发送的消息会传递到该方法处理，返回值会作为响应回传给Executor
   * <p>
   * 若Executor等待响应，抛出的异常会作为错误回传给Executor；若无需响应则仅在Driver日志记录
   * <p>
   * 该方法必须实现为线程安全，所有插件共享RPC调度线程且会同步调用，避免在此执行耗时操作影响其他插件
   * <p>
   * Spark保证所有Executor启动前，Driver端已经完成初始化可以接收消息
   *
   * @param message 接收到的消息对象
   * @return 响应给调用方的返回值，若调用方不期待响应则忽略该值
   * @throws Exception 处理过程中发生的异常，会传递给请求方
   */
  default Object receive(Object message) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * 通知插件Spark应用即将关闭，用于执行资源清理
   * <p>
   * 该方法在Driver关闭阶段调用，建议不要再此调用Spark核心功能（如发送RPC消息）
   */
  default void shutdown() {}

}
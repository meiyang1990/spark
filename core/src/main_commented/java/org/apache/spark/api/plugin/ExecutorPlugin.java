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

import java.util.Map;

import org.apache.spark.TaskFailedReason;
import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 * Spark插件的Executor端组件接口，定义了插件在Executor进程需要实现的生命周期回调方法。
 * 开发者通过实现该接口扩展Executor侧的自定义功能，比如指标收集、任务监控等。
 *
 * @since 3.0.0
 */
@DeveloperApi
public interface ExecutorPlugin {

  /**
   * 初始化Executor端插件，在Executor进程启动阶段调用，会阻塞Executor初始化直到方法返回。
   * 需要注册 metrics 的插件应在此方法中将指标注册到上下文的metricRegistry中，后续注册不保证能被正确采集。
   *
   * @param ctx 插件运行所在Executor的上下文信息，提供运行环境访问能力
   * @param extraConf Driver端插件在初始化时传递给Executor的额外配置信息
   */
  default void init(PluginContext ctx, Map<String, String> extraConf) {}

  /**
   * 关闭清理插件资源，在Executor进程关闭阶段调用，会阻塞Executor关闭流程直到方法返回。
   */
  default void shutdown() {}

  /**
   * 任务开始执行前的回调钩子，在任务执行线程中调用。
   * 可通过TaskContext获取当前任务的上下文信息，不建议在此执行耗时操作，会影响整体作业性能。
   * 该方法抛出的异常会被捕获、日志记录后忽略，不会导致任务失败。
   *
   * @since 3.1.0
   */
  default void onTaskStart() {}

  /**
   * 任务成功完成后的回调钩子，即使onTaskStart抛出异常，该方法仍然会被调用。
   * 同样不建议在此执行耗时操作，避免影响作业性能。
   *
   * @since 3.1.0
   */
  default void onTaskSucceeded() {}

  /**
   * 任务执行失败后的回调钩子，同样不建议在此执行耗时操作，避免影响作业性能。
   *
   * @param failureReason 任务失败的原因信息
   *
   * @since 3.1.0
   */
  default void onTaskFailed(TaskFailedReason failureReason) {}
}
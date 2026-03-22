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

import org.apache.spark.annotation.DeveloperApi;

/**
 * 文件说明: Spark插件机制的核心接口，用于实现可动态加载到Spark应用中的自定义插件
 * 
 * :: DeveloperApi ::
 * A plugin that can be dynamically loaded into a Spark application.
 * <p>
 * Plugins can be loaded by adding the plugin's class name to the appropriate Spark configuration.
 * Check the Spark monitoring guide for details.
 * <p>
 * Plugins have two optional components: a driver-side component, of which a single instance is
 * created per application, inside the Spark driver. And an executor-side component, of which one
 * instance is created in each executor that is started by Spark. Details of each component can be
 * found in the documentation for {@link DriverPlugin} and {@link ExecutorPlugin}.
 *
 * 插件分为Driver端和Executor端两个可选组件，分别运行在Driver和每个Executor进程中
 * @since 3.0.0
 */
@DeveloperApi
public interface SparkPlugin {

  /**
   * 获取插件的Driver端组件实例
   *
   * @return 插件的Driver端组件，如果不需要Driver端组件则返回null
   */
  DriverPlugin driverPlugin();

  /**
   * 获取插件的Executor端组件实例
   *
   * @return 插件的Executor端组件，如果不需要Executor端组件则返回null
   */
  ExecutorPlugin executorPlugin();

}
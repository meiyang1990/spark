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

package org.apache.spark.shuffle.api;

import org.apache.spark.annotation.Private;

/**
 * 文件级注释：Shuffle数据IO插件接口，为SortShuffleManager提供可扩展的Shuffle临时数据存储后端
 * 
 * :: Private ::
 * 为存储和读取Shuffle临时数据提供可插拔的扩展接口。
 * <p>
 * 这是一个插件系统的根接口，允许将Shuffle数据存储到任意自定义存储后端，用于
 * {@link org.apache.spark.shuffle.sort.SortShuffleManager}实现的基于排序的Shuffle算法中。
 * 如果需要实现基于排序Shuffle之外的其他Shuffle算法，应该直接实现
 * {@link org.apache.spark.shuffle.ShuffleManager}接口。
 * <p>
 * 在Spark应用中，每个进程仅会加载一个该接口的实例。
 * 默认实现从执行器节点的本地磁盘读写Shuffle数据，这是Spark发展过程中一直沿用的Shuffle文件存储实现。
 * <p>
 * 自定义Shuffle数据存储实现可以通过配置项<code>spark.shuffle.sort.io.plugin.class</code>加载使用。
 * @since 3.0.0
 */
@Private
public interface ShuffleDataIO {

  /**
   * 获取执行器端Shuffle组件，在执行器进程启动时调用一次，初始化仅在执行器端使用的Shuffle数据存储模块
   * 
   * @return 执行器端Shuffle组件实例
   */
  ShuffleExecutorComponents executor();

  /**
   * 获取Driver端Shuffle组件，在Driver进程启动时调用一次，初始化由Driver维护的Shuffle元数据模块
   * 
   * @return Driver端Shuffle组件实例
   */
  ShuffleDriverComponents driver();
}
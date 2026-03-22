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

import java.util.Map;

import org.apache.spark.annotation.Private;

/**
 * 文件说明: Spark Shuffle 驱动端组件接口，定义Driver端需要实现的Shuffle相关功能，用于扩展自定义Shuffle存储服务
 * :: Private ::
 * 供Spark内部使用的Driver端Shuffle支持模块构建接口
 */
@Private
public interface ShuffleDriverComponents {

  /**
   * 在Driver端对当前应用的Shuffle模块进行初始化，在向集群管理器申请Executor之前调用
   * 负责完成Shuffle组件的前期准备工作，例如注册外部Shuffle服务、初始化存储元数据表等
   *
   * @return 需要传递给Executor端初始化的额外Spark配置项，通常包含无法静态配置的动态信息
   *         例如外部Shuffle存储服务的地址和端口等
   */
  Map<String, String> initializeApplication();

  /**
   * Spark应用结束时调用一次，负责清理所有Shuffle相关状态和资源
   */
  void cleanupApplication();

  /**
   * 当Shuffle ID第一次为Shuffle阶段生成时调用一次，用于注册新的Shuffle
   *
   * @param shuffleId Shuffle阶段的唯一标识ID
   */
  default void registerShuffle(int shuffleId) {}

  /**
   * 删除指定Shuffle对应的所有Shuffle数据
   *
   * @param shuffleId Shuffle阶段的唯一标识ID
   * @param blocking 是否阻塞等待删除操作完成
   */
  default void removeShuffle(int shuffleId, boolean blocking) {}

  /**
   * 检查当前Shuffle组件是否支持可靠存储，即存储生命周期独立于Executor节点
   * 例如将Shuffle数据写入分布式文件系统或持久化到远程Shuffle服务
   *
   * @return 是否支持可靠存储，默认返回false
   */
  default boolean supportsReliableStorage() {
    return false;
  }
}
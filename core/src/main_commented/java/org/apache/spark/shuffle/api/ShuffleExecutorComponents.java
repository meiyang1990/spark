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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.Private;

/**
 * 文件说明: Shuffle执行器组件接口，定义了Executor端Shuffle处理所需的核心能力
 * 核心职责: 为不同的Shuffle存储实现提供统一的Executor端扩展接口，支持自定义Shuffle后端
 * 
 * :: Private ::
 * An interface for building shuffle support for Executors.
 *
 * @since 3.0.0
 */
@Private
public interface ShuffleExecutorComponents {

  /**
   * 初始化Executor端的Shuffle组件，在每个Executor启动时调用一次
   * 传入应用和Executor标识，以及Driver端返回的额外配置信息
   *
   * @param appId Spark应用ID
   * @param execId 待初始化的Executor唯一标识
   * @param extraConfigs {@link ShuffleDriverComponents#initializeApplication()} 返回的额外配置
   */
  void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs);

  /**
   * 为每个Map任务创建Shuffle输出写入器，负责持久化该Map任务输出到各个reduce分区的字节数据
   *
   * @param shuffleId 当前Map任务所属Shuffle的唯一标识
   * @param mapTaskId Map任务ID，在整个Spark应用中唯一
   * @param numPartitions 当前Map任务需要写入的分区数量，部分分区可能为空
   * @return ShuffleMap输出写入器实例
   */
  ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) throws IOException;

  /**
   * 创建单文件Map输出写入器的可选扩展接口，用于优化将整个Map任务结果作为单个分区文件传输到后端存储的场景
   * 大多数实现不需要支持该优化，默认返回空即可，该优化主要用于兼容原本地磁盘Shuffle存储的优化实现
   *
   * @param shuffleId 当前Map任务所属Shuffle的唯一标识
   * @param mapId Map任务ID，在整个Spark应用中唯一
   * @return 单溢出ShuffleMap输出写入器的Optional包装，不支持则返回空
   */
  default Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) throws IOException {
    return Optional.empty();
  }
}
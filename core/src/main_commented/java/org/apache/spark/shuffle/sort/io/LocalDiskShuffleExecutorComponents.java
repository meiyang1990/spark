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

package org.apache.spark.shuffle.sort.io;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;

/**
 * 基于本地磁盘存储的Shuffle执行组件实现，用于排序Shuffle流程
 * 负责在Executor节点创建Map任务输出写入器，将Shuffle数据写入本地磁盘
 * 实现了ShuffleExecutorComponents扩展接口，支持基于本地磁盘的传统Shuffle存储
 */
public class LocalDiskShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;
  private IndexShuffleBlockResolver blockResolver;

  /**
   * 构造函数，仅传入Spark配置，初始化时会从SparkEnv获取依赖组件
   * @param sparkConf Spark配置对象
   */
  public LocalDiskShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  /**
   * 测试用构造函数，允许注入所有依赖组件，用于单元测试
   * @param sparkConf Spark配置对象
   * @param blockManager 块管理器实例
   * @param blockResolver 索引Shuffle块解析器实例
   */
  @VisibleForTesting
  public LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.blockResolver = blockResolver;
  }

  /**
   * 初始化Executor端Shuffle组件，从SparkEnv获取依赖并创建块解析器
   * @param appId 应用ID
   * @param execId Executor ID
   * @param extraConfigs 额外配置参数
   */
  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    // 从当前SparkEnv获取块管理器
    blockManager = SparkEnv.get().blockManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    // 创建索引Shuffle块解析器，用于处理Shuffle块的索引和数据文件
    blockResolver =
      new IndexShuffleBlockResolver(
        sparkConf, blockManager, new ConcurrentHashMap<>() /* Shouldn't be accessed */
      );
  }

  /**
   * 创建普通Map任务输出写入器，用于将Map结果按分区写入本地磁盘
   * @param shuffleId Shuffle阶段ID
   * @param mapTaskId Map任务ID
   * @param numPartitions 下游分区数量
   * @return 面向本地磁盘的Map输出写入器实例
   */
  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return new LocalDiskShuffleMapOutputWriter(
        shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  /**
   * 创建单文件溢出输出写入器，用于将整个Map输出合并写入单个文件的场景
   * @param shuffleId Shuffle阶段ID
   * @param mapId Map任务ID
   * @return 包装了本地单文件溢出写入器的Optional实例
   */
  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
  }
}
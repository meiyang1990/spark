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

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerMaster;

/**
 * 本地磁盘Shuffle的Driver端组件实现
 * 基于本地磁盘存储的Shuffle机制，提供Driver端初始化、清理和移除Shuffle数据的能力
 */
public class LocalDiskShuffleDriverComponents implements ShuffleDriverComponents {

  private BlockManagerMaster blockManagerMaster;

  /**
   * 初始化应用级的Shuffle驱动组件
   * @return 空配置映射
   */
  @Override
  public Map<String, String> initializeApplication() {
    // 从Spark环境获取BlockManager主节点引用
    blockManagerMaster = SparkEnv.get().blockManager().master();
    return Collections.emptyMap();
  }

  /**
   * 清理应用级Shuffle资源
   * 本地磁盘模式下无需额外清理资源
   */
  @Override
  public void cleanupApplication() {
    // nothing to clean up
  }

  /**
   * 移除指定ShuffleId对应的所有Shuffle数据
   * @param shuffleId 要移除的Shuffle编号
   * @param blocking 是否阻塞等待移除完成
   */
  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    // 检查组件是否已完成初始化
    if (blockManagerMaster == null) {
      throw new IllegalStateException("Driver components must be initialized before using");
    }
    // 委托BlockManagerMaster执行Shuffle数据移除
    blockManagerMaster.removeShuffle(shuffleId, blocking);
  }
}
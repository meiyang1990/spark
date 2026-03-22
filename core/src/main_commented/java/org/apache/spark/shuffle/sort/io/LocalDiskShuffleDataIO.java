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

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;

/**
 * 文件级注释：本地磁盘Shuffle数据IO实现，兼容Spark 2.4及更早版本的传统本地磁盘Shuffle存储机制
 *
 * 类级注释：实现ShuffleDataIO扩展接口，提供传统基于本地磁盘的Shuffle数据存储能力，
 * 包含数据文件和索引文件的存储逻辑，用于排序Shuffle的传统本地存储方案。
 */
public class LocalDiskShuffleDataIO implements ShuffleDataIO {

  private final SparkConf sparkConf;

  /**
   * 构造方法：初始化本地磁盘Shuffle数据IO，保存Spark配置对象
   * @param sparkConf Spark应用配置
   */
  public LocalDiskShuffleDataIO(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  /**
   * 获取执行器端Shuffle组件，提供执行器节点本地磁盘存储功能
   * @return 本地磁盘实现的执行器端Shuffle组件
   */
  @Override
  public ShuffleExecutorComponents executor() {
    return new LocalDiskShuffleExecutorComponents(sparkConf);
  }

  /**
   * 获取Driver端Shuffle组件，提供Driver端Shuffle元数据管理功能
   * @return 本地磁盘实现的Driver端Shuffle组件
   */
  @Override
  public ShuffleDriverComponents driver() {
    return new LocalDiskShuffleDriverComponents();
  }
}
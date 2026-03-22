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

package org.apache.spark.shuffle.api.metadata;

import java.util.Optional;

import org.apache.spark.annotation.Private;

/**
 * 文件说明：Shuffle阶段Map任务输出提交结果消息，定义在Spark Core Shuffle API元数据模块
 * 
 * :: Private ::
 * 表示Shuffle Map任务写入输出结果后的提交消息，承载Map任务输出的元数据信息
 * <p>
 * 分区长度数组保存了Map任务输出每个分区块的字节长度，下游读取器可以提前根据该信息分配资源（例如内存缓冲区）
 * <p>
 * Map输出写入器可以附加自定义元数据，注册到Shuffle输出追踪器中（该模块计划在后续Shuffle存储API迭代中实现）
 */
@Private
public final class MapOutputCommitMessage {

  // Map任务输出各个reduce分区的字节长度数组
  private final long[] partitionLengths;
  // 可选的自定义Map输出元数据
  private final Optional<MapOutputMetadata> mapOutputMetadata;

  /**
   * 构造Map输出提交消息
   * @param partitionLengths 各reduce分区输出长度数组
   * @param mapOutputMetadata 可选自定义元数据
   */
  private MapOutputCommitMessage(
      long[] partitionLengths, Optional<MapOutputMetadata> mapOutputMetadata) {
    this.partitionLengths = partitionLengths;
    this.mapOutputMetadata = mapOutputMetadata;
  }

  /**
   * 创建不带自定义元数据的Map输出提交消息
   * @param partitionLengths 各reduce分区输出长度数组
   * @return 构建完成的提交消息实例
   */
  public static MapOutputCommitMessage of(long[] partitionLengths) {
    return new MapOutputCommitMessage(partitionLengths, Optional.empty());
  }

  /**
   * 创建携带自定义元数据的Map输出提交消息
   * @param partitionLengths 各reduce分区输出长度数组
   * @param mapOutputMetadata 自定义Map输出元数据
   * @return 构建完成的提交消息实例
   */
  public static MapOutputCommitMessage of(
      long[] partitionLengths, MapOutputMetadata mapOutputMetadata) {
    return new MapOutputCommitMessage(partitionLengths, Optional.of(mapOutputMetadata));
  }

  /**
   * 获取Map任务输出各reduce分区的长度数组
   * @return 分区长度数组，下标对应reduce分区编号，值为对应分区输出字节长度
   */
  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * 获取可选的自定义Map输出元数据
   * @return 包含自定义元数据的Optional容器，无自定义元数据时返回空
   */
  public Optional<MapOutputMetadata> getMapOutputMetadata() {
    return mapOutputMetadata;
  }
}
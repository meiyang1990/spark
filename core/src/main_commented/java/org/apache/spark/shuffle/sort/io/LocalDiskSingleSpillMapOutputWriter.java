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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.util.Utils;

/**
 * 本地单溢出Map输出写入器，用于Shuffle排序阶段将单次溢出的Map输出直接写入本地磁盘
 * 核心职责是将已经按分区整理好的溢出文件直接移动到最终输出位置，无需合并操作
 */
public class LocalDiskSingleSpillMapOutputWriter
    implements SingleSpillShuffleMapOutputWriter {

  private final int shuffleId;
  private final long mapId;
  private final IndexShuffleBlockResolver blockResolver;

  /**
   * 构造本地磁盘单溢出输出写入器
   * @param shuffleId Shuffle阶段ID
   * @param mapId Map任务ID
   * @param blockResolver Shuffle块索引解析器，用于处理输出文件和元数据
   */
  public LocalDiskSingleSpillMapOutputWriter(
      int shuffleId,
      long mapId,
      IndexShuffleBlockResolver blockResolver) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
  }

  /**
   * 将Map任务溢出文件转移到最终输出目录，并写入索引元数据
   * 由于溢出文件已经是正确格式，直接移动无需重新合并分区
   * @param mapSpillFile 待转移的Map溢出文件，已经包含所有分区数据且格式正确
   * @param partitionLengths 各分区数据长度数组，用于构建索引文件
   * @param checksums 各分区校验和数组
   * @throws IOException 文件操作异常
   */
  @Override
  public void transferMapSpillFile(
      File mapSpillFile,
      long[] partitionLengths,
      long[] checksums) throws IOException {
    // The map spill file already has the proper format, and it contains all of the partition data.
    // So just transfer it directly to the destination without any merging.
    // 获取最终数据文件路径
    File outputFile = blockResolver.getDataFile(shuffleId, mapId);
    // 创建临时文件用于原子提交
    File tempFile = Utils.tempFileWith(outputFile);
    // 将溢出文件移动到临时文件位置
    Files.move(mapSpillFile.toPath(), tempFile.toPath());
    // 写入索引元数据并原子提交，将临时文件改为最终文件
    blockResolver
      .writeMetadataFileAndCommit(shuffleId, mapId, partitionLengths, checksums, tempFile);
  }
}
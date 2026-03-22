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

package org.apache.spark.shuffle.sort;

import java.io.File;

import org.apache.spark.storage.TempShuffleBlockId;

/**
 * 溢出文件元数据信息，存储ShuffleExternalSorter溢出到磁盘的数据块的元数据
 * 用于排序Shuffle过程中，记录一次内存溢出产生的磁盘文件信息
 */
final class SpillInfo {
  /** 各个分区在溢出文件中的长度数组 */
  final long[] partitionLengths;
  /** 溢出文件对象 */
  final File file;
  /** 临时Shuffle块ID */
  final TempShuffleBlockId blockId;

  /**
   * 构造溢出信息对象，初始化分区长度数组
   * @param numPartitions 分区数量
   * @param file 溢出到磁盘的文件
   * @param blockId 临时Shuffle块ID
   */
  SpillInfo(int numPartitions, File file, TempShuffleBlockId blockId) {
    this.partitionLengths = new long[numPartitions];
    this.file = file;
    this.blockId = blockId;
  }
}
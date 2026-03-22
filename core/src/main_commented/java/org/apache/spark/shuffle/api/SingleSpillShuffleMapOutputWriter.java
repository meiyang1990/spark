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

import java.io.File;
import java.io.IOException;

import org.apache.spark.annotation.Private;

/**
 * 文件级注释：Shuffle输出写入接口的扩展优化，针对单个溢出文件场景优化Map任务输出写入
 * 可选的分区写入扩展接口，针对将整个Map任务输出合并为单个文件传输到后端存储的场景做了优化。
 * 用于支持将Map任务所有分区输出合并后一次性溢出到存储，减少IO操作提升性能。
 */
@Private
public interface SingleSpillShuffleMapOutputWriter {

  /**
   * 传输包含当前Map任务所有分区数据的溢出文件，写入到后端存储
   * 将包含此Map任务所有分区字节数据的文件传输到Shuffle后端存储中。
   * @param mapOutputFile 包含所有分区数据的溢出文件
   * @param partitionLengths 每个分区在文件中的字节长度数组
   * @param checksums 每个分区数据的校验和数组
   * @throws IOException 文件传输过程中IO异常
   */
  void transferMapSpillFile(
      File mapOutputFile,
      long[] partitionLengths,
      long[] checksums) throws IOException;
}
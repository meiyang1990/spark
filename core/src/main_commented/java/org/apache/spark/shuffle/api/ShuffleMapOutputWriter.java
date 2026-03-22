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

import org.apache.spark.annotation.Private;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;

/**
 * 文件级：Shuffle Map任务输出写操作的顶层扩展接口，定义了Map阶段输出数据写入的核心契约
 * 
 * :: Private ::
 * 顶级写入器接口，用于为Map任务输出创建分区子写入器，并以原子操作提交所有写入结果
 * 是Spark Shuffle扩展点之一，允许自定义Shuffle输出存储实现
 *
 * @since 3.0.0
 */
@Private
public interface ShuffleMapOutputWriter {

  /**
   * 为指定Reduce分区创建对应的分区写入器，用于写入该分区的Map输出数据
   * <p>
   * 在同一个Map任务中，同一个Reduce分区不会调用两次该方法；分区ID范围为[0, numPartitions)，
   * numPartitions由创建该写入器时{@link ShuffleExecutorComponents#createMapOutputWriter(int, long, int)}指定。
   * <p>
   * 调用该方法时，传入的reducePartitionId严格单调递增；不保证为范围内每个分区ID都调用该方法，
   * 空分区不会调用该方法创建写入器。
   *
   * @param reducePartitionId 目标Reduce分区ID
   * @return 对应分区的写入器实例
   * @throws IOException 创建写入器失败时抛出IO异常
   */
  ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException;

  /**
   * 提交所有分区的写入操作，返回每个分区的输出字节统计信息和元数据
   * <p>
   * 提交后需要保证所有写入结果对下游Reduce任务可见；如果该方法抛出异常，会先调用{@link #abort(Throwable)}
   * 再向上传播异常。
   * <p>
   * 需要启用Shuffle校验的扩展实现需要正确存储校验和，用于后续数据 corruption 诊断。
   * <p>
   * 该方法还负责关闭资源和清理临时状态。
   * <p>
   * 返回的提交消息包含两部分：
   * <p>
   * 1) 长度为numPartitions的long数组，每个位置对应分区的写入字节数
   * <p>
   * 2) 可选的元数据Blob，可供Shuffle读取器使用
   *
   * @param checksums 每个分区的校验和数组，下标对应分区ID；未启用校验时为空数组
   * @return 包含分区输出统计和元数据的提交消息
   * @throws IOException 提交过程IO失败时抛出异常
   */
  MapOutputCommitMessage commitAllPartitions(long[] checksums) throws IOException;

  /**
   * 中止所有已执行的写入操作，清理资源和临时数据
   * <p>
   * 中止后需要使所有已写入结果无效，保证不会被下游任务读取，同时释放占用的资源
   *
   * @param error 导致中止的错误原因
   * @throws IOException 中止清理过程IO失败时抛出异常
   */
  void abort(Throwable error) throws IOException;
}
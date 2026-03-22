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
import java.util.Optional;
import java.io.OutputStream;

import org.apache.spark.annotation.Private;

/**
 * 文件级：Shuffle 分区写操作接口，定义了将单个Shuffle分区数据写入后端存储的规范
 * :: Private ::
 * 用于打开输出流将分区数据持久化到底层存储的接口
 * <p>
 * 该写入器负责存储一个(mapper, reducer)对对应的数据，对应一个Shuffle块
 *
 * @since 3.0.0
 */
@Private
public interface ShufflePartitionWriter {

  /**
   * 打开并返回输出流，用于将分区数据写入底层存储
   * <p>
   * 在Map任务中，该方法仅会被调用一次来写入当前分区的数据，输出流仅用于写入当前分区的数据
   * Map任务会在当前块所有数据写入完成后，或写入失败时关闭该输出流
   * <p>
   * 如果实现需要将当前Map任务的所有分区数据合并写入，应当复用父 {@link ShuffleMapOutputWriter}
   * 提供给所有分区写入器的同一个OutputStream实例。这种情况下需要确保 {@link OutputStream#close()}
   * 不关闭底层资源，因为该流会被多个分区复用。底层资源应当在 {@link ShuffleMapOutputWriter#commitAllPartitions(long[])}
   * 或 {@link ShuffleMapOutputWriter#abort(Throwable)} 中进行清理
   */
  OutputStream openStream() throws IOException;

  /**
   * 打开并返回可写字节通道包装器，支持从输入字节通道直接拷贝数据到Shuffle底层存储，用于零拷贝优化
   * <p>
   * 在Map任务中，该方法仅会被调用一次来写入当前分区的数据，通道仅用于写入当前分区的数据
   * Map任务会在当前块所有数据写入完成后，或写入失败时关闭该通道
   * <p>
   * 如果实现需要将当前Map任务的所有分区数据合并写入，应当复用父 {@link ShuffleMapOutputWriter}
   * 提供给所有分区写入器的同一个通道实例。这种情况下需要确保 {@link WritableByteChannelWrapper#close()}
   * 不关闭底层资源，因为该通道会被多个分区复用。底层资源应当在 {@link ShuffleMapOutputWriter#commitAllPartitions(long[])}
   * 或 {@link ShuffleMapOutputWriter#abort(Throwable)} 中进行清理
   * <p>
   * 该方法主要用于高级优化场景：可以直接将溢出文件中的数据拷贝到输出通道，不需要将数据拷贝到堆内存
   * 如果不支持该优化，实现应当返回 {@link Optional#empty()}，默认实现即返回空
   * <p>
   * 注意：返回的 {@link WritableByteChannelWrapper} 自身会被关闭，但 {@link WritableByteChannelWrapper#channel()}
   * 返回的底层通道不会关闭。需要确保底层通道在 {@link WritableByteChannelWrapper#close()}、
   * {@link ShuffleMapOutputWriter#commitAllPartitions(long[])} 或 {@link ShuffleMapOutputWriter#abort(Throwable)}
   * 中被清理
   */
  default Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
    return Optional.empty();
  }

  /**
   * 获取当前分区写入器实际写入到底层存储的字节数
   * <p>
   * 该值可能与调用方传入的字节数不同，例如写入过程中对数据进行了压缩或加密，实际存储字节数会发生变化
   */
  long getNumBytesWritten();
}
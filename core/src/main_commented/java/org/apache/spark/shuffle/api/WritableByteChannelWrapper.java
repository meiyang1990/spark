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

import java.io.Closeable;
import java.nio.channels.WritableByteChannel;

import org.apache.spark.annotation.Private;

/**
 * 文件说明: Spark Shuffle模块中可写字节通道的包装接口，属于Spark私有API
 * 
 * 该接口用于为本地磁盘Shuffle实现提供包装，支持跨多个分区写入时保持底层通道打开，
 * 避免重复打开关闭文件通道带来的性能开销。
 * 
 * :: Private ::
 * A thin wrapper around a {@link WritableByteChannel}.
 * <p>
 * This is primarily provided for the local disk shuffle implementation to provide a
 * {@link java.nio.channels.FileChannel} that keeps the channel open across partition writes.
 *
 * @since 3.0.0
 */
@Private
public interface WritableByteChannelWrapper extends Closeable {

  /**
   * 获取底层可用于写入字节的通道实例
   * @return 底层WritableByteChannel通道对象
   */
  WritableByteChannel channel();
}
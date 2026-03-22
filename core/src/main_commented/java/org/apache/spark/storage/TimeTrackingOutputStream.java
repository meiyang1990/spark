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

package org.apache.spark.storage;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.spark.annotation.Private;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;

/**
 * 文件：org.apache.spark.storage.TimeTrackingOutputStream
 * 职责：包装输出流，统计Shuffle写入操作耗时，更新Shuffle写入度量指标
 * 功能：拦截所有输出流操作，自动记录写入耗时，用于Spark Shuffle阶段的性能监控
 * 说明：非线程安全，仅用于单线程Shuffle写入场景
 */
/**
 * Intercepts write calls and tracks total time spent writing in order to update shuffle write
 * metrics. Not thread safe.
 */
@Private
public final class TimeTrackingOutputStream extends OutputStream {

  private final ShuffleWriteMetricsReporter writeMetrics;
  private final OutputStream outputStream;

  /**
   * 构造计时输出流，包装底层输出流并关联Shuffle度量报表
   * @param writeMetrics Shuffle写入度量报表，用于更新写入耗时
   * @param outputStream 底层实际输出流，实际写入操作委托给该流
   */
  public TimeTrackingOutputStream(
      ShuffleWriteMetricsReporter writeMetrics, OutputStream outputStream) {
    this.writeMetrics = writeMetrics;
    this.outputStream = outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    // 记录写入开始时间
    final long startTime = System.nanoTime();
    // 委托底层流执行单字节写入
    outputStream.write(b);
    // 统计耗时并更新到度量指标
    writeMetrics.incWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void write(byte[] b) throws IOException {
    // 记录写入开始时间
    final long startTime = System.nanoTime();
    // 委托底层流执行字节数组写入
    outputStream.write(b);
    // 统计耗时并更新到度量指标
    writeMetrics.incWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // 记录写入开始时间
    final long startTime = System.nanoTime();
    // 委托底层流执行偏移量写入
    outputStream.write(b, off, len);
    // 统计耗时并更新到度量指标
    writeMetrics.incWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void flush() throws IOException {
    // 记录刷盘开始时间
    final long startTime = System.nanoTime();
    // 委托底层流执行刷盘
    outputStream.flush();
    // 统计耗时并更新到度量指标
    writeMetrics.incWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void close() throws IOException {
    // 记录关闭开始时间
    final long startTime = System.nanoTime();
    // 委托底层流执行关闭
    outputStream.close();
    // 统计耗时并更新到度量指标
    writeMetrics.incWriteTime(System.nanoTime() - startTime);
  }
}
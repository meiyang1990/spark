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

package org.apache.spark.input

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.internal.Logging

/**
 * 定长二进制文件输入格式，用于读取每条记录占用固定字节数的二进制平面文件
 * 核心功能：按照指定字节长度切割记录，同时保证分片只包含完整记录，满足分布式读取需求
 */
private[spark] object FixedLengthBinaryInputFormat {
  /** Hadoop配置中记录长度参数的键名 */
  val RECORD_LENGTH_PROPERTY = "org.apache.spark.input.FixedLengthBinaryInputFormat.recordLength"

  /**
   * 从Hadoop配置中读取定长记录的长度配置
   * @param context Hadoop任务上下文
   * @return 配置的记录长度（字节）
   */
  def getRecordLength(context: JobContext): Int = {
    context.getConfiguration.get(RECORD_LENGTH_PROPERTY).toInt
  }
}

/**
 * 定长二进制文件的Hadoop InputFormat实现
 * 用于将包含固定长度记录的二进制文件切分为符合Hadoop MapReduce规范的分片，每个分片只包含完整记录
 * 支持Spark分布式读取定长二进制格式文件，每条记录作为单独一条数据处理
 */
private[spark] class FixedLengthBinaryInputFormat
  extends FileInputFormat[LongWritable, BytesWritable]
  with Logging {

  private var recordLength = -1

  /**
   * 判断文件是否可分片，同时从配置中加载记录长度
   * @param context Hadoop任务上下文
   * @param filename 输入文件路径
   * @return 文件是否可被切分为多个分片
   */
  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    if (recordLength == -1) {
      // 首次调用时加载配置的记录长度
      recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
    }
    if (recordLength <= 0) {
      logDebug("record length is less than 0, file cannot be split")
      false
    } else {
      true
    }
  }

  /**
   * 计算分片大小，保证分片只包含完整记录，分片起始和结束都对齐记录边界
   * @param blockSize HDFS块大小
   * @param minSize 分片最小大小
   * @param maxSize 分片最大大小
   * @return 对齐记录边界后的分片大小
   */
  override def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long = {
    val defaultSize = super.computeSplitSize(blockSize, minSize, maxSize)
    // 默认分片小于单条记录时，直接使用记录长度作为分片大小
    // 否则对齐分片大小为记录长度的整数倍，保证分片包含完整记录
    if (defaultSize < recordLength) {
      recordLength.toLong
    } else {
      defaultSize / recordLength * recordLength
    }
  }

  /**
   * 创建用于读取定长记录的RecordReader实例
   * @param split 输入分片
   * @param context 任务执行上下文
   * @return 定长二进制记录读取器
   */
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
      : RecordReader[LongWritable, BytesWritable] = {
    new FixedLengthBinaryRecordReader
  }
}
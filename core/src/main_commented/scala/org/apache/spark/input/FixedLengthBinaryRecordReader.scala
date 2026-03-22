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

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileSplit

/**
 * 定长二进制文件记录读取器，用于从固定长度记录格式的二进制文件中逐条读取记录
 * 
 * 由FixedLengthBinaryInputFormat创建，使用输入格式中配置的记录长度，
 * 从指定输入分片每次读取一条记录。
 * 
 * 每次调用nextKeyValue()会更新当前的键值对：
 * - 键: 记录在文件中的索引位置（Long类型）
 * - 值: 二进制记录内容（BytesWritable类型）
 */
private[spark] class FixedLengthBinaryRecordReader
  extends RecordReader[LongWritable, BytesWritable] {

  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var recordLength: Int = 0
  private var fileInputStream: FSDataInputStream = null
  private var recordKey: LongWritable = null
  private var recordValue: BytesWritable = null

  /**
   * 关闭输入流，释放资源
   */
  override def close(): Unit = {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  /**
   * 获取当前读取到的记录键
   * @return 当前记录的索引键
   */
  override def getCurrentKey: LongWritable = {
    recordKey
  }

  /**
   * 获取当前读取到的记录值
   * @return 当前记录的二进制内容
   */
  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  /**
   * 获取当前分片读取进度，范围[0.0, 1.0]
   * @return 读取进度百分比
   */
  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  /**
   * 初始化读取器，打开文件并定位到分片起始位置
   * @param inputSplit Hadoop输入分片
   * @param context 任务尝试上下文
   */
  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    // 转换为文件分片类型
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // 获取分片起始字节位置
    splitStart = fileSplit.getStart

    // 计算分片结束字节位置
    splitEnd = splitStart + fileSplit.getLength

    // 获取文件路径
    val file = fileSplit.getPath
    // 获取作业配置
    val conf = context.getConfiguration
    // 检查文件是否压缩
    val codec = new CompressionCodecFactory(conf).getCodec(file)
    if (codec != null) {
      throw new IOException("FixedLengthRecordReader does not support reading compressed files")
    }
    // 从配置获取定长记录长度
    recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
    // 获取文件系统
    val fs = file.getFileSystem(conf)
    // 打开输入文件
    fileInputStream = fs.open(file)
    // 定位到分片起始位置
    fileInputStream.seek(splitStart)
    // 初始化当前位置为分片起始位置
    currentPosition = splitStart
  }

  /**
   * 读取下一条定长记录，更新当前键值对
   * @return 成功读取到记录返回true，读取完分片结束返回false
   */
  override def nextKeyValue(): Boolean = {
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // 键为记录索引，由当前起始位置除以记录长度计算得到
    recordKey.set(currentPosition / recordLength)
    // 初始化值对象，懒加载
    if (recordValue == null) {
      recordValue = new BytesWritable(new Array[Byte](recordLength))
    }
    // 当前位置未超过分片结束，读取一条记录
    if (currentPosition < splitEnd) {
      // 获取存储记录内容的缓冲区
      val buffer = recordValue.getBytes
      // 读取完整定长字节到缓冲区
      fileInputStream.readFully(buffer)
      // 更新当前读取位置，向后移动一个记录长度
      currentPosition = currentPosition + recordLength
      // 返回成功读取标记
      return true
    }
    // 分片读取完成
    false
  }
}
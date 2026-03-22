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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat

/**
 * WholeTextFile 输入格式实现，基于Hadoop CombineFileInputFormat实现
 * 用于批量读取多个小文本文件，将每个整个文件作为一条记录输出：key为文件路径，value为文件完整内容
 * 主要用于Spark wholeTextFiles API读取大量小文件场景，通过合并小文件减少分区数量提升执行效率
 */
private[spark] class WholeTextFileInputFormat
  extends CombineFileInputFormat[Text, Text] with Configurable {

  /**
   * 判断文件是否可切分，整个文件作为一条记录，因此禁止切分
   * @param context 作业上下文
   * @param file 待读取文件路径
   * @return 固定返回false，表示不可切分
   */
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  /**
   * 创建记录读取器，使用可配置的合并文件记录读取器包装WholeTextFileRecordReader
   * @param split 输入分片
   * @param context 任务尝试上下文
   * @return 整文件读取的记录读取器实例
   */
  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Text, Text] = {
    val reader =
      new ConfigurableCombineFileRecordReader(split, context, classOf[WholeTextFileRecordReader])
    reader.setConf(getConf)
    reader
  }

  /**
   * 根据用户设置的最小分区数计算分片最大大小，兼容旧Hadoop API
   * 通过用户指定的最小分区数反推分片最大大小，同时适配节点和机架级最小分片大小配置
   * @param context 作业上下文
   * @param minPartitions 用户期望的最小分区数
   */
  def setMinPartitions(context: JobContext, minPartitions: Int): Unit = {
    // 获取所有输入文件状态
    val files = listStatus(context).asScala
    // 计算所有非目录文件的总长度
    val totalLen = files.map(file => if (file.isDirectory) 0L else file.getLen).sum
    // 根据总长度和最小分区数计算每个分片的最大大小
    val maxSplitSize = Math.ceil(totalLen * 1.0 /
      (if (minPartitions == 0) 1 else minPartitions)).toLong

    // 处理小文件场景：确保节点/机架级最小分片大小不超过计算得到的最大分片大小
    val config = context.getConfiguration
    val minSplitSizePerNode = config.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERNODE, 0L)
    val minSplitSizePerRack = config.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERRACK, 0L)

    if (maxSplitSize < minSplitSizePerNode) {
      super.setMinSplitSizeNode(maxSplitSize)
    }

    if (maxSplitSize < minSplitSizePerRack) {
      super.setMinSplitSizeRack(maxSplitSize)
    }
    super.setMaxSplitSize(maxSplitSize)
  }
}
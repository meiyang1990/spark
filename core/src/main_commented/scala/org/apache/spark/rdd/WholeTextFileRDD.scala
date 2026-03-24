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

package org.apache.spark.rdd

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.input.WholeTextFileInputFormat

/**
 * 文件级文本读取RDD，将整个文本文件作为一条记录处理
 * 
 * 核心职责：实现"整个文件为一条记录"的文本读取语义，每个分区对应一个完整文本文件，
 * 输出的键为文件路径，值为文件全部内容，适用于小文件批量读取场景
 */
private[spark] class WholeTextFileRDD(
    sc : SparkContext,
    inputFormatClass: Class[_ <: WholeTextFileInputFormat],
    keyClass: Class[Text],
    valueClass: Class[Text],
    conf: Configuration,
    minPartitions: Int)
  extends NewHadoopRDD[Text, Text](sc, inputFormatClass, keyClass, valueClass, conf) {

  /**
   * 生成RDD分区，每个分区对应一个输入文件（将单个文件整体作为一个分区）
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = {
    val conf = getConf
    // 遍历大量目录文件时listStatus操作较慢，启用多线程并行化加速
    conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    // 创建输入格式实例
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    inputFormat match {
      case configurable: Configurable =>
        // 配置Hadoop输入格式
        configurable.setConf(conf)
      case _ =>
    }
    // 创建Hadoop Job上下文
    val jobContext = new JobContextImpl(conf, jobId)
    // 设置最小分区数，控制输入分片粒度
    inputFormat.setMinPartitions(jobContext, minPartitions)
    // 获取输入分片（每个文件对应一个分片）
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    // 转换为Spark内部Partition格式
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}
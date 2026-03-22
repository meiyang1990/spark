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

/**
 * 二进制文件读取RDD实现
 * 核心职责：以二进制流方式批量读取整个二进制文件，每个文件作为一个记录输出
 * 属于Spark核心RDD模块，用于支持wholeBinaryFiles API读取二进制文件场景
 */
package org.apache.spark.rdd

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.JobContextImpl

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.input.StreamFileInputFormat

private[spark] class BinaryFileRDD[T](
    @transient private val sc: SparkContext,
    inputFormatClass: Class[_ <: StreamFileInputFormat[T]],
    keyClass: Class[String],
    valueClass: Class[T],
    conf: Configuration,
    minPartitions: Int)
  extends NewHadoopRDD[String, T](sc, inputFormatClass, keyClass, valueClass, conf) {

  /**
   * 计算并获取RDD的分区，基于文件输入切分生成分区
   * @return 分区数组，每个分区对应一个或一组二进制文件切分
   */
  override def getPartitions: Array[Partition] = {
    val conf = getConf
    // 遍历大量目录文件时listStatus()操作很慢，并行化该操作，使用CPU核心数作为线程数
    conf.setIfUnset(FileInputFormat.LIST_STATUS_NUM_THREADS,
      Runtime.getRuntime.availableProcessors().toString)
    // 创建输入格式实例
    val inputFormat = inputFormatClass.getConstructor().newInstance()
    inputFormat match {
      case configurable: Configurable =>
        // 如果输入格式支持可配置，注入Hadoop配置
        configurable.setConf(conf)
      case _ =>
    }
    // 创建Hadoop Job上下文
    val jobContext = new JobContextImpl(conf, jobId)
    // 根据用户指定的最小分区数设置输入切分参数
    inputFormat.setMinPartitions(sc, jobContext, minPartitions)
    // 获取Hadoop输入切分结果
    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)
    // 转换Hadoop切分为Spark NewHadoopPartition分区
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}
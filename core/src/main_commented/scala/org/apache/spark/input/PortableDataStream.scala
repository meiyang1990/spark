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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.jdk.CollectionConverters._

import com.google.common.io.Closeables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{CombineFileInputFormat, CombineFileRecordReader, CombineFileSplit}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.internal.config

/**
 * 文件输入格式抽象基类，支持将整个文件读取为流、字节数组或其他自定义格式，
 * 主要用于小文件合并读取场景，优化小文件处理性能。
 */
private[spark] abstract class StreamFileInputFormat[T]
  extends CombineFileInputFormat[String, T]
{
  /** 整个文件作为一个分区，不进行切分 */
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  /**
   * 根据用户指定的最小分区数调整分片大小，适配旧版Hadoop API，保证分区数量符合预期
   * @param sc Spark上下文
   * @param context MapReduce任务上下文
   * @param minPartitions 用户期望的最小分区数
   */
  def setMinPartitions(sc: SparkContext, context: JobContext, minPartitions: Int): Unit = {
    // 获取默认最大分片字节数配置
    val defaultMaxSplitBytes = sc.conf.get(config.FILES_MAX_PARTITION_BYTES)
    // 获取文件打开成本字节数配置
    val openCostInBytes = sc.conf.get(config.FILES_OPEN_COST_IN_BYTES)
    // 计算默认并行度，取Spark默认并行度和用户指定最小分区数的较大值
    val defaultParallelism = Math.max(sc.defaultParallelism, minPartitions)
    // 获取所有文件状态信息
    val files = listStatus(context).asScala
    // 计算所有非目录文件的总字节数，包含每个文件的打开成本
    val totalBytes = files.filterNot(_.isDirectory).map(_.getLen + openCostInBytes).sum
    // 计算每个核心应处理的字节数
    val bytesPerCore = totalBytes / defaultParallelism
    // 计算最终最大分片大小，取默认最大值和计算值的较小值
    val maxSplitSize = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))

    // 针对小文件场景，确保节点和机架级最小分片大小不超过计算出的最大分片大小
    val jobConfig = context.getConfiguration
    val minSplitSizePerNode = jobConfig.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERNODE, 0L)
    val minSplitSizePerRack = jobConfig.getLong(CombineFileInputFormat.SPLIT_MINSIZE_PERRACK, 0L)

    if (maxSplitSize < minSplitSizePerNode) {
      super.setMinSplitSizeNode(maxSplitSize)
    }
    if (maxSplitSize < minSplitSizePerRack) {
      super.setMinSplitSizeRack(maxSplitSize)
    }
    super.setMaxSplitSize(maxSplitSize)
  }

  def createRecordReader(split: InputSplit, taContext: TaskAttemptContext): RecordReader[String, T]

}

/**
 * 基于流的RecordReader抽象基类，用于将整个文件读取为流格式，供子类实现自定义解析逻辑
 */
private[spark] abstract class StreamBasedRecordReader[T](
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends RecordReader[String, T] {

  // 标记当前文件是否已处理完成
  private var processed = false

  private var key = ""
  private var value: T = null.asInstanceOf[T]

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}
  override def close(): Unit = {}

  override def getProgress: Float = if (processed) 1.0f else 0.0f

  override def getCurrentKey: String = key

  override def getCurrentValue: T = value

  override def nextKeyValue: Boolean = {
    if (!processed) {
      // 创建可序列化的便携数据流封装对象
      val fileIn = new PortableDataStream(split, context, index)
      // 调用子类实现的流解析逻辑得到结果
      value = parseStream(fileIn)
      // 将文件路径作为Record的key
      key = fileIn.getPath()
      processed = true
      true
    } else {
      false
    }
  }

  /**
   * 解析输入流并返回类型为T的结果，解析完成后关闭流
   * @param inStream 待解析的便携数据流
   * @return 解析后的结果对象
   */
  def parseStream(inStream: PortableDataStream): T
}

/**
 * 直接返回PortableDataStream本身的RecordReader实现，将整个文件以可序列化流的形式返回给用户处理
 */
private[spark] class StreamRecordReader(
    split: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends StreamBasedRecordReader[PortableDataStream](split, context, index) {

  def parseStream(inStream: PortableDataStream): PortableDataStream = inStream
}

/**
 * 用于读取PortableDataStream的输入格式实现，基于CombineFileInputFormat实现小文件合并
 */
private[spark] class StreamInputFormat extends StreamFileInputFormat[PortableDataStream] {
  override def createRecordReader(split: InputSplit, taContext: TaskAttemptContext)
    : CombineFileRecordReader[String, PortableDataStream] = {
    new CombineFileRecordReader[String, PortableDataStream](
      split.asInstanceOf[CombineFileSplit], taContext, classOf[StreamRecordReader])
  }
}

/**
 * 可序列化的便携数据流封装类，延迟打开文件，支持在Spark任务间序列化传输，
 * 解决Hadoop不可序列化对象的传输问题，适合整个文件作为一个分区读取的场景。
 * @param isplit 合并后的输入分片
 * @param context MapReduce任务尝试上下文
 * @param index 当前文件在合并分片中的索引
 */
class PortableDataStream(
    isplit: CombineFileSplit,
    context: TaskAttemptContext,
    index: Integer)
  extends Serializable {

  // 序列化Hadoop配置为字节数组，解决TaskAttemptContext不可序列化问题
  private val confBytes = {
    val baos = new ByteArrayOutputStream()
    context.getConfiguration.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  // 序列化CombineFileSplit为字节数组，解决分片不可序列化问题
  private val splitBytes = {
    val baos = new ByteArrayOutputStream()
    isplit.write(new DataOutputStream(baos))
    baos.toByteArray
  }

  // 延迟反序列化得到分片对象，仅在第一次使用时反序列化
  @transient private lazy val split = {
    val bais = new ByteArrayInputStream(splitBytes)
    val nsplit = new CombineFileSplit()
    nsplit.readFields(new DataInputStream(bais))
    nsplit
  }

  // 延迟反序列化得到Hadoop配置对象，仅在第一次使用时反序列化
  @transient private lazy val conf = {
    val bais = new ByteArrayInputStream(confBytes)
    val nconf = new Configuration(false)
    nconf.readFields(new DataInputStream(bais))
    nconf
  }

  // 延迟计算得到当前文件路径，仅在第一次使用时计算
  @transient private lazy val path = {
    val pathp = split.getPath(index)
    pathp.toString
  }

  /**
   * 打开当前文件返回DataInputStream，调用方负责使用后关闭流
   * @return 文件输入流
   */
  @Since("1.2.0")
  def open(): DataInputStream = {
    val pathp = split.getPath(index)
    val fs = pathp.getFileSystem(conf)
    fs.open(pathp)
  }

  /**
   * 将整个文件内容读取为字节数组
   * @return 文件内容字节数组
   */
  @Since("1.2.0")
  def toArray(): Array[Byte] = {
    val stream = open()
    try {
      stream.readAllBytes()
    } finally {
      Closeables.close(stream, true)
    }
  }

  /**
   * 获取当前文件的路径字符串
   * @return 文件路径
   */
  @Since("1.2.0")
  def getPath(): String = path

  /**
   * 获取反序列化后的Hadoop配置对象
   * @return Hadoop配置
   */
  @Since("2.2.0")
  def getConfiguration: Configuration = conf
}
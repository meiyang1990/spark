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

package org.apache.spark.internal.io

import scala.reflect.ClassTag

import org.apache.hadoop.mapreduce._

import org.apache.spark.SparkConf

/**
 * Hadoop写入配置工具抽象基类，负责为RDD写入Hadoop文件创建输出格式、提交协议、写入器等核心组件
 * 同时兼容旧版mapred API和新版mapreduce API两种Hadoop API
 *
 * 设计约定：
 * 1. 当传入错误的Hadoop API类型时，实现类应抛出[[IllegalArgumentException]]异常
 * 2. 所有实现类必须可序列化，因为Driver端创建的实例会被序列化发送到Executor端的任务中使用
 * 3. 实现类必须包含仅一个参数的构造器，参数类型为SerializableConfiguration或SerializableJobConf
 */
/**
 * 抽象Hadoop写入配置工具基类，定义了RDD写入Hadoop格式所需的所有核心操作接口
 * 
 * @tparam K 输出键类型
 * @tparam V 输出值类型
 */
abstract class HadoopWriteConfigUtil[K, V: ClassTag] extends Serializable {

  // --------------------------------------------------------------------------
  // 创建JobContext/TaskAttemptContext上下文对象
  // --------------------------------------------------------------------------

  /**
   * 创建Hadoop Job上下文对象
   * 
   * @param jobTrackerId JobTracker ID
   * @param jobId Job ID
   * @return Hadoop JobContext实例
   */
  def createJobContext(jobTrackerId: String, jobId: Int): JobContext

  /**
   * 创建Hadoop任务尝试上下文对象
   * 
   * @param jobTrackerId JobTracker ID
   * @param jobId Job ID
   * @param splitId 分片ID
   * @param taskAttemptId 任务尝试ID
   * @return Hadoop TaskAttemptContext实例
   */
  def createTaskAttemptContext(
      jobTrackerId: String,
      jobId: Int,
      splitId: Int,
      taskAttemptId: Int): TaskAttemptContext

  // --------------------------------------------------------------------------
  // 创建输出提交协议
  // --------------------------------------------------------------------------

  /**
   * 创建Hadoop MapReduce输出提交协议实例，负责处理输出文件的提交与清理
   * 
   * @param jobId Job ID
   * @return HadoopMapReduceCommitProtocol输出提交协议实例
   */
  def createCommitter(jobId: Int): HadoopMapReduceCommitProtocol

  // --------------------------------------------------------------------------
  // 创建并管理数据写入器
  // --------------------------------------------------------------------------

  /**
   * 初始化数据写入器
   * 
   * @param taskContext 任务尝试上下文
   * @param splitId 分片ID
   */
  def initWriter(taskContext: TaskAttemptContext, splitId: Int): Unit

  /**
   * 写入一条键值对数据
   * 
   * @param pair 待写入的键值对
   */
  def write(pair: (K, V)): Unit

  /**
   * 关闭写入器，完成写入操作
   * 
   * @param taskContext 任务尝试上下文
   */
  def closeWriter(taskContext: TaskAttemptContext): Unit

  // --------------------------------------------------------------------------
  // 初始化输出格式
  // --------------------------------------------------------------------------

  /**
   * 初始化Hadoop输出格式
   * 
   * @param jobContext Job上下文
   */
  def initOutputFormat(jobContext: JobContext): Unit

  // --------------------------------------------------------------------------
  // 验证Hadoop配置合法性
  // --------------------------------------------------------------------------

  /**
   * 验证Hadoop配置是否符合当前API要求
   * 
   * @param jobContext Job上下文
   * @param conf Spark配置
   */
  def assertConf(jobContext: JobContext, conf: SparkConf): Unit
}
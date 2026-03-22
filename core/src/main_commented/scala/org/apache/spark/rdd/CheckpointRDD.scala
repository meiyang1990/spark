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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext}

/**
 * 文件级注释：CheckpointRDD相关核心实现，提供检查点数据恢复使用的RDD抽象，
 * 属于Spark核心RDD模块，用于RDD检查点机制，将持久化到存储的检查点数据恢复为可计算的RDD
 */

/**
 * 用于恢复检查点数据的RDD分区实现，每个分区对应原RDD的一个分区
 * 
 * @param index 分区索引，对应原RDD分区的位置
 */
private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
 * 从存储中恢复检查点数据的抽象RDD基类，作为所有检查点恢复RDD的父类
 * 用于切断原RDD的依赖链，从检查点文件重新读取数据，实现长作业的容错与依赖裁剪
 * 
 * @tparam T RDD元素类型
 * @param sc Spark上下文实例
 */
private[spark] abstract class CheckpointRDD[T: ClassTag](sc: SparkContext)
  extends RDD[T](sc, Nil) {

  // CheckpointRDD本身不支持再次检查点，避免冗余持久化
  override def doCheckpoint(): Unit = { }
  override def checkpoint(): Unit = { }
  override def localCheckpoint(): this.type = this
}
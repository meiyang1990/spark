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

import scala.collection.mutable

import org.apache.spark.Partition
import org.apache.spark.annotation.DeveloperApi

/**
 * 文件说明：RDD分区合并（coalesce）操作核心抽象，定义分区合并的接口和数据结构
 * 用于RDD调整分区数量时的分组逻辑抽象，被coalesce/repartition操作使用
 */

/**
 * ::DeveloperApi::
 * 分区合并器Trait，定义对指定RDD进行分区合并的抽象接口
 * 不同实现类可实现不同的分区合并策略，用于调整RDD分区数量
 */
@DeveloperApi
trait PartitionCoalescer {

  /**
   * 对父RDD的分区执行分区合并操作
   *
   * @param maxPartitions 合并后期望的最大分区数
   * @param parent 需要合并分区的父RDD
   * @return 合并后的分区组数组，每个分区组对应合并后的一个新分区，包含若干原分区
   */
  def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup]
}

/**
 * ::DeveloperApi::
 * 合并后的分区组，用于聚合多个原分区形成一个新分区
 * @param prefLoc 该分区组的优先位置（所在节点位置偏好）
 */
@DeveloperApi
class PartitionGroup(val prefLoc: Option[String] = None) {
  val partitions = mutable.ArrayBuffer[Partition]()
  def numPartitions: Int = partitions.size
}
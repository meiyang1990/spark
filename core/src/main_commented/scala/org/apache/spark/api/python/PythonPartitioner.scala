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

package org.apache.spark.api.python

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

/**
 * 供Python API使用的分区器，专门处理长整型类型key的分区计算
 *
 * 存储Python端分区函数的唯一id，并将其纳入相等性比较。要求该id在程序生命周期内唯一，
 * 不被不同分区函数复用，可通过Python内置id()函数并保持分区函数引用来保证这一点。
 *
 * @param numPartitions 分区总数
 * @param pyPartitionFunctionId Python端分区函数的唯一标识id
 */
private[spark] class PythonPartitioner(
  override val numPartitions: Int,
  val pyPartitionFunctionId: Long)
  extends Partitioner {

  /**
   * 根据key计算对应的分区编号
   * @param key 待计算分区的key
   * @return 对应的分区索引
   */
  override def getPartition(key: Any): Int = key match {
    case null => 0
    // 不信任Python分区函数一定会返回合法分区ID，因此无论如何都做取模运算保证结果合法
    case key: Long => Utils.nonNegativeMod(key.toInt, numPartitions)
    case _ => Utils.nonNegativeMod(key.hashCode(), numPartitions)
  }

  /**
   * 判断两个分区器是否相等，基于分区总数和Python分区函数id判断
   * @param other 待比较的对象
   * @return 是否相等
   */
  override def equals(other: Any): Boolean = other match {
    case h: PythonPartitioner =>
      h.numPartitions == numPartitions && h.pyPartitionFunctionId == pyPartitionFunctionId
    case _ =>
      false
  }

  /**
   * 计算分区器的哈希值，基于分区总数和Python分区函数id计算
   * @return 哈希值
   */
  override def hashCode: Int = 31 * numPartitions + pyPartitionFunctionId.hashCode
}
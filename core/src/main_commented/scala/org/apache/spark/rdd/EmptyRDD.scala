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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.errors.SparkCoreErrors

/**
 * 文件说明：实现空RDD，不包含任何分区和数据元素，用于表示空结果的分布式数据集
 *
 * 空RDD类，代表一个没有任何分区、没有任何数据元素的弹性分布式数据集
 * 通常用于过滤操作后结果为空、计算生成空结果等场景
 *
 * @tparam T RDD存储的数据元素类型
 */
private[spark] class EmptyRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {

  /**
   * 获取空RDD的所有分区，返回空数组
   * @return 空分区数组
   */
  override def getPartitions: Array[Partition] = Array.empty

  /**
   * 计算指定分区的数据，空RDD不应该被执行计算，直接抛出异常
   * @param split 待计算的分区
   * @param context 任务上下文
   * @return 永远不返回，直接抛出异常
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw SparkCoreErrors.emptyRDDError()
  }
}
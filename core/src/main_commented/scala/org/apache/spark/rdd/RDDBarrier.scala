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

import org.apache.spark.{PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}

/**
 * 文件描述：RDD屏障阶段包装类，实现屏障调度模式，支持所有任务同时启动执行，主要用于分布式深度学习、需要全局同步的计算场景
 */

/**
 * :: Experimental ::
 * 屏障阶段RDD包装器，强制Spark同步启动当前阶段的所有任务，用于需要全局同步的分布式计算场景
 * 由[[org.apache.spark.rdd.RDD#barrier]]方法创建实例
 *
 * @tparam T 原始RDD元素类型
 * @param rdd 要包装的原始RDD
 */
@Experimental
@Since("2.4.0")
class RDDBarrier[T: ClassTag] private[spark] (rdd: RDD[T]) {

  /**
   * :: Experimental ::
   * 在屏障阶段内对RDD每个分区执行map操作，所有任务同步启动，接口语义与标准mapPartitions一致
   *
   * @tparam S 输出RDD元素类型
   * @param f 分区处理函数，输入为原分区迭代器，输出为新分区迭代器
   * @param preservesPartitioning 是否保留原分区划分
   * @return 处理后生成的新RDD，标记为屏障阶段
   * @see [[org.apache.spark.BarrierTaskContext]]
   */
  @Experimental
  @Since("2.4.0")
  def mapPartitions[S: ClassTag](
      f: Iterator[T] => Iterator[S],
      preservesPartitioning: Boolean = false): RDD[S] = rdd.withScope {
    // 清理函数闭包中不必要的引用，优化序列化性能
    val cleanedF = rdd.sparkContext.clean(f)
    new MapPartitionsRDD(
      rdd,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning,
      isFromBarrier = true
    )
  }

  /**
   * :: Experimental ::
   * 在屏障阶段内对RDD每个分区执行带分区索引的map操作，所有任务同步启动，接口语义与标准mapPartitionsWithIndex一致
   *
   * @tparam S 输出RDD元素类型
   * @param f 分区处理函数，参数为分区索引和原分区迭代器，输出为新分区迭代器
   * @param preservesPartitioning 是否保留原分区划分
   * @return 处理后生成的新RDD，标记为屏障阶段
   * @see [[org.apache.spark.BarrierTaskContext]]
   */
  @Experimental
  @Since("3.0.0")
  def mapPartitionsWithIndex[S: ClassTag](
      f: (Int, Iterator[T]) => Iterator[S],
      preservesPartitioning: Boolean = false): RDD[S] = rdd.withScope {
    // 清理函数闭包中不必要的引用，优化序列化性能
    val cleanedF = rdd.sparkContext.clean(f)
    new MapPartitionsRDD(
      rdd,
      (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning,
      isFromBarrier = true
    )
  }

  /**
   * :: DeveloperApi ::
   * 使用自定义分区评估器工厂在屏障阶段处理每个分区，评估器工厂会序列化发送到执行器，每个任务创建评估器实例处理分区数据
   * 用于支持复杂的分区处理逻辑，允许在执行器侧初始化评估器资源
   *
   * @tparam U 输出RDD元素类型
   * @param evaluatorFactory 分区评估器工厂
   * @return 处理后生成的新RDD，标记为屏障阶段
   */
  @DeveloperApi
  @Since("3.5.0")
  def mapPartitionsWithEvaluator[U: ClassTag](
      evaluatorFactory: PartitionEvaluatorFactory[T, U]): RDD[U] = rdd.withScope {
    new MapPartitionsWithEvaluatorRDD(rdd, evaluatorFactory)
  }
  // TODO: [SPARK-25247] add extra conf to RDDBarrier, e.g., timeout.
}
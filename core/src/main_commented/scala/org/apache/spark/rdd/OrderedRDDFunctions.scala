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

import org.apache.spark.{InterruptibleIterator, Partitioner, RangePartitioner, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.ExternalSorter

/**
 * 为可排序key的(key, value)对RDD提供额外排序相关功能，通过隐式转换生效。
 * 只要key类型K在当前作用域存在隐式Ordering[K]即可使用，标准基本类型默认已提供Ordering实现。
 * 用户也可以为自定义类型自定义排序规则，或覆盖默认排序，规则采用最近作用域的隐式排序对象。
 * 
 * 使用示例：
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) =
 *       a.toLowerCase(Locale.ROOT).compare(b.toLowerCase(Locale.ROOT))
 *   }
 *
 *   // 使用上述不区分大小写的排序规则按key排序
 *   rdd.sortByKey()
 * }}}
 * 
 * @tparam K RDD中key的类型，需要支持排序
 * @tparam V RDD中value的类型
 * @tparam P RDD中元素类型，必须是Product2[K, V]的子类，通常为Tuple2[K, V]
 * @param self 待扩展功能的原始RDD
 */
class OrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: RDD[P])
  extends Logging with Serializable {
  private val ordering = implicitly[Ordering[K]]

  /**
   * 按key对RDD进行全局排序，结果每个分区包含一个有序范围的元素。
   * 对结果RDD调用collect或save操作会返回有序的记录列表；
   * 保存到文件系统时，会按key顺序生成多个part-X文件。
   *
   * @param ascending 是否升序排序，默认为true
   * @param numPartitions 排序结果的分区数，默认为原RDD的分区数
   * @return 按key排序后的RDD
   */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
  {
    // 创建Range分区器，根据key范围划分分区
    val part = new RangePartitioner(numPartitions, self, ascending)
    // 基于Range分区器创建ShuffleRDD，并设置对应排序规则
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
   * 根据给定分区器重新划分RDD，并在每个结果分区内按key排序记录。
   * 该方法比先repartition再在每个分区内排序更高效，因为可以将排序下推到Shuffle阶段完成。
   *
   * @param partitioner 分区器，用于重新划分分区
   * @return 重分区并按key排序后的RDD
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    // 如果原RDD已经使用目标分区器，直接在原分区内排序
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        // 获取当前任务上下文
        val context = TaskContext.get()
        // 创建外部排序器，使用当前key排序规则
        val sorter = new ExternalSorter[K, V, V](context, None, None, Some(ordering))
        // 插入所有元素并更新指标，返回可中断迭代器
        new InterruptibleIterator(context,
          sorter.insertAllAndUpdateMetrics(iter).asInstanceOf[Iterator[(K, V)]])
      }, preservesPartitioning = true)
    } else {
      // 否则重新Shuffle分区，并在Shuffle阶段完成排序
      new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
    }
  }

  /**
   * 过滤RDD，只保留key在[lower, upper]闭区间内的元素。
   * 如果RDD已经使用RangePartitioner分区，可以通过只扫描可能包含匹配元素的分区实现高效过滤；
   * 否则退化为对全部分区执行标准filter操作。
   *
   * @param lower 范围下界（包含）
   * @param upper 范围上界（包含）
   * @return 过滤后的RDD，仅包含key在指定范围内的元素
   */
  def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {
    // 判断key是否在目标范围内
    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    // 根据是否为Range分区决定是否裁剪分区
    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[_, _]) =>
        // 获取上下界对应的分区索引，确定需要扫描的分区范围
        val partitionIndices = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        // 创建分区裁剪RDD，只保留目标范围内的分区
        PartitionPruningRDD.create(self, partitionIndices.contains)
      case _ =>
        // 非Range分区，使用原RDD全量过滤
        self
    }
    // 对保留的分区执行过滤，只保留范围内元素
    rddToFilter.filter { case (k, v) => inRange(k) }
  }

}
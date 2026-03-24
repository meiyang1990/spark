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

import java.util.{HashMap => JHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext

/**
 * 文件: core/src/main/scala/org/apache/spark/rdd/SubtractedRDD.scala
 * 描述: 实现RDD集合差集操作的优化版本RDD，相比通用cogroup实现更高效，特别适用于第一个RDD远小于第二个RDD的场景
 */
/**
 * 针对集合差集操作优化的RDD实现
 * 相较于直接使用cogroup实现差集，本实现做了内存优化：仅将第一个RDD的数据放入内存，第二个RDD流式处理，
 * 当第一个RDD远小于第二个RDD时，能大幅降低内存占用，提升计算效率
 * 
 * @param rdd1 被减的输入RDD，存储(Key, Value)对
 * @param rdd2 要减去的输入RDD，存储(Key, Value)对
 * @param part 输出结果的分区器
 * @tparam [K] Key的类型
 * @tparam [V] rdd1中Value的类型
 * @tparam [W] rdd2中Value的类型
 */
private[spark] class SubtractedRDD[K: ClassTag, V: ClassTag, W: ClassTag](
    @transient var rdd1: RDD[_ <: Product2[K, V]],
    @transient var rdd2: RDD[_ <: Product2[K, W]],
    part: Partitioner)
  extends RDD[(K, V)](rdd1.context, Nil) {

  /**
   * 获取当前RDD依赖列表，根据输入RDD是否已匹配目标分区器决定使用窄依赖还是Shuffle依赖
   * @return 依赖列表，包含rdd1和rdd2对应的依赖
   */
  override def getDependencies: Seq[Dependency[_]] = {
    def rddDependency[T1: ClassTag, T2: ClassTag](rdd: RDD[_ <: Product2[T1, T2]])
      : Dependency[_] = {
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[T1, T2, Any](rdd, part)
      }
    }
    Seq(rddDependency[K, V](rdd1), rddDependency[K, W](rdd2))
  }

  /**
   * 获取当前RDD的所有分区，根据分区器的分区数量生成CoGroupPartition
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- array.indices) {
      // 每个输出分区同时依赖rdd1和rdd2对应分区
      array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  /**
   * 计算指定分区的数据，执行差集逻辑：先加载rdd1所有key到内存，再移除rdd2中出现的key，最终输出剩余key对应的结果
   * @param p 待计算分区
   * @param context 任务上下文
   * @return 差集计算结果迭代器
   */
  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    // 存储rdd1的key和对应value列表
    val map = new JHashMap[K, ArrayBuffer[V]]
    // 获取key对应的value列表，不存在则新建
    def getSeq(k: K): ArrayBuffer[V] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = new ArrayBuffer[V]()
        map.put(k, seq)
        seq
      }
    }
    // 集成处理指定依赖的所有数据，应用指定操作
    def integrate(depNum: Int, op: Product2[K, V] => Unit): Unit = {
      dependencies(depNum) match {
        case oneToOneDependency: OneToOneDependency[_] =>
          // 窄依赖直接获取对应分区迭代器
          val dependencyPartition = partition.narrowDeps(depNum).get.split
          oneToOneDependency.rdd.iterator(dependencyPartition, context)
            .asInstanceOf[Iterator[Product2[K, V]]].foreach(op)

        case shuffleDependency: ShuffleDependency[_, _, _] =>
          // Shuffle依赖从Shuffle管理器读取数据
          val metrics = context.taskMetrics().createTempShuffleReadMetrics()
          val iter = SparkEnv.get.shuffleManager
            .getReader(
              shuffleDependency.shuffleHandle,
              partition.index,
              partition.index + 1,
              context,
              metrics)
            .read()
          iter.foreach(op)
      }
    }

    // 第一个依赖是rdd1：将所有key-value加入内存哈希表
    integrate(0, t => getSeq(t._1) += t._2)
    // 第二个依赖是rdd2：移除所有rdd2中出现过的key
    integrate(1, t => map.remove(t._1))
    // 展开剩余结果，输出每个key对应每个value
    map.asScala.iterator.flatMap(t => t._2.iterator.map((t._1, _)))
  }

  /**
   * 清空依赖引用，帮助GC回收内存
   */
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

}
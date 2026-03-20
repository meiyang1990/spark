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

import java.io._

import scala.collection.Map
import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * ParallelCollectionRDD 的分区实现
 * 
 * 直接持有分配给该分区的数据序列，是最简单的分区实现
 * 
 * 【序列化优化】
 * 自定义了 writeObject/readObject 方法，可以利用 Spark 配置的序列化器
 * （如 Kryo）而不仅仅是 Java 序列化，提高序列化效率
 * 
 * @param rddId 所属 RDD 的唯一 ID
 * @param slice 分区索引
 * @param values 分区包含的数据序列
 */
private[spark] class ParallelCollectionPartition[T: ClassTag](
    var rddId: Long,
    var slice: Int,
    var values: Seq[T]
  ) extends Partition with Serializable {

  // 返回分区数据的迭代器
  def iterator: Iterator[T] = values.iterator

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice

  /**
   * 自定义序列化：根据配置的序列化器决定序列化方式
   * 
   * 如果使用 Java 序列化器，直接使用默认序列化（避免额外开销）
   * 否则使用 Spark 配置的序列化器（如 Kryo）进行嵌套序列化
   */
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer

    sfactory match {
      // Java 序列化器直接使用默认方式，避免产生额外的序列化头
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        // 其他序列化器：先写元数据，再用嵌套流写数据
        out.writeLong(rddId)
        out.writeInt(slice)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
  }

  /**
   * 自定义反序列化：与 writeObject 对应
   */
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[Seq[T]]())
    }
  }
}

/**
 * 并行集合 RDD —— sc.parallelize() 和 sc.makeRDD() 的底层实现
 * 
 * 【核心功能】
 * 将 Driver 端的本地集合（如 Array、List、Range）转换为分布式的 RDD
 * 数据会被切分成多个分区，分发到各个 Executor 上并行处理
 * 
 * 【使用场景】
 * - 将小规模本地数据并行化处理
 * - 创建测试数据集
 * - 与其他 RDD 进行 union、join 等操作
 * 
 * 【性能注意】
 * - 每个分区的数据会随 Task 一起序列化传输
 * - 对于大数据集，建议使用 checkpoint 到 HDFS 以减少重复传输
 * - 如果数据量很大，应该直接从分布式存储读取，而不是 parallelize
 * 
 * @param sc SparkContext
 * @param data 要并行化的本地数据集合
 * @param numSlices 切分的分区数
 * @param locationPrefs 各分区的首选位置（用于数据本地化调度）
 */
private[spark] class ParallelCollectionRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
    extends RDD[T](sc, Nil) {
  // TODO: 目前每个分区会携带完整数据，即使后续 RDD 链中被缓存
  // 可以考虑将数据写入 DFS 文件，在分区中读取以减少数据传输
  // UPDATE: 可以通过 checkpoint 到 HDFS 实现这个目标

  /**
   * 获取所有分区
   * 
   * 使用伴生对象的 slice 方法将数据切分成 numSlices 个部分
   * 每个部分封装为一个 ParallelCollectionPartition
   */
  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  /**
   * 计算分区数据
   * 
   * 直接返回分区内存储的数据迭代器
   * 使用 InterruptibleIterator 包装以支持任务取消
   */
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
  }

  /**
   * 获取分区的首选位置
   * 
   * 从 locationPrefs 映射中查找，如果没有指定则返回空
   * 用户可以通过 sc.makeRDD(data, preferredLocations) 指定
   */
  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

/**
 * ParallelCollectionRDD 的伴生对象
 * 
 * 提供将集合切分为多个子集合的 slice 方法
 */
private object ParallelCollectionRDD {
  /**
   * 将集合切分为 numSlices 个子集合
   * 
   * 【核心设计】
   * 对 Range 类型进行特殊处理，将切片也编码为 Range 以最小化内存开销
   * 这使得 Spark 可以高效地处理大范围数字集合
   * 例如：sc.parallelize(1 to 1000000000, 1000) 不会真正创建 10 亿个元素
   * 
   * 【切分算法】
   * 使用 positions 函数计算每个切片的起止索引，确保：
   * - 切片大小尽量均匀
   * - 切片边界在相同索引位置（对于 zip 操作很重要）
   * 
   * 【Range 优化】
   * - scala.collection.immutable.Range：直接创建新的 Range 对象
   * - NumericRange（Long、Double、BigInteger 等）：逐个切分
   * - 普通 Seq：转换为数组后按索引切分
   * 
   * @param seq 要切分的集合
   * @param numSlices 目标切片数
   * @return 切片后的子集合序列
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
    // 计算切片边界的辅助函数
    // 确保所有序列在相同的索引位置被切分，这对于 RDD.zip() 等操作至关重要
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      // Range 特殊处理：创建新的 Range 子对象，内存开销极小
      case r: Range =>
        positions(r.length, numSlices).zipWithIndex.map { case ((start, end), index) =>
          // 如果原 Range 是包含边界的（inclusive），最后一个切片也要包含边界
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          } else {
            new Range.Inclusive(r.start + start * r.step, r.start + (end - 1) * r.step, r.step)
          }
        }.toSeq.asInstanceOf[Seq[Seq[T]]]
      // NumericRange 处理：适用于 Long、Double、BigInteger 等类型的 Range
      case nr: NumericRange[T] =>
        val slices = new ArrayBuffer[Seq[T]](numSlices)
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start
          slices += r.take(sliceSize).asInstanceOf[Seq[T]]
          r = r.drop(sliceSize)
        }
        slices.toSeq
      // 通用 Seq 处理：先转数组避免 O(n²) 的索引访问（如 List）
      case _ =>
        val array = seq.toArray // 转数组避免 List 等的 O(n²) 操作
        positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toImmutableArraySeq
        }.toSeq
    }
  }
}

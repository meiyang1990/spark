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

package org.apache.spark

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.log10
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import org.apache.spark.internal.config
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.SamplingUtils

/**
 * 定义键值对RDD中元素如何按键进行分区的对象。
 * 将每个键映射到一个分区ID（从0到numPartitions-1）。
 *
 * 注意：分区器必须是确定性的，即对同一个键必须返回相同的分区ID。
 */
abstract class Partitioner extends Serializable {
  /** 返回分区总数 */
  def numPartitions: Int
  /** 根据键返回对应的分区ID */
  def getPartition(key: Any): Int
}

/** Partitioner伴生对象，提供为cogroup类操作选择默认分区器的工厂方法 */
object Partitioner {
  /**
   * 为多个RDD之间的cogroup类操作选择合适的分区器。
   *
   * 如果设置了spark.default.parallelism，使用SparkContext的defaultParallelism作为默认分区数；
   * 否则使用所有上游RDD中最大的分区数。
   *
   * 优先选择拥有最多分区的RDD的分区器。如果该分区器"合格"（分区数在最大分区数的一个数量级内），
   * 或者其分区数大于等于默认分区数，则使用该分区器。
   * 否则创建一个新的HashPartitioner。
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val rdds = (Seq(rdd) ++ others)
    // 过滤出已有分区器且分区数大于0的RDD
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))

    // 从有分区器的RDD中找出分区数最多的那个
    val hasMaxPartitioner: Option[RDD[_]] = if (hasPartitioner.nonEmpty) {
      Some(hasPartitioner.maxBy(_.partitions.length))
    } else {
      None
    }

    // 确定默认分区数：如果配置了spark.default.parallelism则使用配置值，否则取所有RDD最大分区数
    val defaultNumPartitions = if (rdd.context.conf.contains(config.DEFAULT_PARALLELISM.key)) {
      rdd.context.defaultParallelism
    } else {
      rdds.map(_.partitions.length).max
    }

    // 如果存在最大分区器，且该分区器合格或其分区数 >= 默认分区数，则使用它
    if (hasMaxPartitioner.nonEmpty && (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions <= hasMaxPartitioner.get.getNumPartitions)) {
      hasMaxPartitioner.get.partitioner.get
    } else {
      // 否则创建新的HashPartitioner
      new HashPartitioner(defaultNumPartitions)
    }
  }

  /**
   * 判断分区器是否"合格"：其分区数与所有RDD最大分区数之间的差距在一个数量级以内。
   * 即 log10(maxPartitions) - log10(该分区器分区数) < 1
   */
  private def isEligiblePartitioner(
     hasMaxPartitioner: RDD[_],
     rdds: Seq[RDD[_]]): Boolean = {
    val maxPartitions = rdds.map(_.partitions.length).max
    log10(maxPartitions) - log10(hasMaxPartitioner.getNumPartitions) < 1
  }
}

/**
 * 基于哈希的分区器，使用Java的Object.hashCode进行分区。
 *
 * 注意：Java数组的hashCode基于数组引用而非内容，
 * 因此对RDD[Array[_]]使用HashPartitioner会产生不正确的结果。
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  /**
   * 根据键的hashCode计算分区ID：null键固定分配到分区0，
   * 其他键通过非负取模运算映射到[0, numPartitions)范围
   */
  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  /** 两个HashPartitioner相等当且仅当它们的分区数相同 */
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
 * 透传分区器：直接将记录的键（Int类型的分区ID）作为分区结果返回。
 * 用于分区ID已经预先计算好的场景。
 */
private[spark] class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * 基于分区值映射表的分区器。valueMap包含(分区值, 分区ID)的映射，
 * 由KeyedPartitioning生成，用于确保Join两侧具有相同分区值的记录在同一分区中。
 */
private[spark] class KeyGroupedPartitioner(
    valueMap: mutable.Map[Seq[Any], Int],
    override val numPartitions: Int) extends Partitioner {
  /**
   * 将键转换为Seq[Any]并在映射表中查找对应分区ID，
   * 如果未找到则通过hashCode取模计算并缓存到映射表中
   */
  override def getPartition(key: Any): Int = {
    val keys = key.asInstanceOf[Seq[Any]]
    val normalizedKeys = ArraySeq.from(keys)
    valueMap.getOrElseUpdate(normalizedKeys,
      Utils.nonNegativeMod(normalizedKeys.hashCode, numPartitions))
  }
}

/**
 * 常量分区器：将所有记录分配到单个分区（分区0）。
 */
private[spark] class ConstantPartitioner extends Partitioner {
  override def numPartitions: Int = 1
  override def getPartition(key: Any): Int = 0
}

/**
 * 基于范围的分区器：通过对RDD内容进行采样来确定分区边界，将可排序的记录分成大致相等的范围。
 *
 * @note 实际创建的分区数可能与partitions参数不同（当采样记录数少于partitions时）。
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
  extends Partitioner {

  // SPARK-22160: 为保持Java的向后兼容性而声明的3参数构造函数
  def this(partitions: Int, rdd: RDD[_ <: Product2[K, V]], ascending: Boolean) = {
    this(partitions, rdd, ascending, samplePointsPerPartitionHint = 20)
  }

  // 允许partitions=0的情况（使用默认设置对空RDD排序时会发生）
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  private var ordering = implicitly[Ordering[K]]

  // 存储前(partitions-1)个分区的上界数组，用于二分查找确定分区归属
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      // 单分区或零分区时无需边界
      Array.empty
    } else {
      // 计算所需采样量：每分区采样点数 * 分区数，上限100万。转为double避免int溢出
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // 假设输入分区大致均衡，过采样3倍以提高准确性
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      // 通过水塘采样对每个分区进行采样
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // 计算采样比例
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          // 如果某个分区的数据量远超平均值（采样量不足以代表该分区），标记为不均衡
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // 权重 = 1/采样概率 = 分区实际数量/采样数量
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // 对不均衡的分区使用期望的采样概率重新采样
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        // 根据带权重的候选样本确定分区边界
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  /** 实际分区数 = 边界数 + 1 */
  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  /**
   * 根据键确定所属分区：
   * - 边界数 <= 128时使用线性搜索
   * - 边界数 > 128时使用二分搜索
   * 如果是降序排列则将分区号翻转
   */
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // 边界数较少时使用朴素线性搜索
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // 边界数较多时使用二分搜索
      partition = binarySearch(rangeBounds, k)
      // binarySearch返回匹配位置或 -(插入点)-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    // 升序时直接返回分区号，降序时翻转分区号
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  /** 两个RangePartitioner相等当且仅当它们的边界数组和排序方向都相同 */
  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  /** 基于所有边界值和排序方向计算hashCode */
  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  /**
   * 自定义序列化写入：如果使用JavaSerializer则走默认序列化；
   * 否则手动写入ascending、ordering、binarySearch，并使用Spark的序列化器序列化rangeBounds
   */
  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  /**
   * 自定义序列化读取：与writeObject对应，如果使用JavaSerializer则走默认反序列化；
   * 否则手动读取各字段并使用Spark的序列化器反序列化rangeBounds
   */
  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

/** RangePartitioner的伴生对象，提供采样和边界确定的工具方法 */
private[spark] object RangePartitioner {

  /**
   * 通过水塘采样对输入RDD的每个分区进行采样。
   *
   * @param rdd 输入RDD
   * @param sampleSizePerPartition 每个分区的最大采样数
   * @return (总元素数, (分区ID, 分区元素数, 采样数组)的数组)
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // 对每个分区执行水塘采样，使用分区索引和rdd.id生成随机种子确保可重复性
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    // 汇总所有分区的元素总数
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * 根据带权重的候选样本确定范围分区的边界。
   * 权重通常是采样概率的倒数（1/采样概率）。
   *
   * @param candidates 无序的带权重候选样本
   * @param partitions 目标分区数
   * @return 选定的分区边界数组
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    // 按键排序
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    // 计算所有权重之和
    val sumWeights = ordered.map(_._2.toDouble).sum
    // 每个分区应分配的权重步长
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    // 遍历排序后的候选样本，累加权重直到达到步长阈值时设置一个边界
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // 跳过重复值，避免出现空分区
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}

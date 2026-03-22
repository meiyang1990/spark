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

import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap}

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.SPECULATION_ENABLED
import org.apache.spark.internal.io._
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf, Utils}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 *
 * 为键值对(K-V)类型RDD提供扩展操作的工具类，通过Scala隐式转换为普通RDD[(K, V)]注入丰富的键值对操作。
 * 是Spark中所有键值对操作(如reduceByKey、join、groupByKey等)的核心实现类。
 *
 * @param self 被隐式转换的原始 RDD[(K, V)]
 * @param kt Key 类型的 ClassTag，用于运行时类型信息
 * @param vt Value 类型的 ClassTag
 * @param ord Key 的排序规则（可选），用于排序相关操作
 */
class PairRDDFunctions[K, V](self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable {

  /**
   * 按Key聚合元素的通用基础方法，所有ByKey聚合操作都基于此方法实现。
   * 将RDD[(K, V)]转换为结果类型RDD[(K, C)]，C可以是与V不同的类型。
   *
   * 支持用户自定义三个聚合阶段函数：
   * 1. createCombiner: 为每个分区中首次出现的Key创建初始累加器
   * 2. mergeValue: 在分区内将新Value合并到累加器
   * 3. mergeCombiners: 合并来自不同分区的两个累加器
   *
   * 支持Map端预聚合（类似MapReduce的Combiner），可以大幅减少Shuffle数据量。
   *
   * @param createCombiner 创建累加器函数，将第一个value转换为累加器类型C
   * @param mergeValue 分区内合并函数，将value合并到已有累加器
   * @param mergeCombiners 分区间合并函数，合并两个累加器为一个
   * @param partitioner 输出结果的分区器
   * @param mapSideCombine 是否启用Map端预聚合，默认开启
   * @param serializer 自定义序列化器，null表示使用默认序列化器
   * @param ct 结果类型C的ClassTag
   * @return 聚合完成的RDD[(K, C)]
   */
  def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined")
    // 数组类型Key不支持Map端预聚合，因为数组equals/hashCode语义不符合预期
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw SparkCoreErrors.cannotUseMapSideCombiningWithArrayKeyError()
      }
      // 数组Key也不能使用HashPartitioner，因为Java数组的hashCode基于对象地址而非内容
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
      }
    }
    // 封装聚合函数，清理闭包避免序列化问题
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    // 优化：如果当前RDD已经使用目标分区器，无需Shuffle，直接在分区内聚合即可
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        // 支持任务取消时中断迭代
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      // 需要Shuffle，创建ShuffledRDD执行重分区和聚合
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }

  /**
   * 兼容旧版本API的combineByKey，不提供结果类型ClassTag信息，保留用于向后兼容。
   *
   * @see `combineByKeyWithClassTag`
   */
  def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
      partitioner, mapSideCombine, serializer)(null)
  }

  /**
   * 简化版本combineByKey，使用Hash分区，分区数由用户指定，保留用于向后兼容。
   *
   * @see `combineByKeyWithClassTag`
   */
  def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, numPartitions)(null)
  }

  /**
   * 简化版本combineByKeyWithClassTag，使用Hash分区，分区数由用户指定。
   */
  def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
      new HashPartitioner(numPartitions))
  }

  /**
   * 基于初始值的按Key聚合，可以返回与输入Value不同类型的结果。
   * 需要提供两个函数：一个用于在分区内合并Value到累加器，一个用于合并不同分区的累加器。
   * 允许修改传入的累加器对象避免额外内存分配。
   *
   * @param zeroValue 初始零值，每个Key首次聚合时会克隆此对象作为初始累加器
   * @param partitioner 输出结果的分区器
   * @param seqOp 分区内聚合函数 (U, V) => U
   * @param combOp 分区间聚合函数 (U, U) => U
   * @tparam U 聚合结果类型
   * @return 聚合完成的RDD[(K, U)]
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    // 将zeroValue序列化到字节数组，保证每个Key都能获得独立的克隆副本
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // 每个Task延迟创建一个序列化器实例，反序列化zeroValue
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    val cleanedSeqOp = self.context.clean(seqOp)
    // 基于combineByKey实现，createCombiner阶段用初始值合并第一个Value
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)
  }

  /**
   * aggregateByKey简化版本，使用指定分区数的Hash分区。
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp)
  }

  /**
   * aggregateByKey简化版本，使用默认分区器。
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
  }

  /**
   * 基于零值的按Key折叠聚合，使用满足结合律的函数合并同Key的值。
   * 零值可以被任意多次添加到结果中，且不改变结果（如列表拼接用Nil，加法用0，乘法用1）。
   */
  def foldByKey(
      zeroValue: V,
      partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // 将zeroValue序列化到字节数组，每个Key克隆独立副本
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // 每个Task延迟创建一个序列化器实例
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

    val cleanedFunc = self.context.clean(func)
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      cleanedFunc, cleanedFunc, partitioner)
  }

  /**
   * foldByKey简化版本，使用指定分区数的Hash分区。
   */
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
  }

  /**
   * foldByKey简化版本，使用默认分区器。
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, defaultPartitioner(self))(func)
  }

  /**
   * 按Key进行分层抽样，根据不同Key指定不同采样率，一次遍历完成采样。
   * 结果大小约等于所有Key的numItems * samplingRate之和。
   *
   * @param withReplacement 是否有放回采样
   * @param fractions 各个Key对应的采样率Map
   * @param seed 随机数种子
   * @return 采样后的RDD[(K, V)]
   */
  def sampleByKey(withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, false, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, false, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true, isOrderSensitive = true)
  }

  /**
   * 精确分层按Key抽样，保证每个Key的样本数精确等于ceil(numItems * samplingRate)。
   * 相比sampleByKey需要额外的遍历来保证精度，无放回采样需要多一次遍历，有放回需要多两次遍历。
   *
   * @param withReplacement 是否有放回采样
   * @param fractions 各个Key对应的采样率Map
   * @param seed 随机数种子
   * @return 采样后的RDD[(K, V)]
   */
  def sampleByKeyExact(
      withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, true, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, true, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true, isOrderSensitive = true)
  }

  /**
   * 使用满足结合律和交换律的函数按Key聚合，在Map端会执行本地预聚合减少Shuffle数据量，类似MapReduce的Combiner。
   * 是Spark最常用的按Key聚合算子，性能远优于groupByKey。
   *
   * @param partitioner 输出结果的分区器
   * @param func 聚合函数 (V, V) => V，必须满足结合律和交换律
   * @return 聚合完成的RDD[(K, V)]
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // 底层基于combineByKey实现，三个函数都使用同一个聚合函数
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * reduceByKey简化版本，使用指定分区数的Hash分区。
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * reduceByKey简化版本，使用默认分区器。
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * 按Key聚合后直接将全部结果收集到Driver端返回为Map，是一个Action操作。
   * 同样在Map端执行本地预聚合，相比reduceByKey + collect更直接。
   *
   * @note 结果必须能够完整放入Driver内存，仅适用于聚合后Key数量较小的场景。
   *
   * @param func 聚合函数 (V, V) => V
   * @return 包含所有聚合结果的Map[K, V]
   */
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = self.withScope {
    val cleanedF = self.sparkContext.clean(func)

    // 数组Key不支持此操作，因为equals/hashCode问题
    if (keyClass.isArray) {
      throw SparkCoreErrors.reduceByKeyLocallyNotSupportArrayKeys
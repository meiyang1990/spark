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
 * PairRDDFunctions —— Spark 键值对（K-V）RDD 的核心操作类
 *
 * 【设计理念】
 * 这个类通过 Scala 的隐式转换机制，为所有 RDD[(K, V)] 类型的 RDD 提供丰富的键值对操作。
 * 当你在代码中对 RDD[(K, V)] 调用 reduceByKey、groupByKey、join 等方法时，
 * Scala 编译器会自动将其转换为 PairRDDFunctions 的实例来调用对应方法。
 *
 * 【核心功能分类】
 * 1. 聚合操作（Aggregation）：
 *    - combineByKey：最通用的聚合原语，所有 ByKey 操作的基础
 *    - reduceByKey：使用关联函数合并同 key 的 values（有 Map 端预聚合，性能好）
 *    - aggregateByKey：带初始值的聚合，可以返回不同类型
 *    - foldByKey：带零值的折叠操作
 *    - groupByKey：将同 key 的 values 收集成序列（无预聚合，慎用）
 *
 * 2. 连接操作（Join）：
 *    - join：内连接，返回两个 RDD 中 key 都存在的记录
 *    - leftOuterJoin：左外连接，保留左 RDD 所有记录
 *    - rightOuterJoin：右外连接，保留右 RDD 所有记录
 *    - fullOuterJoin：全外连接，保留两侧所有记录
 *    - cogroup：将多个 RDD 按 key 分组，是所有 join 操作的基础
 *
 * 3. 分区操作（Partitioning）：
 *    - partitionBy：使用指定的 Partitioner 重新分区
 *    - keys/values：提取所有 key 或 value
 *    - mapValues/flatMapValues：只对 value 进行转换，保持分区不变
 *
 * 4. 输出操作（Output）：
 *    - saveAsHadoopFile/saveAsNewAPIHadoopFile：保存到 HDFS
 *    - saveAsHadoopDataset/saveAsNewAPIHadoopDataset：保存到 Hadoop 数据集
 *
 * 【性能优化要点】
 * - reduceByKey vs groupByKey：优先使用 reduceByKey，因为它在 Map 端预聚合，
 *   减少 Shuffle 数据量。groupByKey 会将所有数据 Shuffle 到 Reduce 端。
 * - Partitioner 的重要性：如果两个 RDD 使用相同的 Partitioner，
 *   join 等操作可以避免 Shuffle（称为"窄依赖"优化）。
 * - mapValues 保持分区：使用 mapValues 而非 map 可以保持分区信息，
 *   避免后续操作产生不必要的 Shuffle。
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
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   *
   * Users provide three functions:
   *
   *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *  - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   *
   * @note V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]).
   *
   * combineByKeyWithClassTag —— Spark 所有 ByKey 聚合操作的核心基础方法
   *
   * 【执行原理】
   * 这是一个类似 MapReduce 中 Combiner + Reducer 的实现：
   *
   * 1. Map 端（如果启用 mapSideCombine）：
   *    - 对于每个分区内遇到的第一个 key，调用 createCombiner(v) 创建累加器 C
   *    - 对于后续相同 key 的 value，调用 mergeValue(c, v) 将 value 合并到累加器
   *    - 这一步相当于 MapReduce 的 Combiner，在本地预聚合减少 Shuffle 数据量
   *
   * 2. Shuffle 阶段：
   *    - 将 Map 端的 (K, C) 数据按照 Partitioner 重新分区
   *    - 相同 key 的数据被发送到同一个 Reduce 分区
   *
   * 3. Reduce 端：
   *    - 调用 mergeCombiners(c1, c2) 合并来自不同 Map 任务的累加器
   *    - 最终每个 key 产生一个聚合结果
   *
   * 【三个函数的作用】
   * - createCombiner: V => C     创建累加器，将第一个 value 转为累加器类型
   * - mergeValue: (C, V) => C    分区内合并，将新 value 合并到累加器
   * - mergeCombiners: (C, C) => C 分区间合并，合并两个累加器
   *
   * 【示例：实现 reduceByKey】
   * combineByKeyWithClassTag(
   *   (v: V) => v,           // createCombiner：第一个 value 直接作为累加器
   *   func,                   // mergeValue：用聚合函数合并
   *   func                    // mergeCombiners：用相同函数合并累加器
   * )
   *
   * 【示例：实现 groupByKey】
   * combineByKeyWithClassTag(
   *   (v: V) => CompactBuffer(v),    // createCombiner：创建列表
   *   (buf, v) => buf += v,          // mergeValue：追加到列表
   *   (buf1, buf2) => buf1 ++= buf2, // mergeCombiners：合并列表
   *   mapSideCombine = false         // 关闭 Map 端预聚合（因为不减少数据量）
   * )
   *
   * @param createCombiner 创建累加器函数
   * @param mergeValue 分区内合并函数
   * @param mergeCombiners 分区间合并函数
   * @param partitioner 输出 RDD 的分区器
   * @param mapSideCombine 是否启用 Map 端预聚合，默认 true
   * @param serializer 序列化器，null 表示使用默认
   */
  def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    // 数组类型的 key 不支持 Map 端预聚合（因为数组的 equals/hashCode 语义不符合预期）
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw SparkCoreErrors.cannotUseMapSideCombiningWithArrayKeyError()
      }
      // HashPartitioner 也不能用于数组 key，因为 Java 数组的 hashCode 不是基于内容的
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
      }
    }
    // 创建 Aggregator，封装三个聚合函数（使用 clean 方法清理闭包，避免序列化问题）
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    // 优化：如果当前 RDD 的 Partitioner 与目标相同，无需 Shuffle
    // 直接在每个分区内做聚合即可（窄依赖）
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        // InterruptibleIterator 支持任务取消时中断迭代
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      // 需要 Shuffle：创建 ShuffledRDD 执行重分区和聚合
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. This method is here for backward compatibility. It does not provide combiner
   * classtag information to the shuffle.
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
   * Simplified version of combineByKeyWithClassTag that hash-partitions the output RDD.
   * This method is here for backward compatibility. It does not provide combiner
   * classtag information to the shuffle.
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
   * Simplified version of combineByKeyWithClassTag that hash-partitions the output RDD.
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
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.IterableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   *
   * aggregateByKey —— 带初始值的按 key 聚合操作
   *
   * 【与 reduceByKey 的区别】
   * - reduceByKey：聚合结果类型必须与 value 类型相同
   * - aggregateByKey：聚合结果类型 U 可以与 value 类型 V 不同
   *
   * 【使用场景示例】
   * 计算每个 key 的 (sum, count)：
   * rdd.aggregateByKey((0, 0))(
   *   (acc, v) => (acc._1 + v, acc._2 + 1),  // seqOp: 分区内累加
   *   (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // combOp: 分区间合并
   * )
   *
   * 【zeroValue 的重要性】
   * - zeroValue 会被序列化后在每个 key 首次出现时反序列化创建新实例
   * - 这样保证每个 key 都有独立的初始累加器，避免共享状态问题
   *
   * @param zeroValue 初始值（零值），会被克隆用于每个 key
   * @param partitioner 输出 RDD 的分区器
   * @param seqOp 分区内聚合函数 (U, V) => U
   * @param combOp 分区间聚合函数 (U, U) => U
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    // 将 zeroValue 序列化为字节数组，这样每个 key 都能获得独立的克隆副本
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // 延迟创建序列化器实例（每个 task 一个），用于反序列化 zeroValue
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    // We will clean the combiner closure later in `combineByKey`
    val cleanedSeqOp = self.context.clean(seqOp)
    // 转换为 combineByKey 调用：createCombiner 用 seqOp(zeroValue, v) 实现
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.IterableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.IterableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(
      zeroValue: V,
      partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // When deserializing, use a lazy val to create just one instance of the serializer per task
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

    val cleanedFunc = self.context.clean(func)
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      cleanedFunc, cleanedFunc, partitioner)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, defaultPartitioner(self))(func)
  }

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling).
   *
   * Create a sample of this RDD using variable sampling rates for different keys as specified by
   * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
   * RDD, to produce a sample of size that's approximately equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @return RDD containing the sampled subset
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
   * Return a subset of this RDD sampled by key (via stratified sampling) containing exactly
   * math.ceil(numItems * samplingRate) for each stratum (group of pairs with the same key).
   *
   * This method differs from [[sampleByKey]] in that we make additional passes over the RDD to
   * create a sample size that's exactly equal to the sum of math.ceil(numItems * samplingRate)
   * over all key values with a 99.99% confidence. When sampling without replacement, we need one
   * additional pass over the RDD to guarantee sample size; when sampling with replacement, we need
   * two additional passes.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @return RDD containing the sampled subset
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
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce.
   *
   * reduceByKey —— Spark 中最常用的按 key 聚合操作
   *
   * 【核心特点】
   * 1. Map 端预聚合：在发送到 Reduce 端之前，先在每个 Map 任务本地做聚合
   *    这大大减少了 Shuffle 的数据量，是性能优于 groupByKey 的关键原因
   *
   * 2. 要求函数满足：
   *    - 结合律（Associative）：(a op b) op c = a op (b op c)
   *    - 交换律（Commutative）：a op b = b op a
   *    常见满足条件的函数：+ (加法)、* (乘法)、max、min、字符串连接（需注意顺序）
   *
   * 【使用示例】
   * val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
   *
   * 【vs groupByKey】
   * 假设有 100 万条数据，1000 个不同的 key：
   * - reduceByKey：Map 端预聚合后，Shuffle 约 1000 条数据
   * - groupByKey：Shuffle 全部 100 万条数据，然后在 Reduce 端聚合
   *
   * @param partitioner 输出 RDD 的分区器
   * @param func 聚合函数 (V, V) => V，必须满足结合律和交换律
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // 底层实现：combineByKey 的三个函数都用同一个 func
    // createCombiner: v => v（第一个 value 直接作为累加器）
    // mergeValue: func（合并 value 到累加器）
    // mergeCombiners: func（合并两个累加器）
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function, but return
   * the results immediately to the master as a Map. This will also perform the merging locally on
   * each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce.
   *
   * reduceByKeyLocally —— 将聚合结果直接收集到 Driver 端的 Map
   *
   * 【与 reduceByKey.collect 的区别】
   * reduceByKeyLocally 是一个 Action 操作，直接返回 Map[K, V]，
   * 而 reduceByKey 返回 RDD[(K, V)]，需要再调用 collect 才能获得结果。
   *
   * 【注意事项】
   * 结果需要能够完整放入 Driver 内存，适用于聚合后数据量较小的场景。
   *
   * @param func 聚合函数 (V, V) => V
   * @return 包含所有 (key, aggregatedValue) 的 Map
   */
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = self.withScope {
    val cleanedF = self.sparkContext.clean(func)

    // 数组类型的 key 不支持此操作（因为数组的 equals/hashCode 问题）
    if (keyClass.isArray) {
      throw SparkCoreErrors.reduceByKeyLocallyNotSupportArrayKeysError()
    }

    // 分区内聚合：使用 HashMap 按 key 累加
    val reducePartition = (iter: Iterator[(K, V)]) => {
      val map = new JHashMap[K, V]
      iter.foreach { pair =>
        val old = map.get(pair._1)
        // 如果是该 key 的第一个值，直接放入；否则用聚合函数合并
        map.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
      }
      Iterator(map)
    } : Iterator[JHashMap[K, V]]

    // 分区间合并：将多个 HashMap 合并为一个
    val mergeMaps = (m1: JHashMap[K, V], m2: JHashMap[K, V]) => {
      m2.asScala.foreach { pair =>
        val old = m1.get(pair._1)
        m1.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
      }
      m1
    } : JHashMap[K, V]

    // mapPartitions + reduce 实现两阶段聚合
    self.mapPartitions(reducePartition).reduce(mergeMaps).asScala
  }

  /**
   * Count the number of elements for each key, collecting the results to a local Map.
   *
   * @note This method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
   * To handle very large results, consider using rdd.mapValues(_ => 1L).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
   */
  def countByKey(): Map[K, Long] = self.withScope {
    self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
  }

  /**
   * Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   *
   * The confidence is the probability that the error bounds of the result will
   * contain the true value. That is, if countApprox were called repeatedly
   * with confidence 0.9, we would expect 90% of the results to contain the
   * true count. The confidence must be in the range [0,1] or an exception will
   * be thrown.
   *
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param confidence the desired statistical confidence in the result
   * @return a potentially incomplete result, with error bounds
   */
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
      : PartialResult[Map[K, BoundedDouble]] = self.withScope {
    self.map(_._1).countByValueApprox(timeout, confidence)
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero (`sp` is
   * greater than `p`) would trigger sparse representation of registers, which may reduce the
   * memory consumption and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   * @param partitioner Partitioner to use for the resulting RDD.
   */
  def countApproxDistinctByKey(
      p: Int,
      sp: Int,
      partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val createHLL = (v: V) => {
      val hll = new HyperLogLogPlus(p, sp)
      hll.offer(v)
      hll
    }
    val mergeValueHLL = (hll: HyperLogLogPlus, v: V) => {
      hll.offer(v)
      hll
    }
    val mergeHLL = (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
      h1.addAll(h2)
      h1
    }

    combineByKeyWithClassTag(createHLL, mergeValueHLL, mergeHLL, partitioner)
      .mapValues(_.cardinality())
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param partitioner partitioner of the resulting RDD
   */
  def countApproxDistinctByKey(
      relativeSD: Double,
      partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    assert(p <= 32)
    countApproxDistinctByKey(if (p < 4) 4 else p, 0, partitioner)
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param numPartitions number of partitions of the resulting RDD
   */
  def countApproxDistinctByKey(
      relativeSD: Double,
      numPartitions: Int): RDD[(K, Long)] = self.withScope {
    countApproxDistinctByKey(relativeSD, new HashPartitioner(numPartitions))
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinctByKey(relativeSD: Double = 0.05): RDD[(K, Long)] = self.withScope {
    countApproxDistinctByKey(relativeSD, defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   * The ordering of elements within each group is not guaranteed, and may even differ
   * each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   *
   * @note As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
   *
   * groupByKey —— 按 key 分组，将同一 key 的所有 value 收集到一个序列
   *
   * 【重要警告：性能问题】
   * groupByKey 是 Spark 中最容易被误用的算子之一！
   *
   * 【为什么 groupByKey 性能差？】
   * 1. 无 Map 端预聚合：所有数据都要 Shuffle 到 Reduce 端
   *    - 如果有 100 万条记录、1000 个 key，全部 100 万条都要 Shuffle
   *    - 而 reduceByKey 只需 Shuffle 1000 条（Map 端已聚合）
   *
   * 2. 内存压力大：某个 key 的所有 value 必须同时放入内存
   *    - 如果某 key 有 100 万个 value，可能导致 OOM
   *
   * 【什么时候必须用 groupByKey？】
   * - 需要保留所有原始 value 时（如去重前的数据）
   * - 聚合逻辑无法用 reduce 表达时（如取中位数）
   *
   * 【更好的替代方案】
   * - 求和/计数：reduceByKey(_ + _)
   * - 求最大/最小：reduceByKey(math.max)
   * - 复杂聚合：aggregateByKey
   * - 分组后 map：使用 reduceByKey 或 combineByKey 直接聚合
   *
   * @param partitioner 输出 RDD 的分区器
   * @return RDD[(K, Iterable[V])]，每个 key 对应其所有 value 的可迭代集合
   */
  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    // 注意：mapSideCombine = false，因为 groupByKey 的 merge 操作（追加到列表）
    // 不减少数据量，反而增加 Map 端内存压力，所以禁用 Map 端预聚合
    val createCombiner = (v: V) => CompactBuffer(v)  // 创建单元素列表
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v  // 追加到列表
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2  // 合并列表
    val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions. The ordering of elements within
   * each group is not guaranteed, and may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   *
   * @note As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
   */
  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(new HashPartitioner(numPartitions))
  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   *
   * partitionBy —— 使用指定的 Partitioner 重新分区
   *
   * 【使用场景】
   * 1. 在 join 之前对两个 RDD 使用相同的 Partitioner，可以避免 join 时的 Shuffle
   * 2. 将数据按特定规则分布，如按日期分区、按地区分区等
   *
   * 【优化说明】
   * 如果当前 RDD 已经使用了相同的 Partitioner，直接返回 self，不产生新 RDD
   *
   * @param partitioner 目标分区器
   * @return 重新分区后的 RDD
   */
  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    // 数组 key 不能使用 HashPartitioner（hashCode 语义问题）
    if (keyClass.isArray && partitioner.isInstanceOf[HashPartitioner]) {
      throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
    }
    // 优化：如果 Partitioner 相同，无需重新分区
    if (self.partitioner == Some(partitioner)) {
      self
    } else {
      // 创建 ShuffledRDD 执行重分区
      new ShuffledRDD[K, V, V](self, partitioner)
    }
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   *
   * join —— 内连接操作，返回两个 RDD 中 key 都存在的配对
   *
   * 【执行原理】
   * 底层使用 cogroup 实现：先将两个 RDD 按 key 分组，然后做笛卡尔积
   *
   * 【结果说明】
   * 如果 RDD1 中 key=k 有 m 个 value，RDD2 中 key=k 有 n 个 value，
   * 则结果中会有 m × n 个 (k, (v1, v2)) 配对
   *
   * 【性能优化】
   * 如果两个 RDD 使用相同的 Partitioner，join 可以避免 Shuffle（窄依赖）
   *
   * @param other 要连接的另一个 RDD
   * @param partitioner 输出 RDD 的分区器
   * @return RDD[(K, (V, W))]，每个 key 对应两侧 value 的配对
   */
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
    // 使用 cogroup 将两个 RDD 按 key 分组，然后对每个 key 的两组 value 做笛卡尔积
    this.cogroup(other, partitioner).flatMapValues( pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)
    )
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   *
   * leftOuterJoin —— 左外连接，保留左 RDD 的所有记录
   *
   * 【语义说明】
   * - 对于左 RDD 中的每个 (k, v)：
   *   - 如果右 RDD 中存在 key=k 的记录，返回 (k, (v, Some(w)))
   *   - 如果右 RDD 中不存在 key=k，返回 (k, (v, None))
   *
   * @param other 右侧 RDD
   * @param partitioner 输出 RDD 的分区器
   * @return RDD[(K, (V, Option[W]))]
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner): RDD[(K, (V, Option[W]))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      // 如果右侧为空，左侧 value 配对 None
      if (pair._2.isEmpty) {
        pair._1.iterator.map(v => (v, None))
      } else {
        // 左侧与右侧所有 value 做笛卡尔积，右侧包装为 Some
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, Some(w))
      }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   *
   * rightOuterJoin —— 右外连接，保留右 RDD 的所有记录
   *
   * 【语义说明】
   * - 对于右 RDD 中的每个 (k, w)：
   *   - 如果左 RDD 中存在 key=k 的记录，返回 (k, (Some(v), w))
   *   - 如果左 RDD 中不存在 key=k，返回 (k, (None, w))
   *
   * @param other 右侧 RDD
   * @param partitioner 输出 RDD 的分区器
   * @return RDD[(K, (Option[V], W))]
   */
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      // 如果左侧为空，右侧 value 配对 None
      if (pair._1.isEmpty) {
        pair._2.iterator.map(w => (None, w))
      } else {
        // 左侧（包装为 Some）与右侧所有 value 做笛卡尔积
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (Some(v), w)
      }
    }
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   *
   * fullOuterJoin —— 全外连接，保留两侧的所有记录
   *
   * 【语义说明】
   * - 左右都有 key=k：返回 (k, (Some(v), Some(w))) 的笛卡尔积
   * - 只有左侧有 key=k：返回 (k, (Some(v), None))
   * - 只有右侧有 key=k：返回 (k, (None, Some(w)))
   *
   * @param other 右侧 RDD
   * @param partitioner 输出 RDD 的分区器
   * @return RDD[(K, (Option[V], Option[W]))]
   */
  def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], Option[W]))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, Seq()) => vs.iterator.map(v => (Some(v), None))  // 只有左侧
      case (Seq(), ws) => ws.iterator.map(w => (None, Some(w)))  // 只有右侧
      case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w))  // 两侧都有
    }
  }

  /**
   * Simplified version of combineByKeyWithClassTag that hash-partitions the resulting RDD using the
   * existing partitioner/parallelism level. This method is here for backward compatibility. It
   * does not provide combiner classtag information to the shuffle.
   *
   * @see `combineByKeyWithClassTag`
   */
  def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
  }

  /**
   * Simplified version of combineByKeyWithClassTag that hash-partitions the resulting RDD using the
   * existing partitioner/parallelism level.
   */
  def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
   * within each group is not guaranteed, and may even differ each time the resulting RDD is
   * evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(defaultPartitioner(self))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = self.withScope {
    join(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = self.withScope {
    leftOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (V, Option[W]))] = self.withScope {
    leftOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD using the existing partitioner/
   * parallelism level.
   */
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = self.withScope {
    fullOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD into the given number of partitions.
   */
  def fullOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], Option[W]))] = self.withScope {
    fullOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   *
   * Warning: this doesn't return a multimap (so if you have multiple values to the same key, only
   *          one value per key is preserved in the map returned)
   *
   * @note this method should only be used if the resulting data is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  /**
   * collectAsMap —— 将 K-V RDD 收集为 Driver 端的 Map
   *
   * 【注意事项】
   * 1. 如果存在重复 key，只保留最后一个 value
   * 2. 结果必须能放入 Driver 内存
   *
   * @return Map[K, V]，包含 RDD 中所有的键值对
   */
  def collectAsMap(): Map[K, V] = self.withScope {
    val data = self.collect()
    val map = new mutable.HashMap[K, V]
    map.sizeHint(data.length)  // 预设 Map 容量，减少扩容开销
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   *
   * mapValues —— 只对 value 进行转换，保持 key 和分区不变
   *
   * 【重要特性】
   * preservesPartitioning = true：保持原有分区信息不变
   * 这意味着后续的 reduceByKey、join 等操作可能避免 Shuffle
   *
   * 【vs map】
   * map { case (k, v) => (k, f(v)) } 会丢失分区信息
   * mapValues(f) 保持分区信息，性能更好
   *
   * @param f 对 value 的转换函数
   * @return 转换后的 RDD[(K, U)]
   */
  def mapValues[U](f: V => U): RDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) },
      preservesPartitioning = true)  // 关键：保持分区不变
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   *
   * flatMapValues —— 对 value 进行 flatMap，一个 (k, v) 可以产生多个 (k, u)
   *
   * 【使用场景】
   * rdd.flatMapValues(line => line.split(" "))
   * 将 (docId, "hello world") 转换为 [(docId, "hello"), (docId, "world")]
   *
   * @param f 对 value 的 flatMap 函数
   * @return 展开后的 RDD[(K, U)]
   */
  def flatMapValues[U](f: V => IterableOnce[U]): RDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.flatMap { case (k, v) =>
        cleanF(v).iterator.map(x => (k, x))
      },
      preservesPartitioning = true)  // 保持分区不变
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   *
   * cogroup（4个RDD版本） —— 将4个 RDD 按 key 分组
   *
   * 【cogroup 核心概念】
   * cogroup 是所有 join 操作的基础原语。它将多个 RDD 按 key 对齐，
   * 每个 key 对应各个 RDD 中该 key 的所有 value 集合。
   *
   * 【示例】
   * rdd1: [(1, "a"), (2, "b")]
   * rdd2: [(1, "x"), (1, "y")]
   * cogroup结果: [(1, (["a"], ["x", "y"])), (2, (["b"], []))]
   *
   * @param other1 第二个 RDD
   * @param other2 第三个 RDD
   * @param other3 第四个 RDD
   * @param partitioner 输出 RDD 的分区器
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
    }
    // 使用 CoGroupedRDD 实现多 RDD 的 cogroup
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s, w3s) =>
       (vs.asInstanceOf[Iterable[V]],
         w1s.asInstanceOf[Iterable[W1]],
         w2s.asInstanceOf[Iterable[W2]],
         w3s.asInstanceOf[Iterable[W3]])
    }
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   *
   * cogroup（2个RDD版本） —— join、leftOuterJoin 等操作的底层实现
   *
   * @param other 另一个 RDD
   * @param partitioner 输出 RDD 的分区器
   * @return RDD[(K, (Iterable[V], Iterable[W]))]
   */
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
    }
    val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw SparkCoreErrors.hashPartitionerCannotPartitionArrayKeyError()
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, new HashPartitioner(numPartitions))
  }

  /** Alias for cogroup. */
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be less than or equal to us.
   *
   * subtractByKey —— 按 key 做差集，返回 this 中 key 不在 other 中的记录
   *
   * 【使用场景】
   * - 数据清洗：去除黑名单中的 key
   * - 增量处理：找出新增的 key
   *
   * @param other 包含要排除的 key 的 RDD
   * @return 差集结果 RDD
   */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)] = self.withScope {
    subtractByKey(other, self.partitioner.getOrElse(new HashPartitioner(self.partitions.length)))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   */
  def subtractByKey[W: ClassTag](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, V)] = self.withScope {
    subtractByKey(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)] = self.withScope {
    // 使用专门的 SubtractedRDD 实现
    new SubtractedRDD[K, V, W](self, other, p)
  }

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   *
   * lookup —— 查找指定 key 的所有 value
   *
   * 【性能优化】
   * - 如果 RDD 有 Partitioner，只需扫描该 key 所在的分区
   * - 如果没有 Partitioner，需要扫描所有分区
   *
   * 【使用场景】
   * 快速查找少量 key 的值，避免全量 collect
   *
   * @param key 要查找的 key
   * @return 该 key 对应的所有 value 的序列
   */
  def lookup(key: K): Seq[V] = self.withScope {
    self.partitioner match {
      // 有 Partitioner：只扫描目标分区，效率高
      case Some(p) =>
        val index = p.getPartition(key)  // 计算 key 所在的分区索引
        val process = (it: Iterator[(K, V)]) => {
          val buf = new ArrayBuffer[V]
          for (pair <- it if pair._1 == key) {
            buf += pair._2
          }
          buf.toSeq
        } : Seq[V]
        val res = self.context.runJob(self, process, Array(index).toImmutableArraySeq)
        res(0)
      case None =>
        self.filter(_._1 == key).map(_._2).collect().toImmutableArraySeq
    }
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   *
   * saveAsHadoopFile —— 将 K-V RDD 保存到 Hadoop 兼容的文件系统
   *
   * 【Hadoop 输出概述】
   * Spark 支持将数据输出到任何 Hadoop 兼容的存储系统（HDFS、S3、HBase 等），
   * 通过指定不同的 OutputFormat 来控制输出格式：
   * - TextOutputFormat：文本格式
   * - SequenceFileOutputFormat：Hadoop 序列文件
   * - AvroOutputFormat：Avro 格式
   * 等等
   *
   * @param path 输出路径
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress the result with the
   * supplied codec.
   *
   * saveAsHadoopFile（带压缩） —— 输出时使用指定的压缩编码
   *
   * @param path 输出路径
   * @param codec 压缩编码类（如 GzipCodec、SnappyCodec、LZ4Codec）
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String,
      codec: Class[_ <: CompressionCodec])(implicit fm: ClassTag[F]): Unit = self.withScope {
    val runtimeClass = fm.runtimeClass
    saveAsHadoopFile(path, keyClass, valueClass, runtimeClass.asInstanceOf[Class[F]], codec)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   *
   * saveAsNewAPIHadoopFile —— 使用新版 Hadoop API (mapreduce) 保存数据
   *
   * 【新旧 API 区别】
   * - 旧 API (mapred)：org.apache.hadoop.mapred.OutputFormat
   * - 新 API (mapreduce)：org.apache.hadoop.mapreduce.OutputFormat
   * 新 API 更加灵活，推荐在新项目中使用
   *
   * @param path 输出路径
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsNewAPIHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = self.context.hadoopConfiguration): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = NewAPIHadoopJob.getInstance(hadoopConf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    job.setOutputFormatClass(outputFormatClass)
    val jobConfiguration = job.getConfiguration
    jobConfiguration.set("mapreduce.output.fileoutputformat.outputdir", path)
    saveAsNewAPIHadoopDataset(jobConfiguration)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress with the supplied codec.
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      codec: Class[_ <: CompressionCodec]): Unit = self.withScope {
    saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass,
      new JobConf(self.context.hadoopConfiguration), Option(codec))
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   *
   * @note We should make sure our tasks are idempotent when speculation is enabled, i.e. do
   * not use output committer that writes data directly.
   * There is an example in https://issues.apache.org/jira/browse/SPARK-10063 to show the bad
   * result of using direct output committer with speculation enabled.
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(self.context.hadoopConfiguration),
      codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    conf.setOutputFormat(outputFormatClass)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", c.getCanonicalName)
      hadoopConf.set("mapreduce.output.fileoutputformat.compress.type",
        CompressionType.BLOCK.toString)
    }

    // Use configured output committer if already set
    if (conf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    // When speculation is on and output committer class name contains "Direct", we should warn
    // users that they may loss data if they are using a direct output committer.
    val speculationEnabled = self.conf.get(SPECULATION_ENABLED)
    val outputCommitterClass = hadoopConf.get("mapred.output.committer.class", "")
    if (speculationEnabled && outputCommitterClass.contains("Direct")) {
      val warningMessage =
        log"${MDC(CLASS_NAME, outputCommitterClass)} " +
          log"may be an output committer that writes data directly to " +
          log"the final location. Because speculation is enabled, this output committer may " +
          log"cause data loss (see the case in SPARK-10063). If possible, please use an output " +
          log"committer that does not have this behavior (e.g. FileOutputCommitter)."
      logWarning(warningMessage)
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriterUtils.createPathFromString(path, hadoopConf))
    saveAsHadoopDataset(hadoopConf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop
   * Configuration object for that storage system. The Conf should set an OutputFormat and any
   * output paths required (e.g. a table name to write to) in the same way as it would be
   * configured for a Hadoop MapReduce job.
   *
   * @note We should make sure our tasks are idempotent when speculation is enabled, i.e. do
   * not use output committer that writes data directly.
   * There is an example in https://issues.apache.org/jira/browse/SPARK-10063 to show the bad
   * result of using direct output committer with speculation enabled.
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration): Unit = self.withScope {
    val config = new HadoopMapReduceWriteConfigUtil[K, V](new SerializableConfiguration(conf))
    SparkHadoopWriter.write(
      rdd = self,
      config = config)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   */
  def saveAsHadoopDataset(conf: JobConf): Unit = self.withScope {
    val config = new HadoopMapRedWriteConfigUtil[K, V](new SerializableJobConf(conf))
    SparkHadoopWriter.write(
      rdd = self,
      config = config)
  }

  /**
   * Return an RDD with the keys of each tuple.
   */
  def keys: RDD[K] = self.map(_._1)

  /**
   * Return an RDD with the values of each tuple.
   */
  def values: RDD[V] = self.map(_._2)

  private[spark] def keyClass: Class[_] = kt.runtimeClass

  private[spark] def valueClass: Class[_] = vt.runtimeClass

  private[spark] def keyOrdering: Option[Ordering[K]] = Option(ord)
}

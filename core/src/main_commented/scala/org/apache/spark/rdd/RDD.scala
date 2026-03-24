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

import java.util.Random

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.io.Codec
import scala.language.implicitConversions
import scala.ref.WeakReference
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.RDD_LIMIT_SCALE_UP_FACTOR
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{ExternalAppendOnlyMap, OpenHashMap,
  Utils => collectionUtils}
import org.apache.spark.util.random.{BernoulliCellSampler, BernoulliSampler, PoissonSampler,
  SamplingUtils, XORShiftRandom}

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; and
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)])
 * through implicit.
 *
 * Internally, each RDD is characterized by five main properties:
 *
 *  - A list of partitions
 *  - A function for computing each split
 *  - A list of dependencies on other RDDs
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * <a href="http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf">Spark paper</a>
 * for more details on RDD internals.
 *
 * 弹性分布式数据集（RDD）是 Spark 的核心抽象。它代表一个不可变的、分区的元素集合，可以并行操作。
 *
 * RDD 的五大核心属性：
 *   1. partitions（分区列表）：数据被切分成多个分区，每个分区在集群中的不同节点上处理
 *   2. compute（计算函数）：定义如何计算每个分区的数据
 *   3. dependencies（依赖列表）：记录该 RDD 依赖哪些父 RDD，用于容错和血缘追溯
 *   4. partitioner（分区器，可选）：对于 K-V 类型的 RDD，定义数据如何按 key 分区
 *   5. preferredLocations（首选位置，可选）：数据本地化，告诉调度器每个分区最好在哪个节点计算
 *
 * RDD 的设计理念：
 *   - 不可变性：RDD 一旦创建就不能修改，只能通过转换生成新的 RDD
 *   - 惰性求值：转换操作不会立即执行，只有遇到行动操作才会触发真正的计算
 *   - 容错性：通过血缘（lineage）记录数据的来源，分区丢失时可以重新计算
 *
 * @tparam T RDD 中元素的类型
 */
abstract class RDD[T: ClassTag](
    // SparkContext 引用，标记为 @transient 表示序列化时忽略（RDD 会被序列化发送到 Executor）
    @transient private var _sc: SparkContext,
    // 依赖列表，记录该 RDD 与父 RDD 的依赖关系
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  // 检查是否创建了嵌套 RDD（RDD 中包含 RDD），这在 Spark 中是不支持的
  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {
    // This is a warning instead of an exception in order to avoid breaking user programs that
    // might have defined nested RDDs without running jobs with them.
    logWarning("Spark does not support nested RDDs (see SPARK-5063)")
  }

  // 获取 SparkContext，如果为空则抛出异常（RDD 必须与 SparkContext 关联）
  private def sc: SparkContext = {
    if (_sc == null) {
      throw SparkCoreErrors.rddLacksSparkContextError()
    }
    _sc
  }

  /**
   * 辅助构造函数：创建只有一个父 RDD 的子 RDD（一对一依赖）
   * 这是最常见的 RDD 依赖关系，比如 map、filter 等操作都会产生 OneToOneDependency
   */
  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf

  // =======================================================================
  // 子类必须实现的方法 - 这是 RDD 的核心抽象
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   *
   * 计算给定分区的数据。这是 RDD 最核心的方法，定义了如何产生分区数据。
   * 不同的 RDD 子类通过实现此方法来定义自己的数据来源和计算逻辑：
   *   - HadoopRDD：从 HDFS 读取数据
   *   - MapPartitionsRDD：对父 RDD 的每个分区应用转换函数
   *   - ShuffledRDD：从 Shuffle 输出读取数据
   *
   * @param split 要计算的分区
   * @param context 任务上下文，提供任务执行的各种信息和控制
   * @return 分区数据的迭代器
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   *
   * 返回 RDD 的分区数组。此方法只会被调用一次，结果会被缓存。
   * 不同 RDD 的分区数取决于数据源或用户配置：
   *   - HadoopRDD：分区数由 HDFS 文件的 Block 数决定
   *   - ParallelCollectionRDD：分区数由用户指定或默认的并行度决定
   */
  protected def getPartitions: Array[Partition]

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * 返回该 RDD 对父 RDD 的依赖关系。依赖分为两种：
   *   - NarrowDependency（窄依赖）：父 RDD 的每个分区最多被子 RDD 的一个分区使用
   *     例如：map、filter、union
   *   - ShuffleDependency（宽依赖/Shuffle 依赖）：父 RDD 的分区可能被多个子 RDD 分区使用
   *     例如：groupByKey、reduceByKey、join
   *
   * 窄依赖可以流水线执行，宽依赖需要先完成上游的所有任务
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   *
   * 返回分区的首选计算位置（数据本地化）。
   * 调度器会优先将任务调度到数据所在的节点，减少网络传输：
   *   - PROCESS_LOCAL：数据在同一个 JVM 进程中
   *   - NODE_LOCAL：数据在同一个节点的不同进程中
   *   - RACK_LOCAL：数据在同一个机架的不同节点上
   *   - ANY：数据在任意位置
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /**
   * 可选的分区器，只对 K-V 类型的 RDD 有意义。
   * 分区器决定了数据如何按 key 分布到不同分区：
   *   - HashPartitioner：按 key 的 hashCode 分区
   *   - RangePartitioner：按 key 的范围分区（用于排序）
   *
   * 有了分区器，相同 key 的数据一定在同一个分区，这对 join、cogroup 等操作很重要
   */
  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // 所有 RDD 都可用的方法和字段
  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================

  /** The SparkContext that created this RDD. */
  def sparkContext: SparkContext = sc

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD */
  @transient var name: String = _

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * Mark this RDD for persisting using the specified level.
   *
   * 将 RDD 标记为持久化，使用指定的存储级别。
   * 持久化是 Spark 的重要优化手段，可以避免重复计算：
   *   - 第一次计算后，数据会按指定级别存储
   *   - 后续使用该 RDD 时直接读取缓存，无需重新计算
   *
   * @param newLevel the target storage level
   * @param allowOverride whether to override any existing level with the new one
   */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // TODO: Handle changes of StorageLevel
    // 不允许修改已设置的存储级别（除非明确允许覆盖）
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
      throw SparkCoreErrors.cannotChangeStorageLevelError()
    }
    // If this is the first time this RDD is marked for persisting, register it
    // with the SparkContext for cleanups and accounting. Do this only once.
    // 首次持久化时，注册到 SparkContext 以便清理和管理
    if (storageLevel == StorageLevel.NONE) {
      sc.cleaner.foreach(_.registerRDDForCleanup(this))
      sc.persistRDD(this)
    }
    storageLevel = newLevel
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet. Local checkpointing is an exception.
   *
   * 设置 RDD 的存储级别，使数据在第一次计算后被持久化。
   * 常用的存储级别：
   *   - MEMORY_ONLY：只存内存（默认）
   *   - MEMORY_AND_DISK：内存放不下时溢写到磁盘
   *   - MEMORY_ONLY_SER：序列化后存内存，更省空间但需要反序列化
   *   - DISK_ONLY：只存磁盘
   */
  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      // 如果已经设置了本地检查点，需要适配存储级别以使用磁盘
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   * 使用默认存储级别（MEMORY_ONLY）持久化 RDD
   */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   * cache() 是 persist() 的别名，更符合语义习惯
   */
  def cache(): this.type = persist()

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * 取消 RDD 的持久化，释放内存和磁盘空间。
   * 当 RDD 不再需要时应该调用此方法，避免内存泄漏。
   *
   * @param blocking Whether to block until all blocks are deleted (default: false)
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = false): this.type = {
    if (isLocallyCheckpointed) {
      // This means its lineage has been truncated and cannot be recomputed once unpersisted.
      // 警告：本地检查点的 RDD 取消持久化后无法重新计算
      logWarning(log"RDD ${MDC(RDD_ID, id)} was locally checkpointed, its lineage has been" +
        log" truncated and cannot be recomputed after unpersisting")
    }
    logInfo(log"Removing RDD ${MDC(RDD_ID, id)} from persistence list")
    sc.unpersistRDD(id, blocking)
    storageLevel = StorageLevel.NONE
    this
  }

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel: StorageLevel = storageLevel

  /**
   * Lock for all mutable state of this RDD (persistence, partitions, dependencies, etc.).  We do
   * not use `this` because RDDs are user-visible, so users might have added their own locking on
   * RDDs; sharing that could lead to a deadlock.
   *
   * RDD 所有可变状态的锁。使用独立的锁对象而不是 this，是为了避免与用户代码的锁产生死锁。
   *
   * 需要保护的可变状态包括：
   *   - storageLevel（存储级别）
   *   - dependencies_（依赖列表）
   *   - partitions_（分区数组）
   *
   * 注意：多个 RDD 可能形成依赖链，一个线程可能同时持有多个 RDD 的 stateLock。
   * 为避免死锁，在持有 stateLock 时不要尝试获取其他资源的锁。
   *
   * One thread might hold the lock on many of these, for a chain of RDD dependencies. Deadlocks
   * are possible if we try to lock another resource while holding the stateLock,
   * and the lock acquisition sequence of these locks is not guaranteed to be the same.
   * This can lead lead to a deadlock as one thread might first acquire the stateLock,
   * and then the resource,
   * while another thread might first acquire the resource, and then the stateLock.
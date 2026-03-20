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
   *
   * Executors may reference the shared fields (though they should never mutate them,
   * that only happens on the driver).
   */
  private[spark] val stateLock = new Serializable {}

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  // 依赖和分区会通过调用子类方法获取，checkpoint 时会被覆盖
  @volatile private var dependencies_ : Seq[Dependency[_]] = _
  // When we overwrite the dependencies we keep a weak reference to the old dependencies
  // for user controlled cleanup.
  // 保留旧依赖的弱引用，用于用户控制的清理
  @volatile @transient private var legacyDependencies: WeakReference[Seq[Dependency[_]]] = _
  @volatile @transient private var partitions_ : Array[Partition] = _

  /** An Option holding our checkpoint RDD, if we are checkpointed */
  private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
   * Get the list of dependencies of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   *
   * 获取 RDD 的依赖列表，会考虑是否已经 checkpoint：
   *   - 如果已 checkpoint，返回对 CheckpointRDD 的依赖
   *   - 否则返回原始依赖（惰性初始化，使用双重检查锁定）
   */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        stateLock.synchronized {
          if (dependencies_ == null) {
            dependencies_ = getDependencies
          }
        }
      }
      dependencies_
    }
  }

  /**
   * Get the list of dependencies of this RDD ignoring checkpointing.
   * 获取原始依赖（忽略 checkpoint），用于清理 shuffle 依赖等场景
   */
  final private def internalDependencies: Option[Seq[Dependency[_]]] = {
    if (legacyDependencies != null) {
      legacyDependencies.get
    } else if (dependencies_ != null) {
      Some(dependencies_)
    } else {
      // This case should be infrequent.
      stateLock.synchronized {
        if (dependencies_ == null) {
          dependencies_ = getDependencies
        }
        Some(dependencies_)
      }
    }
  }

  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   *
   * 获取 RDD 的分区数组：
   *   - 如果已 checkpoint，返回 CheckpointRDD 的分区
   *   - 否则返回原始分区（惰性初始化）
   *
   * 同时会验证分区索引的正确性：partition.index 必须与数组索引一致
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        stateLock.synchronized {
          if (partitions_ == null) {
            partitions_ = getPartitions
            // 验证分区索引的正确性
            partitions_.zipWithIndex.foreach { case (partition, index) =>
              require(partition.index == index,
                s"partitions($index).partition == ${partition.index}, but it should equal $index")
            }
          }
        }
      }
      partitions_
    }
  }

  /**
   * Returns the number of partitions of this RDD.
   */
  @Since("1.6.0")
  final def getNumPartitions: Int = partitions.length

  /**
   * Get the preferred locations of a partition, taking into account whether the
   * RDD is checkpointed.
   *
   * 获取分区的首选位置（考虑 checkpoint 状态）：
   *   - 如果已 checkpoint，返回 CheckpointRDD 的首选位置
   *   - 否则返回原始的首选位置
   */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementers of custom
   * subclasses of RDD.
   *
   * 获取分区数据的迭代器，这是 RDD 计算的入口点。
   * 计算策略：
   *   1. 如果 RDD 已持久化（storageLevel != NONE），尝试从缓存读取
   *   2. 如果缓存未命中或未持久化，执行计算（或从 checkpoint 读取）
   *
   * 这个方法会被 TaskRunner 调用来执行任务
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      // 已持久化，尝试从缓存获取或计算后缓存
      getOrCompute(split, context)
    } else {
      // 未持久化，直接计算或从 checkpoint 读取
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Return the ancestors of the given RDD that are related to it only through a sequence of
   * narrow dependencies. This traverses the given RDD's dependency tree using DFS, but maintains
   * no ordering on the RDDs returned.
   *
   * 获取所有通过窄依赖连接的祖先 RDD。
   * 窄依赖的祖先在同一个 Stage 内，可以流水线执行
   */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]

    def visit(rdd: RDD[_]): Unit = {
      // 只考虑窄依赖
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
      val narrowParents = narrowDependencies.map(_.rdd)
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)
        visit(parent)
      }
    }

    visit(this)

    // In case there is a cycle, do not include the root itself
    ancestors.filterNot(_ == this).toSeq
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   *
   * 计算分区或从 checkpoint 读取：
   *   - 如果已 checkpoint 且数据已物化，从 checkpoint 读取
   *   - 否则调用 compute 方法计算
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointedAndMaterialized) {
      // checkpoint 数据已就绪，从 CheckpointRDD 读取
      firstParent[T].iterator(split, context)
    } else {
      // 执行实际的计算
      compute(split, context)
    }
  }

  /**
   * Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached.
   *
   * 从 BlockManager 获取缓存的分区数据，如果不存在则计算并缓存。
   * 这是 Spark 缓存机制的核心实现：
   *   1. 构造 blockId（格式：rdd_${rddId}_${partitionIndex}）
   *   2. 尝试从 BlockManager 获取
   *   3. 如果不存在，计算数据并存入 BlockManager
   */
  private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true
    // This method is called on executors, so we need call SparkEnv.get instead of sc.env.
    // 在 Executor 上调用，需要使用 SparkEnv.get 而不是 sc.env
    SparkEnv.get.blockManager.getOrElseUpdateRDDBlock(
      context.taskAttemptId(), blockId, storageLevel, elementClassTag, () => {
        readCachedBlock = false
        computeOrReadCheckpoint(partition, context)
      }
    ) match {
      // Block hit.
      // 缓存命中
      case Left(blockResult) =>
        if (readCachedBlock) {
          // 统计从缓存读取的字节数
          val existingMetrics = context.taskMetrics().inputMetrics
          existingMetrics.incBytesRead(blockResult.bytes)
          new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
            override def next(): T = {
              existingMetrics.incRecordsRead(1)
              delegate.next()
            }
          }
        } else {
          new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
        }
      // Need to compute the block.
      // 需要计算（此时数据已经在计算过程中被缓存）
      case Right(iter) =>
        new InterruptibleIterator(context, iter)
    }
  }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   *
   * Note: Return statements are NOT allowed in the given body.
   *
   * 在操作作用域内执行代码块，所有在此作用域内创建的 RDD 都会被标记为同一个操作。
   * 这用于在 Spark UI 中显示 RDD 的血缘关系和操作层次
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  // =======================================================================
  // 转换操作（Transformations）- 返回新的 RDD
  // 转换操作是惰性的，只有遇到行动操作才会真正执行
  // =======================================================================
  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   *
   * map 是最基础的转换操作，对每个元素应用函数 f，产生一一对应的结果。
   * 特点：
   *   - 窄依赖（OneToOneDependency）
   *   - 保持分区数不变
   *   - 元素数量不变（preservesPartitionSizes = true）
   */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)  // 闭包清理，移除对外部对象的不必要引用
    new MapPartitionsRDD[U, T](
      this,
      (_, _, iter) => iter.map(cleanF),
      preservesPartitionSizes = true)
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   *
   * flatMap 先对每个元素应用函数（返回可迭代对象），然后将结果展平。
   * 常用于一对多的映射，比如按空格切分文本为单词
   */
  def flatMap[U: ClassTag](f: T => IterableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF))
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   *
   * filter 过滤出满足条件的元素。
   * 特点：
   *   - 窄依赖
   *   - 保持分区器（如果有的话）
   */
  def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (_, _, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   *
   * distinct 去重。实现策略：
   *   - 如果 RDD 已有分区器且分区数相同，在分区内局部去重
   *   - 否则使用 reduceByKey 进行全局去重（会触发 Shuffle）
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    def removeDuplicatesInPartition(partition: Iterator[T]): Iterator[T] = {
      // Create an instance of external append only map which ignores values.
      // 使用 ExternalAppendOnlyMap 进行去重，可以处理大数据量（会溢写到磁盘）
      val map = new ExternalAppendOnlyMap[T, Null, Null](
        createCombiner = _ => null,
        mergeValue = (a, b) => a,
        mergeCombiners = (a, b) => a)
      map.insertAll(partition.map(_ -> null))
      map.iterator.map(_._1)
    }
    partitioner match {
      case Some(_) if numPartitions == partitions.length =>
        // 已有分区器且分区数相同，直接在分区内去重
        mapPartitions(removeDuplicatesInPartition, preservesPartitioning = true)
      case _ =>
        // 使用 reduceByKey 进行全局去重
        map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    }
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): RDD[T] = withScope {
    distinct(partitions.length)
  }

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   *
   * repartition 重新分区。
   * 内部调用 coalesce(shuffle = true)，总是会触发 Shuffle。
   * 如果只是减少分区数，建议使用 coalesce(shuffle = false) 避免 Shuffle
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   *
   * coalesce 合并分区。
   *
   * shuffle = false 时（默认）：
   *   - 窄依赖，不触发 Shuffle
   *   - 只能减少分区数，不能增加
   *   - 可能导致数据倾斜（多个旧分区合并到同一个新分区）
   *
   * shuffle = true 时：
   *   - 宽依赖，触发 Shuffle
   *   - 可以增加或减少分区数
   *   - 数据会重新分布，更均匀
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions. If a larger number
   * of partitions is requested, it will stay at the current number of partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * @note With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner. The optional partition coalescer
   * passed in must be serializable.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
      : RDD[T] = withScope {
    require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
    if (shuffle) {
      /** Distributes elements evenly across output partitions, starting from a random partition. */
      // 使用随机起点将元素均匀分布到输出分区
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = new XORShiftRandom(index).nextInt(numPartitions)
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)]

      // include a shuffle step so that our upstream tasks are still distributed
      // 添加 Shuffle 步骤以保持上游任务的并行执行
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](
          mapPartitionsWithIndexInternal(distributePartition, isOrderSensitive = true),
          new HashPartitioner(numPartitions)),
        numPartitions,
        partitionCoalescer).values
    } else {
      // 无 Shuffle，直接合并分区
      new CoalescedRDD(this, numPartitions, partitionCoalescer)
    }
  }

  /**
   * Return a sampled subset of this RDD.
   *
   * 对 RDD 进行采样。
   *
   * @param withReplacement 是否放回采样（true：泊松采样，可能重复；false：伯努利采样，不重复）
   * @param fraction 采样比例
   *   - 无放回采样：每个元素被选中的概率，范围 [0, 1]
   *   - 放回采样：每个元素被选中的期望次数，必须 >= 0
   * @param seed 随机数种子
   *
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   * of the given [[RDD]].
   */
  def sample(
      withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T] = {
    require(fraction >= 0,
      s"Fraction must be nonnegative, but got ${fraction}")

    withScope {
      if (withReplacement) {
        // 放回采样使用泊松采样器
        new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
      } else {
        // 无放回采样使用伯努利采样器
        new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
      }
    }
  }

  /**
   * Randomly splits this RDD with the provided weights.
   *
   * 按权重随机切分 RDD 为多个子 RDD，常用于机器学习中划分训练集和测试集。
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed random seed
   *
   * @return split RDDs in an array
   */
  def randomSplit(
      weights: Array[Double],
      seed: Long = Utils.random.nextLong): Array[RDD[T]] = {
    require(weights.forall(_ >= 0),
      s"Weights must be nonnegative, but got ${weights.mkString("[", ",", "]")}")
    require(weights.sum > 0,
      s"Sum of weights must be positive, but got ${weights.mkString("[", ",", "]")}")

    withScope {
      // 归一化权重并计算累积分布
      val sum = weights.sum
      val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
      // 使用滑动窗口创建多个采样 RDD
      normalizedCumWeights.sliding(2).map { x =>
        randomSampleWithRange(x(0), x(1), seed)
      }.toArray
    }
  }


  /**
   * Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability
   * range.
   * @param lb lower bound to use for the Bernoulli sampler
   * @param ub upper bound to use for the Bernoulli sampler
   * @param seed the seed for the Random number generator
   * @return A random sub-sample of the RDD without replacement.
   */
  private[spark] def randomSampleWithRange(lb: Double, ub: Double, seed: Long): RDD[T] = {
    this.mapPartitionsWithIndex( { (index, partition) =>
      val sampler = new BernoulliCellSampler[T](lb, ub)
      sampler.setSeed(seed + index)
      sampler.sample(partition)
    }, isOrderSensitive = true, preservesPartitioning = true)
  }

  /**
   * Return a fixed-size sampled subset of this RDD in an array
   *
   * 返回固定大小的采样结果数组。与 sample 不同，takeSample 保证返回恰好 num 个元素。
   *
   * 算法策略：
   *   1. 先估算需要的采样比例
   *   2. 执行采样
   *   3. 如果样本不够，增大比例重试
   *   4. 最后随机打乱并取前 num 个
   *
   * @param withReplacement whether sampling is done with replacement
   * @param num size of the returned sample
   * @param seed seed for the random number generator
   * @return sample of specified size in an array
   *
   * @note this method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def takeSample(
      withReplacement: Boolean,
      num: Int,
      seed: Long = Utils.random.nextLong): Array[T] = withScope {
    val numStDev = 10.0

    require(num >= 0, "Negative number of elements requested")
    require(num <= (Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt),
      "Cannot support a sample size > Int.MaxValue - " +
      s"$numStDev * math.sqrt(Int.MaxValue)")

    if (num == 0) {
      new Array[T](0)
    } else {
      val initialCount = this.count()
      if (initialCount == 0) {
        new Array[T](0)
      } else {
        val rand = new Random(seed)
        // 如果请求的样本数大于等于总数，直接收集所有数据并打乱
        if (!withReplacement && num >= initialCount) {
          Utils.randomizeInPlace(this.collect(), rand)
        } else {
          // 计算采样比例并执行采样
          val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
            withReplacement)
          var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

          // If the first sample didn't turn out large enough, keep trying to take samples;
          // this shouldn't happen often because we use a big multiplier for the initial size
          // 如果样本不够，重试（这种情况应该很少发生）
          var numIters = 0
          while (samples.length < num) {
            logWarning(log"Needed to re-sample due to insufficient sample size. " +
              log"Repeat #${MDC(NUM_ITERATIONS, numIters)}")
            samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
            numIters += 1
          }
          // 打乱并取前 num 个
          Utils.randomizeInPlace(samples, rand).take(num)
        }
      }
    }
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   *
   * 合并两个 RDD，返回包含两者所有元素的新 RDD。
   * 注意：相同的元素会出现多次，如需去重请使用 distinct()
   */
  def union(other: RDD[T]): RDD[T] = withScope {
    sc.union(this, other)
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   * ++ 是 union 的别名
   */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }

  /**
   * Return this RDD sorted by the given key function.
   *
   * 按指定的 key 函数排序。内部先转换为 K-V 对，使用 sortByKey 排序，再取 values
   */
  def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.length)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
    this.keyBy[K](f)
        .sortByKey(ascending, numPartitions)
        .values
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * 返回两个 RDD 的交集（自动去重）。
   * 实现方式：使用 cogroup 找出同时存在于两个 RDD 中的元素
   *
   * @note This method performs a shuffle internally.
   */
  def intersection(other: RDD[T]): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * @note This method performs a shuffle internally.
   *
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(
      other: RDD[T],
      partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *
   * @note This method performs a shuffle internally.
   *
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    intersection(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   *
   * glom 将每个分区的所有元素收集到一个数组中。
   * 结果 RDD 的每个元素是一个数组，包含原分区的所有数据
   */
  def glom(): RDD[Array[T]] = withScope {
    new MapPartitionsRDD[Array[T], T](this, (_, _, iter) => Iterator(iter.toArray))
  }

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   *
   * 笛卡尔积，返回两个 RDD 所有元素对的组合。
   * 警告：结果大小是两个 RDD 大小的乘积，可能非常大！
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(sc, this, other)
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * groupBy 按 key 分组，返回 (key, Iterable[T]) 的 RDD。
   *
   * 性能警告：如果你是为了聚合（如求和、平均），请使用 aggregateByKey 或 reduceByKey，
   * 它们会先在 Map 端局部聚合，大大减少 Shuffle 数据量。
   * groupBy 会将所有数据都 Shuffle 到 Reduce 端，可能导致内存溢出。
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy[K](f, defaultPartitioner(this))
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupBy[K](
      f: T => K,
      numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy(f, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   * or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
      : RDD[(K, Iterable[T])] = withScope {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   *
   * pipe 将 RDD 数据通过管道传递给外部命令处理。
   * 每个分区启动一个进程，将数据写入进程的 stdin，从 stdout 读取结果。
   * 常用于调用非 Scala/Java 实现的处理程序
   */
  def pipe(command: String): RDD[String] = withScope {
    // Similar to Runtime.exec(), if we are given a single string, split it into words
    // using a standard StringTokenizer (i.e. by spaces)
    pipe(PipedRDD.tokenize(command))
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String, env: Map[String, String]): RDD[String] = withScope {
    // Similar to Runtime.exec(), if we are given a single string, split it into words
    // using a standard StringTokenizer (i.e. by spaces)
    pipe(PipedRDD.tokenize(command), env)
  }

  /**
   * Return an RDD created by piping elements to a forked external process. The resulting RDD
   * is computed by executing the given process once per partition. All elements
   * of each input partition are written to a process's stdin as lines of input separated
   * by a newline. The resulting partition consists of the process's stdout output, with
   * each line of stdout resulting in one element of the output partition. A process is invoked
   * even for empty partitions.
   *
   * 通过管道将数据传递给外部进程处理。
   *
   * 工作原理：
   *   1. 每个分区启动一个外部进程
   *   2. 将分区数据逐行写入进程的 stdin
   *   3. 从进程的 stdout 读取结果，每行作为输出的一个元素
   *
   * The print behavior can be customized by providing two functions.
   *
   * @param command command to run in forked process.
   * @param env environment variables to set.
   * @param printPipeContext Before piping elements, this function is called as an opportunity
   *                         to pipe context data. Print line function (like out.println) will be
   *                         passed as printPipeContext's parameter.
   * @param printRDDElement Use this function to customize how to pipe elements. This function
   *                        will be called with each RDD element as the 1st parameter, and the
   *                        print line function (like out.println()) as the 2nd parameter.
   *                        An example of pipe the RDD data of groupBy() in a streaming way,
   *                        instead of constructing a huge String to concat all the elements:
   *                        {{{
   *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
   *                          for (e <- record._2) {f(e)}
   *                        }}}
   * @param separateWorkingDir Use separate working directories for each task.
   * @param bufferSize Buffer size for the stdin writer for the piped process.
   * @param encoding Char encoding used for interacting (via stdin, stdout and stderr) with
   *                 the piped process
   * @return the result RDD
   */
  def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false,
      bufferSize: Int = 8192,
      encoding: String = Codec.defaultCharsetCodec.name): RDD[String] = withScope {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir,
      bufferSize,
      encoding)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * mapPartitions 对每个分区应用函数，比 map 更高效（减少函数调用开销）。
   *
   * 适用场景：
   *   - 需要初始化昂贵资源（如数据库连接）时，每个分区只初始化一次
   *   - 需要对分区内数据进行批量操作
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   *
   * @param preservesPartitioning 函数是否保持分区器不变（只有当函数不修改 key 时才为 true）
   */
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }

  /**
   * [performance] Spark's internal mapPartitionsWithIndex method that skips closure cleaning.
   * It is a performance API to be used carefully only if we are sure that the RDD elements are
   * serializable and don't require closure cleaning.
   *
   * Spark 内部的高性能版本，跳过闭包清理。仅在确定不需要闭包清理时使用
   *
   * @param preservesPartitioning indicates whether the input function preserves the partitioner,
   *                              which should be `false` unless this is a pair RDD and the input
   *                              function doesn't modify the keys.
   * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
   *                         sensitive, it may return totally different result when the input order
   *                         is changed. Mostly stateful functions are order-sensitive.
   * @param preservesPartitionSizes Whether the input function preserves the number of rows in each
   *                                partition. This is true for 1:1 element mappings like `map`.
   *                                Used to optimize `RDD.zipWithIndex` by counting rows on a
   *                                cheaper ancestor RDD instead of the immediate parent.
   */
  private[spark] def mapPartitionsWithIndexInternal[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false,
      isOrderSensitive: Boolean = false,
      preservesPartitionSizes: Boolean = false): RDD[U] = withScope {
    new MapPartitionsRDD(
      this,
      (_: TaskContext, index: Int, iter: Iterator[T]) => f(index, iter),
      preservesPartitioning = preservesPartitioning,
      isOrderSensitive = isOrderSensitive,
      preservesPartitionSizes = preservesPartitionSizes)
  }

  /**
   * [performance] Spark's internal mapPartitions method that skips closure cleaning.
   */
  private[spark] def mapPartitionsInternal[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    new MapPartitionsRDD(
      this,
      (_: TaskContext, _: Int, iter: Iterator[T]) => f(iter),
      preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   *
   * mapPartitionsWithIndex 与 mapPartitions 类似，但函数会接收分区索引。
   * 适用于需要知道分区编号的场景，比如为每个分区生成不同的随机种子
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning)
  }

  /**
   * Return a new RDD by applying an evaluator to each partition of this RDD. The given evaluator
   * factory will be serialized and sent to executors, and each task will create an evaluator with
   * the factory, and use the evaluator to transform the data of the input partition.
   *
   * 使用 Evaluator 模式处理分区数据。Evaluator 工厂会被序列化发送到 Executor，
   * 每个任务创建自己的 Evaluator 实例来处理数据。
   * 这种模式适合需要复杂初始化逻辑的场景
   */
  @DeveloperApi
  @Since("3.5.0")
  def mapPartitionsWithEvaluator[U: ClassTag](
      evaluatorFactory: PartitionEvaluatorFactory[T, U]): RDD[U] = withScope {
    mapPartitionsWithEvaluator(evaluatorFactory, false)
  }

  private[spark] def mapPartitionsWithEvaluator[U: ClassTag](
      evaluatorFactory: PartitionEvaluatorFactory[T, U],
      preservesPartitionSizes: Boolean): RDD[U] = withScope {
    new MapPartitionsWithEvaluatorRDD(this, evaluatorFactory,
      preservesPartitionSizes = preservesPartitionSizes)
  }

  /**
   * Zip this RDD's partitions with another RDD and return a new RDD by applying an evaluator to
   * the zipped partitions. Assumes that the two RDDs have the *same number of partitions*, but
   * does *not* require them to have the same number of elements in each partition.
   */
  @DeveloperApi
  @Since("3.5.0")
  def zipPartitionsWithEvaluator[U: ClassTag](
      rdd2: RDD[T],
      evaluatorFactory: PartitionEvaluatorFactory[T, U]): RDD[U] = withScope {
    new ZippedPartitionsWithEvaluatorRDD(this, rdd2, evaluatorFactory)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   *
   * `isOrderSensitive` indicates whether the function is order-sensitive. If it is order
   * sensitive, it may return totally different result when the input order
   * is changed. Mostly stateful functions are order-sensitive.
   */
  private[spark] def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean,
      isOrderSensitive: Boolean): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning,
      isOrderSensitive = isOrderSensitive)
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   *
   * zip 将两个 RDD 按位置配对，类似 Scala 的 zip 操作。
   * 前提条件：两个 RDD 必须有相同的分区数和相同的分区内元素数
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[(T, U)] {
        def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true
          case (false, false) => false
          case _ => throw SparkCoreErrors.canOnlyZipRDDsWithSamePartitionSizeError()
        }
        def next(): (T, U) = (thisIter.next(), otherIter.next())
      }
    }
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   *
   * zipPartitions 将多个 RDD 的分区配对后应用函数。
   * 与 zip 不同的是，不要求分区内元素数相同，由用户函数处理配对逻辑
   */
  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, preservesPartitioning = false)(f)
  }

  // 三个 RDD 的 zipPartitions
  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, preservesPartitioning = false)(f)
  }

  // 四个 RDD 的 zipPartitions
  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning = false)(f)
  }


  // =======================================================================
  // 行动操作（Actions）- 触发 Job 执行并返回结果到 Driver
  // 行动操作会触发整个 RDD 血缘的计算
  // =======================================================================
  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   *
   * foreach 对每个元素执行函数（无返回值），常用于将数据写入外部存储
   */
  def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
  }

  /**
   * Applies a function f to each partition of this RDD.
   *
   * foreachPartition 对每个分区执行函数，比 foreach 更高效。
   * 常用于需要初始化资源的场景，如批量写入数据库
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   *
   * collect 将所有数据收集到 Driver 端，返回数组。
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   * 警告：只有当数据量小到能放入 Driver 内存时才能使用！
   */
  def collect(): Array[T] = withScope {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    import org.apache.spark.util.ArrayImplicits._
    Array.concat(results.toImmutableArraySeq: _*)
  }

  /**
   * Return an iterator that contains all of the elements in this RDD.
   *
   * toLocalIterator 返回包含所有元素的迭代器，按分区依次获取数据。
   * 内存占用是最大分区的大小（而不是整个 RDD）
   *
   * The iterator will consume as much memory as the largest partition in this RDD.
   *
   * @note This results in multiple Spark jobs, and if the input RDD is the result
   * of a wide transformation (e.g. join with different partitioners), to avoid
   * recomputing the input RDD should be cached first.
   * 注意：每个分区触发一个 Job，如果 RDD 是宽依赖的结果，建议先 cache
   */
  def toLocalIterator: Iterator[T] = withScope {
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p)).head
    }
    partitions.indices.iterator.flatMap(i => collectPartition(i))
  }

  /**
   * Return an RDD that contains all matching values by applying `f`.
   * 使用偏函数 collect，只保留函数定义域内的元素并转换
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    filter(cleanF.isDefinedAt).map(cleanF)
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * subtract 返回差集（在 this 中但不在 other 中的元素）
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   */
  def subtract(other: RDD[T]): RDD[T] = withScope {
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.length)))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    subtract(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(
      other: RDD[T],
      p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      // 如果已有相同分区器，创建一个解包元组的分区器
      val p2 = new Partitioner() {
        override def numPartitions: Int = p.numPartitions
        override def getPartition(k: Any): Int = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   *
   * reduce 使用二元函数聚合 RDD 的所有元素。
   *
   * 执行流程：
   *   1. 每个分区内部使用 reduceLeft 聚合
   *   2. 将各分区结果收集到 Driver 进行最终聚合
   *
   * 注意：函数必须满足交换律和结合律，因为各分区的计算是并行的
   */
  def reduce(f: (T, T) => T): T = withScope {
    val cleanF = sc.clean(f)
    // 分区内聚合
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    // 合并各分区结果
    val mergeResult = (_: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    // 如果 RDD 为空则抛出异常
    jobResult.getOrElse(throw SparkCoreErrors.emptyCollectionError())
  }

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * treeReduce 使用树形结构进行归约，减少 Driver 的压力。
   * 当分区很多时，普通 reduce 需要在 Driver 端合并所有分区结果，
   * treeReduce 则在 Executor 端进行多层合并，再将最终结果发送到 Driver
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val cleanF = context.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val partiallyReduced = mapPartitions(it => Iterator(reducePartition(it)))
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    partiallyReduced.treeAggregate(Option.empty[T])(op, op, depth)
      .getOrElse(throw SparkCoreErrors.emptyCollectionError())
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function
   * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * fold 使用初始值和二元函数聚合 RDD。与 reduce 不同，fold 提供初始值。
   *
   * 执行流程：
   *   1. 每个分区使用 zeroValue 和 op 进行 fold
   *   2. 在 Driver 端合并各分区结果
   *
   * This behaves somewhat differently from fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then fold those results into the final result, rather than
   * apply the fold to each element sequentially in some defined ordering. For functions
   * that are not commutative, the result may differ from that of a fold applied to a
   * non-distributed collection.
   *
   * @param zeroValue the initial value for the accumulated result of each partition for the `op`
   *                  operator, and also the initial value for the combine results from different
   *                  partitions for the `op` operator - this will typically be the neutral
   *                  element (e.g. `Nil` for list concatenation or `0` for summation)
   * @param op an operator used to both accumulate results within a partition and combine results
   *                  from different partitions
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    // 克隆零值，因为它会被序列化发送到各个任务
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp)
    val mergeResult = (_: Int, taskResult: T) => jobResult = op(jobResult, taskResult)
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.IterableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   *
   * aggregate 是最灵活的聚合操作，可以返回与 RDD 元素不同类型的结果。
   *
   * 执行流程：
   *   1. 每个分区使用 seqOp 将元素聚合到中间结果（从 zeroValue 开始）
   *   2. 使用 combOp 合并各分区的中间结果
   *
   * 例如：计算平均值
   *   aggregate((0, 0))(
   *     (acc, v) => (acc._1 + v, acc._2 + 1),  // seqOp：累加值和计数
   *     (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // combOp：合并
   *   )
   *
   * @param zeroValue the initial value for the accumulated result of each partition for the
   *                  `seqOp` operator, and also the initial value for the combine results from
   *                  different partitions for the `combOp` operator - this will typically be the
   *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
   * @param seqOp an operator used to accumulate results within a partition
   * @param combOp an associative operator used to combine results from different partitions
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.serializer.newInstance())
    val cleanSeqOp = sc.clean(seqOp)
    val aggregatePartition = (it: Iterator[T]) => it.foldLeft(zeroValue)(cleanSeqOp)
    val mergeResult = (_: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   * This method is semantically identical to [[org.apache.spark.rdd.RDD#aggregate]].
   *
   * treeAggregate 使用树形结构进行聚合，减少 Driver 的压力。
   * 特别适合分区数很多的场景
   *
   * @param depth suggested depth of the tree (default: 2)
   */
  def treeAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int = 2): U = withScope {
      treeAggregate(zeroValue, seqOp, combOp, depth, finalAggregateOnExecutor = false)
  }

  /**
   * [[org.apache.spark.rdd.RDD#treeAggregate]] with a parameter to do the final
   * aggregation on the executor
   *
   * @param finalAggregateOnExecutor do final aggregation on executor
   */
  def treeAggregate[U: ClassTag](
      zeroValue: U,
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int,
      finalAggregateOnExecutor: Boolean): U = withScope {
      require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (partitions.length == 0) {
      Utils.clone(zeroValue, context.env.closureSerializer.newInstance())
    } else {
      val cleanSeqOp = context.clean(seqOp)
      val cleanCombOp = context.clean(combOp)
      // 第一步：每个分区内聚合
      val aggregatePartition =
        (it: Iterator[T]) => it.foldLeft(zeroValue)(cleanSeqOp)
      var partiallyAggregated: RDD[U] = mapPartitions(it => Iterator(aggregatePartition(it)))
      var numPartitions = partiallyAggregated.partitions.length
      // 计算每层合并的分区数
      val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
      // If creating an extra level doesn't help reduce
      // the wall-clock time, we stop tree aggregation.

      // Don't trigger TreeAggregation when it doesn't save wall-clock time
      // 多层合并，直到分区数足够小
      while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
        numPartitions /= scale
        val curNumPartitions = numPartitions
        partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
          (i, iter) => iter.map((i % curNumPartitions, _))
        }.foldByKey(zeroValue, new HashPartitioner(curNumPartitions))(cleanCombOp).values
      }
      // 如果指定在 Executor 上进行最终聚合
      if (finalAggregateOnExecutor && partiallyAggregated.partitions.length > 1) {
        // map the partially aggregated rdd into a key-value rdd
        // do the computation in the single executor with one partition
        // get the new RDD[U]
        partiallyAggregated = partiallyAggregated
          .map(v => (0.toByte, v))
          .foldByKey(zeroValue, new ConstantPartitioner)(cleanCombOp)
          .values
      }
      val copiedZeroValue = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
      // 最终在 Driver 端合并
      partiallyAggregated.fold(copiedZeroValue)(cleanCombOp)
    }
  }

  /**
   * Return the number of elements in the RDD.
   * count 返回 RDD 中元素的个数
   */
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   *
   * countApprox 是 count 的近似版本，在超时时间内返回（可能不完整的）结果。
   * 适用于数据量很大、可以接受近似结果的场景
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
  def countApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = withScope {
    require(0.0 <= confidence && confidence <= 1.0, s"confidence ($confidence) must be in [0,1]")
    val countElements: (TaskContext, Iterator[T]) => Long = { (_, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.length, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
   *
   * countByValue 统计每个不同值出现的次数，返回 Map[T, Long]
   *
   * @note This method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
   * 警告：结果会全部加载到 Driver 内存，只适合不同值较少的情况
   *
   * To handle very large results, consider using
   *
   * {{{
   * rdd.map(x => (x, 1L)).reduceByKey(_ + _)
   * }}}
   *
   * , which returns an RDD[T, Long] instead of a map.
   */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = withScope {
    map(value => (value, null)).countByKey()
  }

  /**
   * Approximate version of countByValue().
   *
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param confidence the desired statistical confidence in the result
   * @return a potentially incomplete result, with error bounds
   */
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
      (implicit ord: Ordering[T] = null)
      : PartialResult[Map[T, BoundedDouble]] = withScope {
    require(0.0 <= confidence && confidence <= 1.0, s"confidence ($confidence) must be in [0,1]")
    // 数组类型不支持近似计数
    if (elementClassTag.runtimeClass.isArray) {
      throw SparkCoreErrors.countByValueApproxNotSupportArraysError()
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T, Long] = { (_, iter) =>
      val map = new OpenHashMap[T, Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.length, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * countApproxDistinct 使用 HyperLogLog 算法估算不同元素的数量。
   * 适用于精确计算代价太高的超大数据集
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero (`sp` is greater
   * than `p`) would trigger sparse representation of registers, which may reduce the memory
   * consumption and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  def countApproxDistinct(p: Int, sp: Int): Long = withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(if (p < 4) 4 else p, 0)
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   *
   * zipWithIndex 为每个元素添加全局唯一的索引。
   * 索引顺序：先按分区索引，再按分区内顺序。
   *
   * 注意：此方法需要先统计各分区的元素数，会触发一个 Job
   *
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
   *
   * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The index assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithIndex(): RDD[(T, Long)] = withScope {
    new ZippedWithIndexRDD(this)
  }

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
   *
   * zipWithUniqueId 为每个元素生成唯一 ID（但可能有间隙）。
   * 第 k 个分区的元素 ID 为 k, n+k, 2n+k, ...（n 是分区数）
   *
   * 与 zipWithIndex 的区别：不需要触发额外的 Job，但 ID 可能有间隙
   *
   * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithUniqueId(): RDD[(T, Long)] = withScope {
    val n = this.partitions.length.toLong
    this.mapPartitionsWithIndex { case (k, iter) =>
      Utils.getIteratorZipWithIndex(iter, 0L).map { case (item, i) =>
        (item, i * n + k)
      }
    }
  }

  /**
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
   *
   * take 获取 RDD 的前 num 个元素。
   *
   * 算法优化：
   *   1. 先扫描一个分区，估算需要扫描多少个分区
   *   2. 如果不够，逐步增加扫描的分区数
   *   3. 使用指数增长策略避免扫描过多分区
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   *
   * @note Due to complications in the internal implementation, this method will raise
   * an exception if called on an RDD of `Nothing` or `Null`.
   */
  def take(num: Int): Array[T] = withScope {
    val scaleUpFactor = Math.max(conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
    if (num == 0) {
      new Array[T](0)
    } else {
      val buf = new ArrayBuffer[T]
      val totalParts = this.partitions.length
      var partsScanned = 0
      // 逐步扫描分区直到收集够元素
      while (buf.size < num && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        var numPartsToTry = conf.get(RDD_LIMIT_INITIAL_NUM_PARTITIONS)
        val left = num - buf.size
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, multiply by
          // limitScaleUpFactor and retry. Otherwise, interpolate the number of partitions we need
          // to try, but overestimate it by 50%. We also cap the estimation in the end.
          // 如果上一轮没找到任何元素，指数增长扫描分区数
          if (buf.isEmpty) {
            numPartsToTry = partsScanned * scaleUpFactor
          } else {
            // As left > 0, numPartsToTry is always >= 1
            // 根据当前收集速率估算需要的分区数，上浮 50% 以确保足够
            numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.size).toInt
            numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
          }
        }

        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts))
        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)

        res.foreach(buf ++= _.take(num - buf.size))
        partsScanned += p.size
      }

      buf.toArray
    }
  }

  /**
   * Return the first element in this RDD.
   * first 返回 RDD 的第一个元素
   */
  def first(): T = withScope {
    take(1) match {
      case Array(t) => t
      case _ => throw SparkCoreErrors.emptyCollectionError()
    }
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of
   * [[takeOrdered]]. For example:
   *
   * top 返回最大的 k 个元素（按降序排列）
   *
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    takeOrdered(num)(ord.reverse)
  }

  /**
   * Returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   *
   * takeOrdered 返回最小的 k 个元素（按升序排列）
   *
   * 算法优化：
   *   1. 每个分区使用优先队列（堆）保留最小的 k 个元素
   *   2. 在 Driver 端合并各分区的 top-k
   *
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   *
   * @param num k, the number of elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    if (num == 0 || this.getNumPartitions == 0) {
      Array.empty
    } else {
      this.mapPartitionsWithIndex { case (pid, iter) =>
        if (iter.nonEmpty) {
          // Priority keeps the largest elements, so let's reverse the ordering.
          // 每个分区取最小的 num 个元素
          Iterator.single(collectionUtils.takeOrdered(iter, num)(ord).toArray)
        } else if (pid == 0) {
          // make sure partition 0 always returns an array to avoid reduce on empty RDD
          // 确保分区 0 返回数组，避免对空 RDD 进行 reduce
          Iterator.single(Array.empty[T])
        } else {
          Iterator.empty
        }
      }.reduce { (array1, array2) =>
        // 合并两个有序数组，取前 num 个
        val size = math.min(num, array1.length + array2.length)
        val array = Array.ofDim[T](size)
        collectionUtils.mergeOrdered[T](Seq(array1, array2))(ord).copyToArray(array, 0, size)
        array
      }
    }
  }

  /**
   * Returns the max of this RDD as defined by the implicit Ordering[T].
   * max 返回 RDD 中的最大元素
   * @return the maximum element of the RDD
   * */
  def max()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.max)
  }

  /**
   * Returns the min of this RDD as defined by the implicit Ordering[T].
   * min 返回 RDD 中的最小元素
   * @return the minimum element of the RDD
   * */
  def min()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.min)
  }

  /**
   * isEmpty 判断 RDD 是否为空
   *
   * @note Due to complications in the internal implementation, this method will raise an
   * exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
   * because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
   * (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
   * @return true if and only if the RDD contains no elements at all. Note that an RDD
   *         may be empty even when it has at least 1 partition.
   */
  def isEmpty(): Boolean = withScope {
    partitions.length == 0 || take(1).length == 0
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   * saveAsTextFile 将 RDD 保存为文本文件，每个元素的 toString() 结果作为一行
   */
  def saveAsTextFile(path: String): Unit = withScope {
    saveAsTextFile(path, null)
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   * 将 RDD 保存为压缩文本文件
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = withScope {
    this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        require(x != null, "text files do not allow null rows")
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }.saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
   * saveAsObjectFile 将 RDD 序列化后保存为 SequenceFile
   */
  def saveAsObjectFile(path: String): Unit = withScope {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   * keyBy 将元素转换为 (key, value) 对，key 由函数 f 生成
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
    val cleanedF = sc.clean(f)
    map(x => (cleanedF(x), x))
  }

  /** A private method for tests, to look at the contents of each partition */
  private[spark] def collectPartitions(): Array[Array[T]] = withScope {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   *
   * checkpoint 将 RDD 保存到可靠存储（如 HDFS），并截断血缘关系。
   *
   * 使用场景：
   *   - 血缘链很长，重算代价高
   *   - 迭代计算中的中间结果
   *
   * 注意事项：
   *   1. 必须在任何 action 之前调用
   *   2. 建议先 persist 到内存，否则会重新计算
   *   3. checkpoint 目录需要通过 SparkContext#setCheckpointDir 设置
   *
   * The data is only checkpointed when `doCheckpoint()` is called, and this only happens at the
   * end of the first action execution on this RDD. The final data that is checkpointed after the
   * first action may be different from the data that was used during the action, due to
   * non-determinism of the underlying operation and retries. If the purpose of the checkpoint is
   * to achieve saving a deterministic snapshot of the data, an eager action may need to be called
   * first on the RDD to trigger the checkpoint.
   */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    if (context.checkpointDir.isEmpty) {
      throw SparkCoreErrors.checkpointDirectoryHasNotBeenSetInSparkContextError()
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }

  /**
   * Mark this RDD for local checkpointing using Spark's existing caching layer.
   *
   * localCheckpoint 使用本地缓存实现检查点，牺牲容错性换取性能。
   *
   * 与 checkpoint 的区别：
   *   - checkpoint：数据保存到 HDFS 等可靠存储，支持容错
   *   - localCheckpoint：数据保存到 Executor 本地存储，不支持容错
   *
   * 适用场景：
   *   - 血缘链很长需要截断
   *   - 可以接受 Executor 失败导致的重算
   *   - 对性能要求高
   *
   * This method is for users who wish to truncate RDD lineages while skipping the expensive
   * step of replicating the materialized data in a reliable distributed file system. This is
   * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
   *
   * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
   * data is written to ephemeral local storage in the executors instead of to a reliable,
   * fault-tolerant storage. The effect is that if an executor fails during the computation,
   * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
   *
   * This is NOT safe to use with dynamic allocation, which removes executors along
   * with their cached blocks. If you must use both features, you are advised to set
   * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
   *
   * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
   *
   * The data is only checkpointed when `doCheckpoint()` is called, and this only happens at the
   * end of the first action execution on this RDD. The final data that is checkpointed after the
   * first action may be different from the data that was used during the action, due to
   * non-determinism of the underlying operation and retries. If the purpose of the checkpoint is
   * to achieve saving a deterministic snapshot of the data, an eager action may need to be called
   * first on the RDD to trigger the checkpoint.
   */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    // 警告：本地检查点与动态资源分配不兼容
    if (Utils.isDynamicAllocationEnabled(conf) &&
      conf.contains(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT)) {
      logWarning("Local checkpointing is NOT safe to use with dynamic allocation, " +
        "which removes executors along with their cached blocks. If you must use both " +
        "features, you are advised to set `spark.dynamicAllocation.cachedExecutorIdleTimeout` " +
        "to a high value. E.g. If you plan to use the RDD for 1 hour, set the timeout to " +
        "at least 1 hour.")
    }

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.
    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.

    // 确保 RDD 被持久化（本地检查点需要使用磁盘）
    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
    // 如果已经 checkpoint 且数据已物化，不能覆盖 checkpointData
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.")
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * isCheckpointed 检查 RDD 是否已完成 checkpoint（数据已物化）
   */
  def isCheckpointed: Boolean = isCheckpointedAndMaterialized

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * This is introduced as an alias for `isCheckpointed` to clarify the semantics of the
   * return value. Exposed for testing.
   */
  private[spark] def isCheckpointedAndMaterialized: Boolean =
    checkpointData.exists(_.isCheckpointed)

  /**
   * Return whether this RDD is marked for local checkpointing.
   * Exposed for testing.
   */
  private[rdd] def isLocallyCheckpointed: Boolean = {
    checkpointData match {
      case Some(_: LocalRDDCheckpointData[T]) => true
      case _ => false
    }
  }

  /**
   * Return whether this RDD is reliably checkpointed and materialized.
   */
  private[spark] def isReliablyCheckpointed: Boolean = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[_]) if reliable.isCheckpointed => true
      case _ => false
    }
  }

  /**
   * Gets the name of the directory to which this RDD was checkpointed.
   * This is not defined if the RDD is checkpointed locally.
   * getCheckpointFile 获取 checkpoint 文件的路径（仅适用于可靠 checkpoint）
   */
  def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }

  /**
   * Removes an RDD's shuffles and it's non-persisted ancestors.
   * When running without a shuffle service, cleaning up shuffle files enables downscaling.
   * If you use the RDD after this call, you should checkpoint and materialize it first.
   * If you are uncertain of what you are doing, please do not use this feature.
   *
   * cleanShuffleDependencies 清理 RDD 的 Shuffle 依赖及其未持久化的祖先。
   * 在没有 Shuffle Service 时，清理 Shuffle 文件可以释放 Executor
   *
   * Additional techniques for mitigating orphaned shuffle files:
   *   * Tuning the driver GC to be more aggressive, so the regular context cleaner is triggered
   *   * Setting an appropriate TTL for shuffle files to be auto cleaned
   */
  @DeveloperApi
  @Since("3.1.0")
  def cleanShuffleDependencies(blocking: Boolean = false): Unit = {
    sc.cleaner.foreach { cleaner =>
      /**
       * Clean the shuffles & all of its parents.
       */
      def cleanEagerly(dep: Dependency[_]): Unit = {
        dep match {
          case dependency: ShuffleDependency[_, _, _] =>
            val shuffleId = dependency.shuffleId
            cleaner.doCleanupShuffle(shuffleId, blocking)
          case _ => // do nothing
        }
        val rdd = dep.rdd
        val rddDepsOpt = rdd.internalDependencies
        // 只清理未持久化的 RDD 的依赖
        if (rdd.getStorageLevel == StorageLevel.NONE) {
          rddDepsOpt.foreach(deps => deps.foreach(cleanEagerly))
        }
      }
      internalDependencies.foreach(deps => deps.foreach(cleanEagerly))
    }
  }


  /**
   * :: Experimental ::
   * Marks the current stage as a barrier stage, where Spark must launch all tasks together.
   * In case of a task failure, instead of only restarting the failed task, Spark will abort the
   * entire stage and re-launch all tasks for this stage.
   *
   * barrier() 将当前 Stage 标记为 Barrier Stage（屏障阶段）。
   *
   * Barrier 执行模式的特点：
   *   - 所有任务必须同时启动
   *   - 如果一个任务失败，整个 Stage 会被中止并重新启动所有任务
   *   - 适用于需要所有任务同时运行的场景，如分布式深度学习
   *
   * The barrier execution mode feature is experimental and it only handles limited scenarios.
   * Please read the linked SPIP and design docs to understand the limitations and future plans.
   * @return an [[RDDBarrier]] instance that provides actions within a barrier stage
   * @see [[org.apache.spark.BarrierTaskContext]]
   * @see <a href="https://issues.apache.org/jira/browse/SPARK-24374">
   *        SPIP: Barrier Execution Mode</a>
   * @see <a href="https://issues.apache.org/jira/browse/SPARK-24582">Design Doc</a>
   */
  @Experimental
  @Since("2.4.0")
  def barrier(): RDDBarrier[T] = withScope(new RDDBarrier[T](this))

  /**
   * Specify a ResourceProfile to use when calculating this RDD. This is only supported on
   * certain cluster managers and currently requires dynamic allocation to be enabled.
   * It will result in new executors with the resources specified being acquired to
   * calculate the RDD.
   *
   * withResources 为 RDD 指定资源配置文件（ResourceProfile）。
   * 可以为特定计算指定不同的 CPU、内存、GPU 等资源
   */
  @Experimental
  @Since("3.1.0")
  def withResources(rp: ResourceProfile): this.type = {
    resourceProfile = Option(rp)
    sc.resourceProfileManager.addResourceProfile(resourceProfile.get)
    this
  }

  /**
   * Get the ResourceProfile specified with this RDD or null if it wasn't specified.
   * @return the user specified ResourceProfile or null (for Java compatibility) if
   *         none was specified
   */
  @Experimental
  @Since("3.1.0")
  def getResourceProfile(): ResourceProfile = resourceProfile.orNull

  // =======================================================================
  // 其他内部方法和字段
  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  // RDD 的存储级别
  private var storageLevel: StorageLevel = StorageLevel.NONE
  // RDD 的资源配置文件
  @transient private var resourceProfile: Option[ResourceProfile] = None

  /** User code that created this RDD (e.g. `textFile`, `parallelize`). */
  /** 记录创建此 RDD 的用户代码位置（如调用 textFile 或 parallelize 的代码行） */
  @transient private[spark] val creationSite = sc.getCallSite()

  /**
   * The scope associated with the operation that created this RDD.
   *
   * This is more flexible than the call site and can be defined hierarchically. For more
   * detail, see the documentation of {{RDDOperationScope}}. This scope is not defined if the
   * user instantiates this RDD himself without using any Spark operations.
   *
   * 创建此 RDD 的操作作用域，用于 Spark UI 显示 RDD 的层次结构
   */
  @transient private[spark] val scope: Option[RDDOperationScope] = {
    Option(sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)).map(RDDOperationScope.fromJson)
  }

  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]

  // checkpoint 数据
  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  // Whether to checkpoint all ancestor RDDs that are marked for checkpointing. By default,
  // we stop as soon as we find the first such RDD, an optimization that allows us to write
  // less data but is not safe for all workloads. E.g. in streaming we may checkpoint both
  // an RDD and its parent in every batch, in which case the parent may never be checkpointed
  // and its lineage never truncated, leading to OOMs in the long run (SPARK-6847).
  // 是否 checkpoint 所有标记的祖先 RDD（针对 Streaming 等场景的优化配置）
  private val checkpointAllMarkedAncestors =
    Option(sc.getLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS)).exists(_.toBoolean)

  /** Returns the first parent RDD */
  /** 返回第一个父 RDD */
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] */
  /** 返回第 j 个父 RDD */
  protected[spark] def parent[U: ClassTag](j: Int): RDD[U] = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context: SparkContext = sc

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  // 防止重复执行 checkpoint
  @transient private var doCheckpointCalled = false

  /**
   * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
   * has completed (therefore the RDD has been materialized and potentially stored in memory).
   * doCheckpoint() is called recursively on the parent RDDs.
   *
   * 执行 checkpoint 操作，在第一个 action 完成后调用。
   * 会递归调用父 RDD 的 doCheckpoint
   */
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        if (checkpointData.isDefined) {
          if (checkpointAllMarkedAncestors) {
            // TODO We can collect all the RDDs that needs to be checkpointed, and then checkpoint
            // them in parallel.
            // Checkpoint parents first because our lineage will be truncated after we
            // checkpoint ourselves
            // 先 checkpoint 父 RDD，因为我们 checkpoint 后血缘会被截断
            dependencies.foreach(_.rdd.doCheckpoint())
          }
          checkpointData.get.checkpoint()
        } else {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }

  /**
   * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
   * created from the checkpoint file, and forget its old dependencies and partitions.
   *
   * checkpoint 完成后调用，将依赖切换为 CheckpointRDD，清除原有依赖和分区信息
   */
  private[spark] def markCheckpointed(): Unit = stateLock.synchronized {
    // 保留旧依赖的弱引用，用于用户控制的清理
    legacyDependencies = new WeakReference(dependencies_)
    clearDependencies()
    partitions_ = null
    deps = null    // Forget the constructor argument for dependencies too
  }

  /**
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs are removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   *
   * 清除依赖关系，使父 RDD 可以被垃圾回收
   */
  protected def clearDependencies(): Unit = stateLock.synchronized {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    def debugSelf(rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
      val storageInfo = rdd.context.getRDDStorageInfo(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize), bytesToString(info.diskSize)))
      (s"$rdd [$persistence]" +: storageInfo).toImmutableArraySeq
    }

    // Apply a different rule to the last child
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_, _, _]], true)

          frontDepStrings ++ lastDepStrings
      }
    }
    // The first RDD in the dependency stack has no parents, so no need for a +-
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = " ".repeat(leftOffset) + "|" + " ".repeat(partitionStr.length - leftOffset)

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix $desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
        + (if (isLastChild) "  " else "| ")
        + (" ".repeat(leftOffset)) + "|" + (" ".repeat(partitionStr.length - leftOffset)))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$thisPrefix+-$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix$desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(
        rdd: RDD[_],
        prefix: String = "",
        isShuffle: Boolean = true,
        isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      } else {
        debugSelf(rdd).map(prefix + _) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }

  /**
   * Whether the RDD is in a barrier stage. Spark must launch all the tasks at the same time for a
   * barrier stage.
   *
   * An RDD is in a barrier stage, if at least one of its parent RDD(s), or itself, are mapped from
   * an [[RDDBarrier]]. This function always returns false for a [[ShuffledRDD]], since a
   * [[ShuffledRDD]] indicates start of a new stage.
   *
   * A [[MapPartitionsRDD]] can be transformed from an [[RDDBarrier]], under that case the
   * [[MapPartitionsRDD]] shall be marked as barrier.
   */
  private[spark] def isBarrier(): Boolean = isBarrier_

  // From performance concern, cache the value to avoid repeatedly compute `isBarrier()` on a long
  // RDD chain.
  @transient protected lazy val isBarrier_ : Boolean =
    dependencies.filter(!_.isInstanceOf[ShuffleDependency[_, _, _]]).exists(_.rdd.isBarrier())

  private final lazy val _outputDeterministicLevel: DeterministicLevel.Value =
    getOutputDeterministicLevel

  /**
   * Returns the deterministic level of this RDD's output. Please refer to [[DeterministicLevel]]
   * for the definition.
   *
   * By default, an reliably checkpointed RDD, or RDD without parents(root RDD) is DETERMINATE. For
   * RDDs with parents, we will generate a deterministic level candidate per parent according to
   * the dependency. The deterministic level of the current RDD is the deterministic level
   * candidate that is deterministic least. Please override [[getOutputDeterministicLevel]] to
   * provide custom logic of calculating output deterministic level.
   */
  // TODO(SPARK-34612): make it public so users can set deterministic level to their custom RDDs.
  // TODO: this can be per-partition. e.g. UnionRDD can have different deterministic level for
  // different partitions.
  private[spark] final def outputDeterministicLevel: DeterministicLevel.Value = {
    if (isReliablyCheckpointed) {
      DeterministicLevel.DETERMINATE
    } else {
      _outputDeterministicLevel
    }
  }

  @DeveloperApi
  protected def getOutputDeterministicLevel: DeterministicLevel.Value = {
    val deterministicLevelCandidates = dependencies.map {
      // The shuffle is not really happening, treat it like narrow dependency and assume the output
      // deterministic level of current RDD is same as parent.
      case dep: ShuffleDependency[_, _, _] if dep.rdd.partitioner.exists(_ == dep.partitioner) =>
        dep.rdd.outputDeterministicLevel

      case dep: ShuffleDependency[_, _, _] =>
        if (dep.rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE) {
          // If map output was indeterminate, shuffle output will be indeterminate as well
          DeterministicLevel.INDETERMINATE
        } else if (dep.keyOrdering.isDefined && dep.aggregator.isDefined) {
          // if aggregator specified (and so unique keys) and key ordering specified - then
          // consistent ordering.
          DeterministicLevel.DETERMINATE
        } else {
          // In Spark, the reducer fetches multiple remote shuffle blocks at the same time, and
          // the arrival order of these shuffle blocks are totally random. Even if the parent map
          // RDD is DETERMINATE, the reduce RDD is always UNORDERED.
          DeterministicLevel.UNORDERED
        }

      // For narrow dependency, assume the output deterministic level of current RDD is same as
      // parent.
      case dep => dep.rdd.outputDeterministicLevel
    }

    if (deterministicLevelCandidates.isEmpty) {
      // By default we assume the root RDD is determinate.
      DeterministicLevel.DETERMINATE
    } else {
      deterministicLevelCandidates.maxBy(_.id)
    }
  }

}


/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 *
 * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
 * key-value-pair RDDs, and enabling extra functionalities such as `PairRDDFunctions.reduceByKey`.
 *
 * RDD 伴生对象，定义了隐式转换函数，为特定类型的 RDD 提供额外功能。
 *
 * 隐式转换机制：
 *   - RDD[(K, V)] 自动获得 PairRDDFunctions 的方法（如 reduceByKey、groupByKey）
 *   - RDD[Double] 自动获得 DoubleRDDFunctions 的方法（如 mean、sum）
 *   - RDD[(K, V)] 自动获得 OrderedRDDFunctions 的方法（如 sortByKey）
 *   - RDD[(K, V)] 自动获得 SequenceFileRDDFunctions 的方法（如 saveAsSequenceFile）
 */
object RDD {

  private[spark] val CHECKPOINT_ALL_MARKED_ANCESTORS =
    "spark.checkpoint.checkpointAllMarkedAncestors"

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  /**
   * 将 RDD[(K, V)] 隐式转换为 PairRDDFunctions，提供 reduceByKey、groupByKey、join 等 K-V 操作
   */
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  /**
   * 将 RDD[T] 隐式转换为 AsyncRDDActions，提供异步版本的 action（如 collectAsync、countAsync）
   */
  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }

  /**
   * 将 RDD[(K, V)] 隐式转换为 SequenceFileRDDFunctions，提供保存为 SequenceFile 的功能
   */
  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V],
                keyWritableFactory: WritableFactory[K],
                valueWritableFactory: WritableFactory[V])
    : SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  /**
   * 将 RDD[(K, V)] 隐式转换为 OrderedRDDFunctions，提供 sortByKey、repartitionAndSortWithinPartitions 等排序操作
   */
  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
    : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  /**
   * 将 RDD[Double] 隐式转换为 DoubleRDDFunctions，提供 mean、sum、variance 等数值统计操作
   */
  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  /**
   * 将数值类型 RDD 隐式转换为 DoubleRDDFunctions
   */
  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
    : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }
}

/**
 * The deterministic level of RDD's output (i.e. what `RDD#compute` returns). This explains how
 * the output will diff when Spark reruns the tasks for the RDD. There are 3 deterministic levels:
 * 1. DETERMINATE: The RDD output is always the same data set in the same order after a rerun.
 * 2. UNORDERED: The RDD output is always the same data set but the order can be different
 *               after a rerun.
 * 3. INDETERMINATE. The RDD output can be different after a rerun.
 *
 * RDD 输出的确定性级别，描述了重新执行任务时输出会如何变化。
 *
 * 三个级别：
 *   1. DETERMINATE（确定性）：重新运行后输出完全相同（数据和顺序都一样）
 *   2. UNORDERED（无序）：重新运行后数据相同但顺序可能不同
 *   3. INDETERMINATE（不确定）：重新运行后输出可能完全不同
 *
 * 这个属性对 Spark 的容错很重要：
 *   - DETERMINATE：失败重试不会影响最终结果
 *   - INDETERMINATE：失败重试可能导致数据不一致（如非确定性 UDF）
 *
 * Note that, the output of an RDD usually relies on the parent RDDs. When the parent RDD's output
 * is INDETERMINATE, it's very likely the RDD's output is also INDETERMINATE.
 */
@DeveloperApi
object DeterministicLevel extends Enumeration {
  val DETERMINATE, UNORDERED, INDETERMINATE = Value
}

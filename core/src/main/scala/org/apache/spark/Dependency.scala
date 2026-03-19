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

import java.util.concurrent.ScheduledFuture

import scala.reflect.ClassTag

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteProcessor}
import org.apache.spark.shuffle.checksum.RowBasedChecksum
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * 依赖关系的基类。在Spark中，RDD之间通过依赖关系形成DAG（有向无环图）。
 * 每个依赖关系记录了子RDD对父RDD的数据依赖。
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  /** 返回该依赖关系所依赖的父RDD */
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * 窄依赖的基类。窄依赖是指子RDD的每个分区只依赖父RDD的少数几个分区。
 * 窄依赖允许在同一个Stage内进行流水线式执行（pipeline execution），无需Shuffle。
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * 根据子RDD的分区ID，获取其所依赖的父RDD分区列表。
   * @param partitionId 子RDD的分区ID
   * @return 子RDD该分区所依赖的父RDD分区ID列表
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}

/** ShuffleDependency的伴生对象，存放空的行级校验和数组常量 */
object ShuffleDependency {
  private[spark] val EMPTY_ROW_BASED_CHECKSUMS: Array[RowBasedChecksum] = Array.empty
}

/**
 * :: DeveloperApi ::
 * 表示对Shuffle阶段输出的依赖（宽依赖）。Shuffle依赖会导致Stage的划分。
 * 注意：RDD字段使用@transient标记，因为在Executor端不需要它。
 *
 * @param _rdd 父RDD
 * @param partitioner 用于Shuffle输出分区的分区器
 * @param serializer 序列化器，未显式设置时使用spark.serializer配置的默认序列化器
 * @param keyOrdering RDD Shuffle的键排序规则
 * @param aggregator RDD Shuffle的map/reduce端聚合器
 * @param mapSideCombine 是否执行map端预聚合（即map-side combine）
 * @param shuffleWriterProcessor 控制ShuffleMapTask中写入行为的处理器
 * @param rowBasedChecksums 每个Shuffle分区的行级校验和
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false,
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor,
    val rowBasedChecksums: Array[RowBasedChecksum] = ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS,
    private val _checksumMismatchFullRetryEnabled: Boolean = false,
    val checksumMismatchQueryLevelRollbackEnabled: Boolean = false)
  extends Dependency[Product2[K, V]] with Logging {

  /** 兼容性构造函数：不含rowBasedChecksums参数的7参数版本 */
  def this(
      rdd: RDD[_ <: Product2[K, V]],
      partitioner: Partitioner,
      serializer: Serializer,
      keyOrdering: Option[Ordering[K]],
      aggregator: Option[Aggregator[K, V, C]],
      mapSideCombine: Boolean,
      shuffleWriterProcessor: ShuffleWriteProcessor) = {
    this(
      rdd,
      partitioner,
      serializer,
      keyOrdering,
      aggregator,
      mapSideCombine,
      shuffleWriterProcessor,
      ShuffleDependency.EMPTY_ROW_BASED_CHECKSUMS
    )
  }

  // 如果启用了map端预聚合，则必须提供聚合器
  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }
  /** 返回类型转换后的父RDD */
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // 注意：combiner的ClassTag可能为null（当使用PairRDDFunctions中的combineByKey而非combineByKeyWithClassTag时）
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  /** 从SparkContext获取全局唯一的shuffleId */
  val shuffleId: Int = _rdd.context.newShuffleId()

  private[this] val numPartitions = rdd.partitions.length

  // 默认情况下，如果启用了push-based shuffle，则允许ShuffleDependency进行shuffle merge
  private[this] var _shuffleMergeAllowed = canShuffleMergeBeEnabled()

  /** 向ShuffleManager注册此shuffle并获取ShuffleHandle */
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, this)

  /** 设置是否允许shuffle merge */
  private[spark] def setShuffleMergeAllowed(shuffleMergeAllowed: Boolean): Unit = {
    _shuffleMergeAllowed = shuffleMergeAllowed
  }

  /** 判断shuffle merge是否实际启用（需要同时允许且有可用的merger节点） */
  def shuffleMergeEnabled : Boolean = shuffleMergeAllowed && mergerLocs.nonEmpty

  /** 判断是否允许进行shuffle merge */
  def shuffleMergeAllowed : Boolean = _shuffleMergeAllowed

  /** 判断是否启用校验和不匹配时的全量重试（仅在push-based shuffle未启用时有效） */
  def checksumMismatchFullRetryEnabled: Boolean =
    _checksumMismatchFullRetryEnabled && !canShuffleMergeBeEnabled()

  /**
   * 存储被选中用于处理该shuffle阶段mapper的shuffle merge请求的外部shuffle服务节点列表
   */
  private[spark] var mergerLocs: Seq[BlockManagerId] = Nil

  /**
   * 记录与此shuffle依赖关联的shuffle map stage的shuffle merge是否已完成
   */
  private[this] var _shuffleMergeFinalized: Boolean = false

  /**
   * shuffleMergeId用于唯一标识不确定性stage尝试中的合并过程
   */
  private[this] var _shuffleMergeId: Int = 0

  /** 获取当前的shuffleMergeId */
  def shuffleMergeId: Int = _shuffleMergeId

  /** 设置merger节点列表（必须在shuffleMergeAllowed为true时调用） */
  def setMergerLocs(mergerLocs: Seq[BlockManagerId]): Unit = {
    assert(shuffleMergeAllowed)
    this.mergerLocs = mergerLocs
  }

  /** 获取merger节点列表 */
  def getMergerLocs: Seq[BlockManagerId] = mergerLocs

  /** 标记shuffle merge已完成 */
  private[spark] def markShuffleMergeFinalized(): Unit = {
    _shuffleMergeFinalized = true
  }

  /** 判断shuffle merge是否已被标记为完成 */
  private[spark] def isShuffleMergeFinalizedMarked: Boolean = {
    _shuffleMergeFinalized
  }

  /**
   * 判断shuffle merge是否已最终完成。
   * 如果push-based shuffle已启用，返回实际的finalized标记；
   * 如果未启用push-based shuffle，直接返回true（无需merge）。
   */
  def shuffleMergeFinalized: Boolean = {
    if (shuffleMergeEnabled) {
      isShuffleMergeFinalizedMarked
    } else {
      true
    }
  }

  /** 重置shuffle merge状态，用于stage重试时开始新一轮的merge过程 */
  def newShuffleMergeState(): Unit = {
    _shuffleMergeFinalized = false
    mergerLocs = Nil
    // 递增mergeId以区分不同轮次的merge
    _shuffleMergeId += 1
    finalizeTask = None
    shufflePushCompleted.clear()
  }

  /**
   * 判断push-based shuffle是否可以启用。
   * 条件：配置启用 && 分区数大于0 && 非Barrier stage
   */
  private def canShuffleMergeBeEnabled(): Boolean = {
    val isPushShuffleEnabled = Utils.isPushBasedShuffleEnabled(rdd.sparkContext.conf,
      // 在driver端调用
      isDriver = true)
    if (isPushShuffleEnabled && rdd.isBarrier()) {
      logWarning("Push-based shuffle is currently not supported for barrier stages")
    }
    isPushShuffleEnabled && numPartitions > 0 &&
      // TODO: SPARK-35547: Push-based shuffle目前不支持Barrier stage
      !rdd.isBarrier()
  }

  // 使用RoaringBitmap追踪已完成block push的map任务
  @transient private[this] val shufflePushCompleted = new RoaringBitmap()

  /**
   * 在追踪位图中标记给定的map任务已完成push。
   * 使用位图确保由于推测执行或stage重试而多次启动的同一map任务只被计数一次。
   * @param mapIndex Map任务索引
   * @return 已完成block push的map任务总数
   */
  private[spark] def incPushCompleted(mapIndex: Int): Int = {
    shufflePushCompleted.add(mapIndex)
    shufflePushCompleted.getCardinality
  }

  // 仅由DAGScheduler使用，用于协调shuffle merge的最终化定时任务
  @transient private[this] var finalizeTask: Option[ScheduledFuture[_]] = None

  /** 获取shuffle merge最终化的定时任务 */
  private[spark] def getFinalizeTask: Option[ScheduledFuture[_]] = finalizeTask

  /** 设置shuffle merge最终化的定时任务 */
  private[spark] def setFinalizeTask(task: ScheduledFuture[_]): Unit = {
    finalizeTask = Option(task)
  }

  // 阈值设为10亿，对应128MB的位图。
  // 实际HighlyCompressedMapStatus的大小可能远超128MB，可能导致Driver OOM。
  if (numPartitions.toLong * partitioner.numPartitions.toLong > (1L << 30)) {
    logWarning(
      log"The number of shuffle blocks " +
        log"(${MDC(NUM_PARTITIONS, numPartitions.toLong * partitioner.numPartitions.toLong)})" +
        log" for shuffleId ${MDC(SHUFFLE_ID, shuffleId)} " +
        log"for ${MDC(RDD_DESCRIPTION, _rdd)} " +
        log"with ${MDC(NUM_PARTITIONS2, numPartitions)} partitions" +
        log" is possibly too large, which could cause the driver to crash with an out-of-memory" +
        log" error. Consider decreasing the number of partitions in this shuffle stage."
    )
  }

  // 向ContextCleaner注册此shuffle以便在不再使用时进行清理
  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
  // 向ShuffleDriverComponents注册此shuffleId
  _rdd.sparkContext.shuffleDriverComponents.registerShuffle(shuffleId)
}


/**
 * :: DeveloperApi ::
 * 一对一依赖：子RDD的每个分区恰好依赖父RDD中相同索引的一个分区。
 * 这是最简单的窄依赖形式，常见于map、filter等转换操作。
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  /** 子分区直接依赖父RDD中相同ID的分区 */
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * 范围依赖：子RDD的一段连续分区范围依赖父RDD的一段连续分区范围。
 * 常见于union操作，将多个RDD的分区拼接在一起。
 * @param rdd 父RDD
 * @param inStart 父RDD中范围的起始分区索引
 * @param outStart 子RDD中范围的起始分区索引
 * @param length 范围的长度（分区数）
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  /**
   * 如果子分区ID在[outStart, outStart+length)范围内，
   * 则映射到父RDD中对应偏移的分区；否则返回空列表表示无依赖。
   */
  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}

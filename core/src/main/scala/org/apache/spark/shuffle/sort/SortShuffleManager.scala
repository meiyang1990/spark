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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.OpenHashSet

/**
 * 基于 Sort 的 Shuffle 实现。
 *
 * 在 Sort-based Shuffle 中，输入记录根据目标分区 ID 排序，然后写入单个 Map 输出文件。
 * Reducer 通过获取该文件的连续区域来读取其 Map 输出部分。
 * 当 Map 输出数据过大无法放入内存时，已排序的输出子集可以溢写到磁盘，
 * 这些磁盘文件会被合并以生成最终输出文件。
 *
 * Sort-based Shuffle 有两种不同的写入路径来生成 Map 输出文件：
 *
 *  - 序列化排序（Serialized sorting）：当以下三个条件全部满足时使用：
 *    1. Shuffle 依赖未指定 Map 端聚合
 *    2. Shuffle 序列化器支持序列化值的重定位（目前 KryoSerializer 和 Spark SQL 的自定义序列化器支持）
 *    3. Shuffle 产生的输出分区数小于等于 16777216
 *  - 反序列化排序（Deserialized sorting）：用于处理其他所有情况
 *
 * -----------------------
 * 序列化排序模式
 * -----------------------
 *
 * 在序列化排序模式下，输入记录一旦传递给 Shuffle Writer 就被序列化，并在排序期间以序列化形式缓冲。
 * 此写入路径实现了多项优化：
 *
 *  - 排序操作在序列化的二进制数据上进行，而非 Java 对象，这减少了内存消耗和 GC 开销。
 *    此优化要求记录序列化器具有某些特性，允许序列化记录在不反序列化的情况下重新排序。
 *    参见 SPARK-4550，这是此优化首次提出和实现的地方。
 *
 *  - 使用专门的缓存高效排序器 ([[ShuffleExternalSorter]])，对压缩记录指针和分区 ID 的数组进行排序。
 *    排序数组中每条记录仅使用 8 字节空间，使更多数组内容能放入缓存。
 *
 *  - 溢写合并过程对属于同一分区的序列化记录块进行操作，合并期间无需反序列化记录。
 *
 *  - 当溢写压缩编解码器支持压缩数据的拼接时，溢写合并只需拼接已序列化和压缩的溢写分区
 *    即可生成最终输出分区。这允许使用高效的数据复制方法（如 NIO 的 `transferTo`），
 *    并避免在合并期间分配解压或复制缓冲区。
 *
 * 有关这些优化的更多详情，请参见 SPARK-7081。
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  import SortShuffleManager._

  /**
   * 从 Shuffle ID 到产生这些 Shuffle 输出的 Mapper 任务 ID 的映射。
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  override val shuffleBlockResolver =
    new IndexShuffleBlockResolver(conf, taskIdMapsForShuffle = taskIdMapsForShuffle)

  /**
   * 获取一个 [[ShuffleHandle]] 用于传递给任务。
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // 如果分区数少于 spark.shuffle.sort.bypassMergeThreshold 且不需要 Map 端聚合，
      // 则直接写入 numPartitions 个文件，最后只需拼接它们。
      // 这避免了使用正常代码路径时需要对溢写文件进行两次序列化/反序列化来合并。
      // 缺点是需要同时打开多个文件，因此会有更多内存用于缓冲区。
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // 否则，尝试以序列化形式缓冲 Map 输出，因为这样更高效
      new SerializedShuffleHandle[K, V](
        shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // 否则，以反序列化形式缓冲 Map 输出
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  /**
   * 获取读取指定范围 Reduce 分区（startPartition 到 endPartition-1）的读取器，
   * 从指定范围的 Map 输出（startMapIndex 到 endMapIndex-1）中读取。
   * 如果 endMapIndex=Int.MaxValue，实际值将在 getMapSizesByExecutorId 中替换为总 Map 输出数。
   *
   * 在 Executor 上由 Reduce 任务调用。
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    val (blocksByAddress, canEnableBatchFetch) =
      if (baseShuffleHandle.dependency.isShuffleMergeFinalizedMarked) {
        val res = SparkEnv.get.mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        (res.iter, res.enableBatchFetch)
      } else {
        val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
        (address, true)
      }
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch =
        canEnableBatchFetch && canUseBatchFetch(startPartition, endPartition, context))
  }

  /**
   * 获取指定分区的写入器。
   * 在 Executor 上由 Map 任务调用。
   */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(mapId) }
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          bypassMergeSortHandle,
          mapId,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(other, mapId, context, metrics, shuffleExecutorComponents)
    }
  }

  /** 从 ShuffleManager 中移除 Shuffle 的元数据 */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.synchronized {
        mapTaskIds.iterator.foreach { mapTaskId =>
          shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
        }
      }
    }
    true
  }

  /** 关闭此 ShuffleManager */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * 当以序列化形式缓冲 Map 输出时，SortShuffleManager 支持的最大 Shuffle 输出分区数。
   * 这是一个极端的防御性编程措施，因为单个 Shuffle 产生超过 1600 万个输出分区极其罕见。
   */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * 连续 Shuffle 数据块获取功能的本地属性键。
   */
  val FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY =
    "__fetch_continuous_blocks_in_batch_enabled"

  /**
   * 辅助方法，用于确定 Shuffle Reader 是否应批量获取连续数据块。
   */
  def canUseBatchFetch(startPartition: Int, endPartition: Int, context: TaskContext): Boolean = {
    val fetchMultiPartitions = endPartition - startPartition > 1
    fetchMultiPartitions &&
      context.getLocalProperty(FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY) == "true"
  }

  /**
   * 辅助方法，用于确定 Shuffle 是否应使用优化的序列化 Shuffle 路径，
   * 还是应回退到操作反序列化对象的原始路径。
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.mapSideCombine) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because we need to do " +
        s"map-side aggregation")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }

  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
        .toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }
}

/**
 * [[BaseShuffleHandle]] 的子类，用于标识我们已选择使用序列化 Shuffle。
 */
private[spark] class SerializedShuffleHandle[K, V](
  shuffleId: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}

/**
 * [[BaseShuffleHandle]] 的子类，用于标识我们已选择使用 Bypass Merge Sort Shuffle 路径。
 */
private[spark] class BypassMergeSortShuffleHandle[K, V](
  shuffleId: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}

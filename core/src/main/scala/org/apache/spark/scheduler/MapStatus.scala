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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * [[MapStatus]] 和 [[MergeStatus]] 的公共特质。
 * 允许在 MapOutputTracker 中复用已有代码来处理 MergeStatus。
 */
private[spark] trait ShuffleOutputStatus

/**
 * ShuffleMapTask 返回给调度器的结果。
 * 包含任务 Shuffle 文件存储位置的 BlockManager 地址，以及每个 Reducer 的输出大小，
 * 用于传递给 Reduce 任务。
 */
private[spark] sealed trait MapStatus extends ShuffleOutputStatus {
  /** 任务输出所在的 BlockManager 位置 */
  def location: BlockManagerId

  /** 更新输出位置 */
  def updateLocation(newLoc: BlockManagerId): Unit

  /**
   * Reduce 块的估计大小（字节）。
   *
   * 如果块非空，此方法必须返回非零值。此不变量对正确性是必要的，
   * 因为块获取器允许跳过零大小的块。
   */
  def getSizeForBlock(reduceId: Int): Long

  /**
   * 此 Shuffle Map 任务的唯一ID。
   * 如果启用了 spark.shuffle.useOldFetchProtocol，则使用任务的 partitionId，
   * 否则使用 taskContext.taskAttemptId。
   */
  def mapId: Long

  /**
   * 此 Shuffle Map 任务的校验和值，可用于评估不同 Map 任务重试之间输出数据是否发生变化。
   */
  def checksumValue: Long = 0
}


private[spark] object MapStatus {

  /**
   * 使用 [[HighlyCompressedMapStatus]] 的最小分区数阈值。
   * 这里略显不优雅，因为在测试代码中不能假设 SparkEnv.get 存在。
   */
  private lazy val minPartitionsToUseHighlyCompressMapStatus = Option(SparkEnv.get)
    .map(_.conf.get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS))
    .getOrElse(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.defaultValue.get)

  /**
   * 工厂方法：根据分区数选择合适的 MapStatus 实现。
   * 分区数较多时使用 HighlyCompressedMapStatus 以节省内存，否则使用 CompressedMapStatus。
   */
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long,
      checksumVal: Long = 0): MapStatus = {
    if (uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus) {
      HighlyCompressedMapStatus(loc, uncompressedSizes, mapTaskId, checksumVal)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes, mapTaskId, checksumVal)
    }
  }

  // 压缩的对数底数
  private[this] val LOG_BASE = 1.1

  /**
   * 将字节大小压缩为 8 位，用于高效报告 Map 输出大小。
   * 通过编码 log(1.1) 的值为整数实现，最大可支持 35GB 的大小，误差不超过 10%。
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size.toDouble) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * 解压缩 8 位编码的块大小，是 compressSize 的逆操作。
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * [[MapStatus]] 实现：使用单字节跟踪每个块的大小。
 *
 * @param loc 任务执行所在位置
 * @param compressedSizes 块大小数组，按 reduce 分区ID 索引
 * @param _mapTaskId 任务的唯一ID
 * @param _checksumVal 任务的校验和值
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte],
    private[this] var _mapTaskId: Long,
    private[this] var _checksumVal: Long = 0)
  extends MapStatus with Externalizable {

  // For deserialization only
  protected def this() = this(null, null.asInstanceOf[Array[Byte]], -1, 0)

  def this(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long,
      checksumVal: Long) = {
    this(loc, uncompressedSizes.map(MapStatus.compressSize), mapTaskId, checksumVal)
  }

  override def location: BlockManagerId = loc

  override def updateLocation(newLoc: BlockManagerId): Unit = {
    loc = newLoc
  }

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def mapId: Long = _mapTaskId

  override def checksumValue: Long = _checksumVal

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
    out.writeLong(_mapTaskId)
    out.writeLong(_checksumVal)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
    _mapTaskId = in.readLong()
    _checksumVal = in.readLong()
  }
}

/**
 * [[MapStatus]] 实现：精确存储大块（超过 spark.shuffle.accurateBlockThreshold 阈值）的大小，
 * 同时存储其余非空块的平均大小，并使用位图跟踪哪些块为空。
 * 这种实现在分区数很多时能显著减少内存占用。
 *
 * @param loc 任务执行所在位置
 * @param numNonEmptyBlocks 非空块的数量
 * @param emptyBlocks 跟踪空块的位图
 * @param avgSize 非空且非大块的平均大小
 * @param hugeBlockSizes 大块的压缩大小，按 reduceId 索引
 * @param _mapTaskId 任务的唯一ID
 * @param _checksumVal 任务的校验和值
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long,
    private[this] var hugeBlockSizes: scala.collection.Map[Int, Byte],
    private[this] var _mapTaskId: Long,
    private[this] var _checksumVal: Long = 0)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || hugeBlockSizes.size > 0
    || numNonEmptyBlocks == 0 || _mapTaskId > 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1, null, -1, 0)  // For deserialization only

  override def location: BlockManagerId = loc

  override def updateLocation(newLoc: BlockManagerId): Unit = {
    loc = newLoc
  }

  override def getSizeForBlock(reduceId: Int): Long = {
    assert(hugeBlockSizes != null)
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => avgSize
      }
    }
  }

  override def mapId: Long = _mapTaskId

  override def checksumValue: Long = _checksumVal

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.serialize(out)
    out.writeLong(avgSize)
    out.writeInt(hugeBlockSizes.size)
    hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
    out.writeLong(_mapTaskId)
    out.writeLong(_checksumVal)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    numNonEmptyBlocks = -1 // SPARK-32436 Scala 2.13 doesn't initialize this during deserialization
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.deserialize(in)
    avgSize = in.readLong()
    val count = in.readInt()
    val hugeBlockSizesImpl = mutable.Map.empty[Int, Byte]
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesImpl(block) = size
    }
    hugeBlockSizes = hugeBlockSizesImpl
    _mapTaskId = in.readLong()
    _checksumVal = in.readLong()
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long,
      checksumVal: Long = 0): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var numSmallBlocks: Int = 0
    var totalSmallBlockSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    val accurateBlockSkewedFactor = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR.defaultValue.get)
    val shuffleAccurateBlockThreshold =
      Option(SparkEnv.get)
        .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD))
        .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.defaultValue.get)
    val threshold =
      if (accurateBlockSkewedFactor > 0) {
        val sortedSizes = uncompressedSizes.sorted
        val medianSize: Long = Utils.median(sortedSizes, true)
        val maxAccurateSkewedBlockNumber =
          Math.min(
            Option(SparkEnv.get)
              .map(_.conf.get(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER))
              .getOrElse(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get),
            totalNumBlocks
          )
        val skewSizeThreshold =
          Math.max(
            medianSize * accurateBlockSkewedFactor,
            sortedSizes(totalNumBlocks - maxAccurateSkewedBlockNumber).toDouble
          )
        Math.min(shuffleAccurateBlockThreshold.toDouble, skewSizeThreshold)
      } else {
        // Disable skew detection if accurateBlockSkewedFactor <= 0
        shuffleAccurateBlockThreshold.toDouble
      }

    val hugeBlockSizes = mutable.Map.empty[Int, Byte]
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate.
        if (size < threshold) {
          totalSmallBlockSize += size
          numSmallBlocks += 1
        } else {
          hugeBlockSizes(i) = MapStatus.compressSize(uncompressedSizes(i))
        }
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numSmallBlocks > 0) {
      totalSmallBlockSize / numSmallBlocks
    } else {
      0
    }
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize,
      hugeBlockSizes, mapTaskId, checksumVal)
  }
}

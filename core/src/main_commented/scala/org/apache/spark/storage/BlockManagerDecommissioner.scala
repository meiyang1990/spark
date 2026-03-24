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

package org.apache.spark.storage

import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 文件级注释：块管理器退服迁移管理器，负责在Executor退服过程中迁移本节点存储的所有RDD缓存块和Shuffle数据块到其他存活节点
 * 核心职责：实现退服过程中的数据块迁移重试机制，保证数据不丢失，减少后续任务计算失败概率
 */
/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry migrating all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissioner(
    conf: SparkConf,
    bm: BlockManager) extends Logging {

  // 获取备用存储实现，用于将无法迁移到其他节点的数据块写入备用存储
  private val fallbackStorage = FallbackStorage.getFallbackStorage(conf)
  // 从配置获取单个块允许的最大复制失败次数
  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)
  // 预获取异常类名称，用于异常判断
  private val blockSavedOnDecommissionedBlockManagerException =
    classOf[BlockSavedOnDecommissionedBlockManagerException].getSimpleName
  // 预获取异常类名称，用于异常判断
  private val shuffleManagerNotInitializedException =
    classOf[ShuffleManagerNotInitializedException].getSimpleName

  // Used for tracking if our migrations are complete. Readable for testing
  @volatile private[storage] var lastRDDMigrationTime: Long = 0
  @volatile private[storage] var lastShuffleMigrationTime: Long = 0
  @volatile private[storage] var rddBlocksLeft: Boolean = true
  @volatile private[storage] var shuffleBlocksLeft: Boolean = true

  /**
   * 跑龙套注释：Shuffle块迁移任务，基于生产者消费者模型消费待迁移Shuffle块队列
   * 每个线程对应一个目标节点，将数据迁移到该目标节点，避免单点压力过大，加快迁移速度
   * 与RDD块迁移不同，RDD使用原有块复制优先级机制选择目标节点，Shuffle不做目标节点偏好
   * 使用生产者消费者模型的目的是最大化在Executor退出前完成所有Shuffle块迁移的概率
   */
  private class ShuffleMigrationRunnable(peer: BlockManagerId) extends Runnable {
    @volatile var keepRunning = true

    private def allowRetry(shuffleBlock: ShuffleBlockInfo, failureNum: Int): Boolean = {
      if (failureNum < maxReplicationFailuresForDecommission) {
        logInfo(log"Add ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlock)} back to migration queue for " +
          log" retry (${MDC(FAILURES, failureNum)} / " +
          log"${MDC(MAX_ATTEMPTS, maxReplicationFailuresForDecommission)})")
        // 块需要重试，重新加入队列，不标记为已完成
        shufflesToMigrate.add((shuffleBlock, failureNum))
        true
      } else {
        logWarning(log"Give up migrating ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlock)} " +
          log"since it's been failed for " +
          log"${MDC(MAX_ATTEMPTS, maxReplicationFailuresForDecommission)} times")
        false
      }
    }

    private def nextShuffleBlockToMigrate(): (ShuffleBlockInfo, Int) = {
      while (!Thread.currentThread().isInterrupted) {
        Option(shufflesToMigrate.poll()) match {
          case Some(head) => return head
          // 当前队列无待迁移块，等待1秒后重试，等待生产者添加新块或失败块重新入队
          case None => Thread.sleep(1000)
        }
      }
      throw SparkCoreErrors.interruptedError()
    }

    override def run(): Unit = {
      logInfo(log"Starting shuffle block migration thread for ${MDC(PEER, peer)}")
      // 一旦向该目标节点迁移失败，停止继续向该节点发送更多块
      while (keepRunning) {
        try {
          val (shuffleBlockInfo, retryCount) = nextShuffleBlockToMigrate()
          // 获取当前Shuffle块对应的所有子块
          val blocks = bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
          var needRetry = false
          // 默认重试时失败计数加1，对于瞬时错误（如目标节点退服、ShuffleManager未就绪）重置计数，不对块惩罚
          var newRetryCount = retryCount + 1
          // 只有索引文件和数据文件都存在时才执行迁移
          if (blocks.isEmpty) {
            logInfo(log"Ignore deleted shuffle block ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}")
          } else {
            logInfo(log"Got migration sub-blocks ${MDC(BLOCK_IDS, blocks)}. Trying to migrate " +
              log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)} to ${MDC(PEER, peer)} " +
              log"(${MDC(NUM_RETRY, retryCount)} / " +
              log"${MDC(MAX_ATTEMPTS, maxReplicationFailuresForDecommission)})")
            // 执行块迁移
            try {
              val startTime = System.currentTimeMillis()
              // 如果配置了备用存储且目标是备用存储节点，使用备用存储复制逻辑
              if (fallbackStorage.isDefined && peer == FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
              } else {
                // 逐个将子块上传到目标节点
                blocks.foreach { case (blockId, buffer) =>
                  logDebug(s"Migrating sub-block ${blockId}")
                  bm.blockTransferService.uploadBlockSync(
                    peer.host,
                    peer.port,
                    peer.executorId,
                    blockId,
                    buffer,
                    StorageLevel.DISK_ONLY,
                    null) // class tag, we don't need for shuffle
                  logDebug(s"Migrated sub-block $blockId")
                }
              }
              logInfo(log"Migrated ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)} (" +
                log"size: ${MDC(SIZE, Utils.bytesToString(blocks.map(b => b._2.size()).sum))}) " +
                log"to ${MDC(PEER, peer)} in " +
                log"${MDC(DURATION, System.currentTimeMillis() - startTime)} ms")
            } catch {
              case e @ ( _ : IOException | _ : SparkException) =>
                // 如果块在打开文件句柄前被删除，加载会失败
                // 这种情况通常发生在Shuffle TTL清理器在迁移过程中删除了过期块
                if (bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo).size < blocks.size) {
                  logWarning(log"Skipping block ${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}, " +
                    log"block deleted.")
                } else if (fallbackStorage.isDefined
                    // 确认目标不是备用存储ID，因为之前已经尝试过备用存储，不需要重试
                    && peer != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                  // 回退到备用存储写入
                  fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
                } else if (e.getCause != null && e.getCause.getMessage != null
                  && e.getCause.getMessage
                  .contains(blockSavedOnDecommissionedBlockManagerException)) {
                  // 目标节点已经退服，不增加失败计数，停止向该节点迁移，重新入队
                  keepRunning = false
                  needRetry = true
                  newRetryCount = retryCount
                } else if (e.getCause != null && e.getCause.getMessage != null
                  && e.getCause.getMessage
                  .contains(shuffleManagerNotInitializedException)) {
                  // 目标节点的ShuffleManager还未初始化，重试
                  // 仍然增加失败计数，保证如果目标永久初始化失败能够终止
                  logWarning(log"Target executor's ShuffleManager not initialized for " +
                    log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}. Will retry.")
                  needRetry = true
                } else {
                  // 其他未知错误，停止向该节点迁移，块重新入队
                  logError(log"Error occurred during migrating " +
                    log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}", e)
                  keepRunning = false
                  needRetry = true
                }
              case e: Exception =>
                // 捕获所有非致命异常，停止线程，块重新入队
                logError(log"Error occurred during migrating " +
                  log"${MDC(SHUFFLE_BLOCK_INFO, shuffleBlockInfo)}", e)
                keepRunning = false
                needRetry = true
            }
          }
          // needRetry: 块是否需要重新入队重试
          // keepRunning: 当前迁移线程是否继续向该目标节点迁移
          if (needRetry) {
            if (!allowRetry(shuffleBlockInfo, newRetryCount)) {
              numMigratedShuffles.incrementAndGet()
            }
          } else {
            numMigratedShuffles.incrementAndGet()
          }
          if (!keepRunning) {
            logWarning(log"Stop migrating shuffle blocks to ${MDC(PEER, peer)}")
          }
        } catch {
          case _: InterruptedException =>
            if (keepRunning) {
              logInfo("Stop shuffle block migration unexpectedly.")
            } else {
              logInfo("Stop shuffle block migration.")
            }
            keepRunning = false
          case NonFatal(e) =>
            keepRunning = false
            logError("Error occurred during shuffle blocks migration.", e)
        }
      }
    }
  }

  // 正在迁移或已迁移的Shuffle块集合
  private[storage] val migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  // 已完成迁移的Shuffle块计数，用于判断迁移是否完成，新生成的Shuffle块会更新计数状态
  private[storage] val numMigratedShuffles = new AtomicInteger(0)

  // 待迁移Shuffle块队列，存储块信息和已失败次数，对storage包可见用于测试
  private[storage] val shufflesToMigrate =
    new java.util.concurrent.ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()

  // 标记迁移是否已停止
  @volatile private var stopped = false
  @volatile private[storage] var stoppedRDD =
    !conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)
  @volatile private var stoppedShuffle =
    !conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)

  // 目标节点对应迁移线程的映射
  private val migrationPeers =
    mutable.HashMap[BlockManagerId, ShuffleMigrationRunnable]()

  // RDD块迁移执行器，单线程后台执行，配置关闭RDD迁移则为None
  private val rddBlockMigrationExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-rdd"))
    } else None

  // RDD块迁移循环任务，定期轮询迁移剩余RDD缓存块
  private val rddBlockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      logInfo("Attempting to migrate all RDD blocks")
      while (!stopped && !stoppedRDD) {
        // 检查是否存在可迁移的存活目标节点，没有则停止迁移
        if (!bm.getPeers(false).exists(_ != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)) {
          logWarning("No available peers to receive RDD blocks, stop migration.")
          stoppedRDD = true
        } else {
          try {
            val startTime = System.nanoTime()
            logInfo("Attempting to migrate all cached RDD blocks")
            // 执行一轮RDD块迁移，返回是否还有未迁移块
            rddBlocksLeft = decommissionRddCacheBlocks()
            lastRDDMigrationTime = startTime
            logInfo(log"Finished current round RDD blocks migration, " +
              log"waiting for ${MDC(SLEEP_TIME, sleepInterval)}ms before the next round migration.")
            Thread.sleep(sleepInterval)
          } catch {
            case _: InterruptedException =>
              if (!stopped && !stoppedRDD) {
                logInfo("Stop RDD blocks migration unexpectedly.")
              } else {
                logInfo("Stop RDD blocks migration.")
              }
              stoppedRDD = true
            case NonFatal(e) =>
              logError("Error occurred during RDD blocks migration.", e)
              stoppedRDD = true
          }
        }
      }
    }
  }

  // Shuffle块刷新执行器，单线程后台执行，定期刷新待迁移Shuffle块列表
  private val shuffleBlockMigrationRefreshExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-shuffle"))
    } else None

  // Shuffle块刷新循环任务，定期发现新生成的Shuffle块并添加到迁移队列
  private val shuffleBlockMigrationRefreshRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      logInfo("Attempting to migrate all shuffle blocks")
      while (!stopped && !stoppedShuffle) {
        try {
          val startTime = System.nanoTime()
          // 刷新待迁移Shuffle块列表，返回是否还有未迁移块
          shuffleBlocksLeft = refreshMigratableShuffleBlocks()
          lastShuffleMigrationTime = startTime
          logInfo(log"Finished current round refreshing migratable shuffle blocks, " +
            log"waiting for ${MDC(SLEEP_TIME, sleepInterval)}ms before the " +
            log"next round refreshing.")
          Thread.sleep(sleepInterval)
        } catch {
          case _: InterruptedException if stopped =>
            logInfo("Stop refreshing migratable shuffle blocks.")
          case NonFatal(e) =>
            logError("Error occurred during shuffle blocks migration.", e)
            stoppedShuffle = true
        }
      }
    }
  }

  // Shuffle块迁移线程池，每个目标节点一个线程，配置关闭Shuffle迁移则为None
  private val shuffleMigrationPool =
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonCachedThreadPool("migrate-shuffles",
        conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_THREADS)))
    } else None

  /**
   * 刷新本地存储的待迁移Shuffle块列表，将新发现的块添加到迁移队列，更新迁移线程池
   * 不会删除本地Shuffle文件，只做数据迁移，避免正在进行的读取失败
   * 返回true表示还有未完成迁移的Shuffle块
   */
  private[storage] def refreshMigratableShuffleBlocks(): Boolean = {
    // 更新待迁移Shuffle队列
    logInfo("Start refreshing migratable shuffle blocks")
    // 获取本节点所有存储的Shuffle块
    val localShuffles = bm.migratableResolver.getStoredShuffles().toSet
    // 找出新增的未加入迁移的Shuffle块，按shuffleId和mapId排序
    val newShufflesToMigrate = (localShuffles.diff(migratingShuffles)).toSeq
      .sortBy(b => (b.shuffleId, b.mapId))
    // 将新块加入迁移队列，初始失败次数为0
    shufflesToMigrate.addAll(newShufflesToMigrate.map(x => (x, 0)).asJava)
    migratingShuffles ++= newShufflesToMigrate
    val remainedShuffles = migratingShuffles.size - numMigratedShuffles.get()
    logInfo(log"${MDC(COUNT, newShufflesToMigrate.size)} of " +
      log"${MDC(TOTAL, localShuffles.size)} local shuffles are added. " +
      log"In total, ${MDC(NUM_REMAINED, remainedShuffles)} shuffles are remained.")

    // 更新迁移线程
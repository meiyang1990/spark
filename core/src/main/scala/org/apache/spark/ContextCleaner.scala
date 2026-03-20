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

package org.apache.spark

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{ACCUMULATOR_ID, BROADCAST_ID, LISTENER, RDD_ID, SHUFFLE_ID}
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * Classes that represent cleaning tasks.
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask
private case class CleanSparkListener(listener: SparkListener) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)

/**
 * RDD、Shuffle 和 Broadcast 等资源的异步清理器。
 *
 * 为每个需要关注的 RDD、ShuffleDependency 和 Broadcast 维护一个弱引用，
 * 当关联对象超出应用作用域时在独立的守护线程中执行实际清理。
 */
private[spark] class ContextCleaner(
    sc: SparkContext,
    shuffleDriverComponents: ShuffleDriverComponents) extends Logging {

  /**
   * 缓冲区：确保 CleanupTaskWeakReference 在被引用队列处理之前不会被 GC 回收。
   */
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)

  // GC 弱引用队列，弱引用对象被 GC 后会被加入此队列
  private val referenceQueue = new ReferenceQueue[AnyRef]

  // 清理事件监听器列表
  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()

  // 执行清理任务的守护线程
  private val cleaningThread = new Thread() { override def run(): Unit = keepCleaning() }

  // 定期触发 GC 的调度线程
  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
   * 定期 GC 的间隔时间。
   * 清理器依赖弱引用被 GC 才会触发清理。在大 JVM 且内存压力低的长期运行应用中，
   * GC 可能很少发生，不清理会导致 Executor 磁盘空间耗尽。
   */
  private val periodicGCInterval = sc.conf.get(CLEANER_PERIODIC_GC_INTERVAL)

  /**
   * 清理线程是否阻塞等待清理任务完成（Shuffle 由单独参数控制）。
   * 因 SPARK-3015 默认为 true，作为 BlockManager 端点间高频阻塞 RPC 问题的临时解决方案。
   */
  private val blockOnCleanupTasks = sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING)

  /**
   * 清理线程是否阻塞等待 Shuffle 清理任务完成。
   * 因 SPARK-3139 默认为 false，避免 Shuffle 清理时的超时异常。
   */
  private val blockOnShuffleCleanupTasks =
    sc.conf.get(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE)

  @volatile private var stopped = false

  /** 添加监听器以获取对象清理时的信息 */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** 启动清理器 */
  def start(): Unit = {
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    periodicGCService.scheduleAtFixedRate(() => System.gc(),
      periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
   * 停止清理线程并等待当前任务完成。
   * 使用 synchronized 防止在清理线程处理任务时被中断，
   * 避免误清理不同 SparkContext 创建的同名资源（SPARK-6132）。
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }

  /** 注册 RDD 以便在垃圾回收时清理 */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** 注册 ShuffleDependency 以便在垃圾回收时清理 */
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** 注册 Broadcast 以便在垃圾回收时清理 */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** 注册 RDDCheckpointData 以便在垃圾回收时清理 */
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** 注册 SparkListener 以便在其所有者被垃圾回收时清理 */
  def registerSparkListenerForCleanup(
      listenerOwner: AnyRef,
      listener: SparkListener): Unit = {
    registerForCleanup(listenerOwner, CleanSparkListener(listener))
  }

  /** 注册对象以便清理 */
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** 持续清理 RDD、Shuffle 和 Broadcast 状态 */
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    while (!stopped) {
      try {
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
              case CleanSparkListener(listener) =>
                doCleanSparkListener(listener)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** 执行 RDD 清理 */
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      sc.unpersistRDD(rddId, blocking)
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logDebug("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError(log"Error cleaning RDD ${MDC(RDD_ID, rddId)}", e)
    }
  }

  /** 执行 Shuffle 清理 */
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      if (mapOutputTrackerMaster.containsShuffle(shuffleId)) {
        logDebug("Cleaning shuffle " + shuffleId)
        // Shuffle must be removed before it's unregistered from the output tracker
        // to find blocks served by the shuffle service on deallocated executors
        shuffleDriverComponents.removeShuffle(shuffleId, blocking)
        mapOutputTrackerMaster.unregisterShuffle(shuffleId)
        listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
        logDebug("Cleaned shuffle " + shuffleId)
      } else {
        logDebug("Asked to cleanup non-existent shuffle (maybe it was already removed)")
      }
    } catch {
      case e: Exception => logError(log"Error cleaning shuffle ${MDC(SHUFFLE_ID, shuffleId)}", e)
    }
  }

  // 清理 Broadcast：销毁广播变量并通知监听器
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception =>
        logError(log"Error cleaning broadcast ${MDC(BROADCAST_ID, broadcastId)}", e)
    }
  }

  /** 执行累加器清理 */
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logDebug("Cleaned accumulator " + accId)
    } catch {
      case e: Exception =>
        logError(log"Error cleaning accumulator ${MDC(ACCUMULATOR_ID, accId)}", e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logDebug("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception =>
        logError(log"Error cleaning rdd checkpoint data ${MDC(RDD_ID, rddId)}", e)
    }
  }

  def doCleanSparkListener(listener: SparkListener): Unit = {
    try {
      logDebug(s"Cleaning Spark listener $listener")
      sc.listenerBus.removeListener(listener)
      logDebug(s"Cleaned Spark listener $listener")
    } catch {
      case e: Exception =>
        logError(log"Error cleaning Spark listener ${MDC(LISTENER, listener)}", e)
    }
  }

  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Cleaner 清理任何项目时使用的监听器接口。
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}

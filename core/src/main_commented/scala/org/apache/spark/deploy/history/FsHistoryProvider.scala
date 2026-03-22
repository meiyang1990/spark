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

package org.apache.spark.deploy.history

import java.io.{File, FileNotFoundException, IOException}
import java.lang.{Long => JLong}
import java.util.{Date, NoSuchElementException, ServiceLoader}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, TimeUnit}
import java.util.zip.ZipOutputStream

import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.xml.Node

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, SafeModeAction}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.AccessControlException

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.History._
import org.apache.spark.internal.config.Status._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI
import org.apache.spark.internal.config.UI._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.ReplayListenerBus._
import org.apache.spark.status._
import org.apache.spark.status.KVUtils._
import org.apache.spark.status.api.v1.{ApplicationAttemptInfo, ApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{CallerContext, Clock, SystemClock, ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.SparkStringUtils.stringToSeq
import org.apache.spark.util.kvstore._

/**
 * 文件级别注释：基于文件系统的Spark应用历史提供者，负责从文件系统中读取事件日志，
 * 定期扫描新的已完成应用，解析事件日志生成历史应用列表和UI。
 *
 * 如何检测新增和更新的应用尝试：
 * - 在[[checkForLogs]]中检测新增尝试：扫描日志目录，相比上次扫描文件大小发生变化的条目被视为新增或更新，重新解析后更新应用列表
 * - 同样在[[checkForLogs]]中检测更新尝试：如果尝试的日志文件变大，会替换原有条目
 * 使用文件大小而非修改时间来检测变化，原因是：
 * - 部分文件系统不会在每次刷新数据时更新修改时间，导致变更无法被检测
 * - 修改时间粒度可能较大（2秒以上），快速变更会被遗漏
 * 该机制成立的前提是日志文件只会随事件增加而变大，当前Spark的JSON事件日志格式满足该不变量。
 */
private[history] class FsHistoryProvider(conf: SparkConf, clock: Clock)
  extends ApplicationHistoryProvider with Logging {

  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  import FsHistoryProvider._

  // HDFS安全模式检查间隔（秒）
  private val SAFEMODE_CHECK_INTERVAL_S = conf.get(History.SAFEMODE_CHECK_INTERVAL_S)

  // 事件日志更新检查间隔（秒）
  private val UPDATE_INTERVAL_S = conf.get(History.UPDATE_INTERVAL_S)

  // 过期日志清理检查间隔（秒）
  private val CLEAN_INTERVAL_S = conf.get(History.CLEANER_INTERVAL_S)

  // 事件日志重放线程数
  private val numReplayThreads = conf.get(History.NUM_REPLAY_THREADS)
  // 滚动事件日志压缩线程数
  private val numCompactThreads = conf.get(History.NUM_COMPACT_THREADS)

  // 解析配置中的多个日志目录
  private val logDirs = conf.get(History.HISTORY_LOG_DIR)
    .split(",").map(_.trim).filter(_.nonEmpty).toSeq

  private val historyUiAclsEnable = conf.get(History.HISTORY_SERVER_UI_ACLS_ENABLE)
  private val historyUiAdminAcls = conf.get(History.HISTORY_SERVER_UI_ADMIN_ACLS)
  private val historyUiAdminAclsGroups = conf.get(History.HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS)
  logInfo(log"History server ui acls" +
    log" ${MDC(ACL_ENABLED, if (historyUiAclsEnable) "enabled" else "disabled")}" +
    log"; users with admin permissions:" +
    log" ${MDC(LogKeys.ADMIN_ACLS, historyUiAdminAcls.mkString(","))}" +
    log"; groups with admin permissions:" +
    log" ${MDC(ADMIN_ACL_GROUPS, historyUiAdminAclsGroups.mkString(","))}")

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  // Visible for testing
  private[history] val fs: FileSystem = new Path(logDirs.head).getFileSystem(hadoopConf)

  // 多日志目录支持：每个日志目录对应其FileSystem实例
  // Visible for testing
  private[history] val logDirFs: Map[String, FileSystem] = logDirs.map { dir =>
    dir -> new Path(dir).getFileSystem(hadoopConf)
  }.toMap

  // 日志目录到UI显示名称的映射
  private val logDirToDisplayName: Map[String, String] = {
    val names = conf.get(HISTORY_LOG_DIR_NAMES).map(_.split(",", -1).map(_.trim))
    names match {
      case Some(n) if n.length == logDirs.length =>
        val mapping = logDirs.zip(n).map { case (dir, name) =>
          dir -> (if (name.nonEmpty) name else dir)
        }
        val displayNames = mapping.map(_._2)
        val duplicates = displayNames.groupBy(identity).filter(_._2.size > 1).keys
        require(duplicates.isEmpty,
          s"Duplicate display names found in ${HISTORY_LOG_DIR_NAMES.key}: " +
            s"${duplicates.mkString(", ")}")
        mapping.toMap
      case Some(n) =>
        logWarning(s"${HISTORY_LOG_DIR_NAMES.key} has ${n.length} names but " +
          s"${logDirs.length} directories were specified. Falling back to full paths.")
        logDirs.map(d => d -> d).toMap
      case None =>
        logDirs.map(d => d -> d).toMap
    }
  }

  // 标准化后的日志目录路径，用于高效安全的路径匹配
  private val normalizedLogDirs: Seq[(Path, String, String)] = logDirs.map { dir =>
    val path = new Path(dir)
    val dirFs = logDirFs(dir)
    val normalized = path.makeQualified(dirFs.getUri, dirFs.getWorkingDirectory)
    (normalized, logDirToDisplayName(dir), dir)
  }

  // 检查线程和清理线程使用的单线程 scheduled 线程池，保证线程安全避免并发问题
  private val pool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-history-task-%d")

  // 上次扫描检测到的最新日志修改时间，仅用于日志，实际扫描基于文件大小
  private val lastScanTime = new java.util.concurrent.atomic.AtomicLong(-1)

  // 等待重放的任务计数
  private val pendingReplayTasksCount = new java.util.concurrent.atomic.AtomicInteger(0)

  // 本地存储目录，用于缓存解析后的应用状态
  private val storePath = conf.get(LOCAL_STORE_DIR).map(new File(_))
  // 是否对进行中的应用启用快速解析
  private val fastInProgressParsing = conf.get(FAST_IN_PROGRESS_PARSING)

  // 是否启用混合存储（内存+磁盘）存储应用状态
  private val hybridStoreEnabled = conf.get(History.HYBRID_STORE_ENABLED)
  private val hybridStoreDiskBackend =
    HybridStoreDiskBackend.withName(conf.get(History.HYBRID_STORE_DISK_BACKEND))

  // Visible for testing.
  // 应用列表元数据存储KVStore
  private[history] val listing: KVStore = {
    KVUtils.createKVStore(storePath, live = false, conf)
  }

  // 磁盘存储管理器
  private val diskManager = storePath.map { path =>
    new HistoryServerDiskManager(conf, path, listing, clock)
  }

  // 混合存储内存管理器，仅在启用混合存储时初始化
  private var memoryManager: HistoryServerMemoryManager = null
  if (hybridStoreEnabled) {
    memoryManager = new HistoryServerMemoryManager(conf)
  }

  // 每个日志目录对应一个事件日志压缩器
  private val fileCompactors: Map[String, EventLogFileCompactor] = logDirFs.map {
    case (dir, dirFs) =>
      dir -> new EventLogFileCompactor(conf, hadoopConf, dirFs,
        conf.get(EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN),
        conf.get(EVENT_LOG_COMPACTION_SCORE_THRESHOLD))
  }

  // 记录当前正在处理的日志路径，避免重复处理同一路径
  private val processing = ConcurrentHashMap.newKeySet[String]

  private def isProcessing(path: Path): Boolean = {
    processing.contains(path.getName)
  }

  private def isProcessing(info: LogInfo): Boolean = {
    processing.contains(info.logPath.split("/").last)
  }

  private def processing(path: Path): Unit = {
    processing.add(path.getName)
  }

  private def endProcessing(path: Path): Unit = {
    processing.remove(path.getName)
  }

  // 记录不可访问的日志路径，以及添加时间，用于过期清理
  private val inaccessibleList = new ConcurrentHashMap[String, Long]

  // Visible for testing
  private[history] def isAccessible(path: Path): Boolean = {
    !inaccessibleList.containsKey(path.getName)
  }

  private def markInaccessible(path: Path): Unit = {
    inaccessibleList.put(path.getName, clock.getTimeMillis())
  }

  /**
   * 清理不可访问列表中已过期的条目
   */
  private def clearInaccessibleList(expireTimeInSeconds: Long): Unit = {
    val expiredThreshold = clock.getTimeMillis() - expireTimeInSeconds * 1000
    inaccessibleList.asScala.filterInPlace((_, creationTime) => creationTime >= expiredThreshold)
  }

  // 维护当前已加载的UI实例
  private val activeUIs = new mutable.HashMap[(String, Option[String]), LoadedAppUI]()

  /**
   * 获取周期性执行操作的Runnable，封装异常处理
   */
  private def getRunner(operateFun: () => Unit): Runnable =
    () => Utils.tryOrExit { operateFun() }

  /**
   * 用于获取和解析日志文件的固定大小线程池
   */
  private val replayExecutor: ExecutorService = {
    if (!Utils.isTesting) {
      ThreadUtils.newDaemonBlockingThreadPoolExecutorService(
        numReplayThreads, 1024, "log-replay-executor")
    } else {
      ThreadUtils.sameThreadExecutorService()
    }
  }

  /**
   * 用于压缩日志文件的固定大小线程池
   */
  private val compactExecutor: ExecutorService = {
    if (!Utils.isTesting) {
      ThreadUtils.newDaemonBlockingThreadPoolExecutorService(
        numCompactThreads, 1024, "log-compact-executor")
    } else {
      ThreadUtils.sameThreadExecutorService()
    }
  }

  var initThread: Thread = null

  /**
   * 初始化历史提供者，启动后台扫描线程
   * 如果HDFS处于安全模式，则等待安全模式退出后再启动
   * @return 初始化线程，如果已经直接启动则返回null
   */
  private[history] def initialize(): Thread = {
    if (!isFsInSafeMode()) {
      startPolling()
      null
    } else {
      startSafeModeCheckThread(None)
    }
  }

  /**
   * 启动安全模式检查线程，等待HDFS退出安全模式后再启动扫描
   */
  private[history] def startSafeModeCheckThread(
      errorHandler: Option[Thread.UncaughtExceptionHandler]): Thread = {
    // FS处于安全模式时无法扫描，启动新线程等待退出安全模式，让History Server UI可以正常显示状态
    val initThread = new Thread(() => {
      try {
        while (isFsInSafeMode()) {
          logInfo("HDFS is still in safe mode. Waiting...")
          val deadline = clock.getTimeMillis() +
            TimeUnit.SECONDS.toMillis(SAFEMODE_CHECK_INTERVAL_S)
          clock.waitTillTime(deadline)
        }
        startPolling()
      } catch {
        case _: InterruptedException =>
      }
    })
    initThread.setDaemon(true)
    initThread.setName(s"${getClass().getSimpleName()}-init")
    initThread.setUncaughtExceptionHandler(errorHandler.getOrElse(
      (_: Thread, e: Throwable) => {
        logError("Error initializing FsHistoryProvider.", e)
        System.exit(1)
      }))
    initThread.start()
    initThread
  }

  /**
   * 启动后台轮询任务，包括日志检查、日志清理、驱动日志清理
   */
  private def startPolling(): Unit = {
    diskManager.foreach(_.initialize())
    if (memoryManager != null) {
      memoryManager.initialize()
    }

    // 启动时验证日志目录，只要存在一个有效目录即可继续
    val validDirs = logDirs.filter { dir =>
      val path = new Path(dir)
      val dirFs = logDirFs(dir)
      try {
        if (!dirFs.getFileStatus(path).isDirectory) {
          logWarning(log"Logging directory specified is not a directory: " +
            log"${MDC(HISTORY_DIR, dir)}, skipping.")
          false
        } else {
          true
        }
      } catch {
        case f: FileNotFoundException =>
          var msg = s"Log directory specified does not exist: $dir"
          if (dir == DEFAULT_LOG_DIR) {
            msg += " Did you configure the correct one through spark.history.fs.logDirectory?"
          }
          logWarning(msg)
          false
      }
    }
    require(validDirs.nonEmpty,
      s"None of the specified log directories exist: ${logDirs.mkString(", ")}")

    // 测试模式下禁用后台线程
    if (!conf.contains(IS_TESTING)) {
      // 周期检查日志更新任务
      logDebug(s"Scheduling update thread every $UPDATE_INTERVAL_S seconds")
      pool.scheduleWithFixedDelay(
        getRunner(() => checkForLogs()), 0, UPDATE_INTERVAL_S, TimeUnit.SECONDS)

      if (conf.get(CLEANER_ENABLED)) {
        // 周期清理过期日志任务
        pool.scheduleWithFixedDelay(
          getRunner(() => cleanLogs()), 0, CLEAN_INTERVAL_S, TimeUnit.SECONDS)
      }

      if (conf.contains(DRIVER_LOG_DFS_DIR) && conf.get(DRIVER_LOG_CLEANER_ENABLED)) {
        pool.scheduleWithFixedDelay(getRunner(() => cleanDriverLogs()),
          0,
          conf.get(DRIVER_LOG_CLEANER_INTERVAL),
          TimeUnit.SECONDS)
      }
    } else {
      logDebug("Background update thread disabled for testing")
    }
  }

  override def getListing(): Iterator[ApplicationInfo] = {
    // 按结束时间倒序返回应用列表
    KVUtils.mapToSeq(listing.view(classOf[ApplicationInfoWrapper])
      .index("endTime").reverse())(_.toApplicationInfo()).iterator
  }

  override def getListing(max: Int)(
      predicate: ApplicationInfo => Boolean): Iterator[ApplicationInfo] = {
    // 按结束时间倒序过滤返回应用列表，限制最大返回数量
    KVUtils.mapToSeqWithFilter(
      listing.view(classOf[ApplicationInfoWrapper]).index("endTime").reverse(),
      max)(_.toApplicationInfo())(predicate).iterator
  }

  override def getApplicationInfo(appId: String): Option[ApplicationInfo] = {
    try {
      Some(load(appId).toApplicationInfo())
    } catch {
      case _: NoSuchElementException =>
        None
    }
  }

  override def getEventLogsUnderProcess(): Int = pendingReplayTasksCount.get()

  override def getLastUpdatedTime(): Long = lastScanTime.get()

  /**
   * 获取指定应用尝试的UI实例，从事件日志重建应用状态
   * @param appId 应用ID
   * @param attemptId 尝试ID
   * @return 加载完成的UI实例，如果不存在则返回None
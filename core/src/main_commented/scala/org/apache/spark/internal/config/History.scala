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

package org.apache.spark.internal.config

import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit

/**
 * Spark History Server 配置项定义单例对象，集中管理所有历史服务器相关配置参数
 */
private[spark] object History {

  /** 默认事件日志存储目录 */
  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  /** 事件日志存储目录配置，支持多个目录逗号分隔 */
  val HISTORY_LOG_DIR = ConfigBuilder("spark.history.fs.logDirectory")
    .version("1.1.0")
    .doc("Directory where app logs are stored. Multiple directories can be specified " +
      "as a comma-separated list to monitor event logs from multiple paths.")
    .stringConf
    .checkValue(v => v.split(",").map(_.trim).exists(_.nonEmpty),
      "must specify at least one non-empty directory")
    .createWithDefault(DEFAULT_LOG_DIR)

  /** 日志目录显示名称配置，对应日志目录按位置匹配，不设置则显示完整路径 */
  val HISTORY_LOG_DIR_NAMES = ConfigBuilder("spark.history.fs.logDirectory.names")
    .version("4.2.0")
    .doc("Optional comma-separated list of display names for the log directories specified " +
      "in spark.history.fs.logDirectory. Names correspond to directories by position. " +
      "If not set, the full path is shown in the UI.")
    .stringConf
    .createOptional

  /** HDFS安全模式检查间隔，单位秒 */
  val SAFEMODE_CHECK_INTERVAL_S = ConfigBuilder("spark.history.fs.safemodeCheck.interval")
    .version("1.6.0")
    .doc("Interval between HDFS safemode checks for the event log directory")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("5s")

  /** 从存储重新加载日志数据的间隔，单位秒 */
  val UPDATE_INTERVAL_S = ConfigBuilder("spark.history.fs.update.interval")
    .version("1.4.0")
    .doc("How often(in seconds) to reload log data from storage")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("10s")

  /** 更新事件日志文件的批次大小，控制每次扫描处理的文件数量，避免初始扫描耗时过长 */
  val UPDATE_BATCHSIZE = ConfigBuilder("spark.history.fs.update.batchSize")
    .doc("Specifies the batch size for updating new eventlog files. " +
      "This controls each scan process to be completed within a reasonable time, and such " +
      "prevent the initial scan from running too long and blocking new eventlog files to " +
      "be scanned in time in large environments.")
    .version("3.4.0")
    .intConf
    .checkValue(v => v > 0, "The update batchSize should be a positive integer.")
    .createWithDefault(Int.MaxValue)

  /** 是否开启历史日志定期清理功能 */
  val CLEANER_ENABLED = ConfigBuilder("spark.history.fs.cleaner.enabled")
    .version("1.4.0")
    .doc("Whether the History Server should periodically clean up event logs from storage")
    .booleanConf
    .createWithDefault(false)

  /** 日志清理检查间隔 */
  val CLEANER_INTERVAL_S = ConfigBuilder("spark.history.fs.cleaner.interval")
    .version("1.4.0")
    .doc("When spark.history.fs.cleaner.enabled=true, specifies how often the filesystem " +
      "job history cleaner checks for files to delete.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("1d")

  /** 日志文件最大保留时长，超过该时长将被删除 */
  val MAX_LOG_AGE_S = ConfigBuilder("spark.history.fs.cleaner.maxAge")
    .version("1.4.0")
    .doc("When spark.history.fs.cleaner.enabled=true, history files older than this will be " +
      "deleted when the filesystem history cleaner runs.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("7d")

  /** 事件日志目录允许保留的最大日志文件数量 */
  val MAX_LOG_NUM = ConfigBuilder("spark.history.fs.cleaner.maxNum")
    .doc("The maximum number of log files in the event log directory.")
    .version("3.0.0")
    .intConf
    .createWithDefault(Int.MaxValue)

  /** 应用历史信息本地缓存目录，不设置则全部保留在内存 */
  val LOCAL_STORE_DIR = ConfigBuilder("spark.history.store.path")
    .doc("Local directory where to cache application history information. By default this is " +
      "not set, meaning all history information will be kept in memory.")
    .version("2.3.0")
    .stringConf
    .createOptional

  /** 本地存储序列化器枚举，支持JSON和PROTOBUF两种格式 */
  object LocalStoreSerializer extends Enumeration {
    val JSON, PROTOBUF = Value
  }

  /** 磁盘KV存储序列化器配置，默认使用JSON */
  val LOCAL_STORE_SERIALIZER = ConfigBuilder("spark.history.store.serializer")
    .doc("Serializer for writing/reading in-memory UI objects to/from disk-based KV Store; " +
      "JSON or PROTOBUF. JSON serializer is the only choice before Spark 3.4.0, thus it is the " +
      "default value. PROTOBUF serializer is fast and compact, and it is the default " +
      "serializer for disk-based KV store of live UI.")
    .version("3.4.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(LocalStoreSerializer.values.map(_.toString))
    .createWithDefault(LocalStoreSerializer.JSON.toString)

  /** 本地缓存目录最大磁盘使用量 */
  val MAX_LOCAL_DISK_USAGE = ConfigBuilder("spark.history.store.maxDiskUsage")
    .version("2.3.0")
    .doc("Maximum disk usage for the local directory where the cache application history " +
      "information are stored.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("10g")

  /** History Server UI页面标题 */
  val HISTORY_SERVER_UI_TITLE = ConfigBuilder("spark.history.ui.title")
    .version("4.0.0")
    .doc("Specifies the title of the History Server UI page.")
    .stringConf
    .createWithDefault("History Server")

  /** History Server UI绑定端口 */
  val HISTORY_SERVER_UI_PORT = ConfigBuilder("spark.history.ui.port")
    .doc("Web UI port to bind Spark History Server")
    .version("1.0.0")
    .intConf
    .createWithDefault(18080)

  /** 是否开启进行中日志的优化处理 */
  val FAST_IN_PROGRESS_PARSING =
    ConfigBuilder("spark.history.fs.inProgressOptimization.enabled")
      .doc("Enable optimized handling of in-progress logs. This option may leave finished " +
        "applications that fail to rename their event logs listed as in-progress.")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(true)

  /** 查找结束事件时从日志末尾解析的字节数，用于加速应用列表生成 */
  val END_EVENT_REPARSE_CHUNK_SIZE =
    ConfigBuilder("spark.history.fs.endEventReparseChunkSize")
      .doc("How many bytes to parse at the end of log files looking for the end event. " +
        "This is used to speed up generation of application listings by skipping unnecessary " +
        "parts of event log files. It can be disabled by setting this config to 0.")
      .version("2.4.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1m")

  /** 滚动事件日志保留的非压缩文件最大数量 */
  private[spark] val EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN =
    ConfigBuilder("spark.history.fs.eventLog.rolling.maxFilesToRetain")
      .doc("The maximum number of event log files which will be retained as non-compacted. " +
        "By default, all event log files will be retained. Please set the configuration " +
        s"and ${EVENT_LOG_ROLLING_MAX_FILE_SIZE.key} accordingly if you want to control " +
        "the overall size of event log files.")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "Max event log files to retain should be higher than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  /** 事件日志压缩得分阈值，得分超过阈值才会执行压缩 */
  private[spark] val EVENT_LOG_COMPACTION_SCORE_THRESHOLD =
    ConfigBuilder("spark.history.fs.eventLog.rolling.compaction.score.threshold")
      .doc("The threshold score to determine whether it's good to do the compaction or not. " +
        "The compaction score is calculated in analyzing, and being compared to this value. " +
        "Compaction will proceed only when the score is higher than the threshold value.")
      .version("3.0.0")
      .internal()
      .doubleConf
      .createWithDefault(0.7d)

  /** 是否在列出文件前按需查找滚动事件日志位置 */
  val EVENT_LOG_ROLLING_ON_DEMAND_LOAD_ENABLED =
    ConfigBuilder("spark.history.fs.eventLog.rolling.onDemandLoadEnabled")
      .doc("Whether to look up rolling event log locations on demand manner before listing files.")
      .version("4.1.0")
      .booleanConf
      .createWithDefault(true)

  /** 是否开启驱动程序日志定期清理，默认回退使用事件日志清理配置 */
  val DRIVER_LOG_CLEANER_ENABLED = ConfigBuilder("spark.history.fs.driverlog.cleaner.enabled")
    .version("3.0.0")
    .doc("Specifies whether the History Server should periodically clean up driver logs from " +
      "storage.")
    .fallbackConf(CLEANER_ENABLED)

  /** 驱动程序日志最大保留时长，默认回退使用事件日志最大保留时长配置 */
  val MAX_DRIVER_LOG_AGE_S = ConfigBuilder("spark.history.fs.driverlog.cleaner.maxAge")
    .version("3.0.0")
    .doc(s"When ${DRIVER_LOG_CLEANER_ENABLED.key}=true, driver log files older than this will be " +
      s"deleted when the driver log cleaner runs.")
    .fallbackConf(MAX_LOG_AGE_S)

  /** 驱动程序日志清理检查间隔，默认回退使用事件日志清理间隔配置 */
  val DRIVER_LOG_CLEANER_INTERVAL = ConfigBuilder("spark.history.fs.driverlog.cleaner.interval")
    .version("3.0.0")
    .doc(s" When ${DRIVER_LOG_CLEANER_ENABLED.key}=true, specifies how often the filesystem " +
      s"driver log cleaner checks for files to delete. Files are only deleted if they are older " +
      s"than ${MAX_DRIVER_LOG_AGE_S.key}.")
    .fallbackConf(CLEANER_INTERVAL_S)

  /** 是否开启History Server的ACL访问控制 */
  val HISTORY_SERVER_UI_ACLS_ENABLE = ConfigBuilder("spark.history.ui.acls.enable")
    .version("1.0.1")
    .doc("Specifies whether ACLs should be checked to authorize users viewing the applications " +
      "in the history server. If enabled, access control checks are performed regardless of " +
      "what the individual applications had set for spark.ui.acls.enable. The application owner " +
      "will always have authorization to view their own application and any users specified via " +
      "spark.ui.view.acls and groups specified via spark.ui.view.acls.groups when the " +
      "application was run will also have authorization to view that application. If disabled, " +
      "no access control checks are made for any application UIs available through the history " +
      "server.")
    .booleanConf
    .createWithDefault(false)

  /** 拥有所有应用查看权限的管理员用户列表，逗号分隔 */
  val HISTORY_SERVER_UI_ADMIN_ACLS = ConfigBuilder("spark.history.ui.admin.acls")
    .version("2.1.1")
    .doc("Comma separated list of users that have view access to all the Spark applications in " +
      "history server.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 拥有所有应用查看权限的管理员用户组列表，逗号分隔 */
  val HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS = ConfigBuilder("spark.history.ui.admin.acls.groups")
    .version("2.1.1")
    .doc("Comma separated list of groups that have view access to all the Spark applications " +
      "in history server.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 历史摘要页面显示的最大应用数量 */
  val HISTORY_UI_MAX_APPS = ConfigBuilder("spark.history.ui.maxApplications")
    .version("2.0.1")
    .doc("The number of applications to display on the history summary page. Application UIs " +
      "are still available by accessing their URLs directly even if they are not displayed on " +
      "the history summary page.")
    .intConf
    .createWithDefault(Integer.MAX_VALUE)

  /** 事件日志重放线程数量，默认值为可用处理器数除以4向上取整 */
  val NUM_REPLAY_THREADS = ConfigBuilder("spark.history.fs.numReplayThreads")
    .version("2.0.0")
    .doc("Number of threads that will be used by history server to process event logs.")
    .intConf
    .createWithDefaultFunction(() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  /** 事件日志压缩线程数量，默认值为可用处理器数除以4向上取整 */
  val NUM_COMPACT_THREADS = ConfigBuilder("spark.history.fs.numCompactThreads")
    .version("4.1.0")
    .doc("Number of threads that will be used by history server to compact event logs.")
    .intConf
    .createWithDefaultFunction(() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  /** 缓存中保留的应用UI数据最大数量，超过后淘汰最旧应用 */
  val RETAINED_APPLICATIONS = ConfigBuilder("spark.history.retainedApplications")
    .version("1.0.0")
    .doc("The number of applications to retain UI data for in the cache. If this cap is " +
      "exceeded, then the oldest applications will be removed from the cache. If an application " +
      "is not in the cache, it will have to be loaded from disk if it is accessed from the UI.")
    .intConf
    .checkValue(v => v > 0, "The number of applications to retain should be a positive integer.")
    .createWithDefault(50)

  /** 应用历史后端实现类名称 */
  val PROVIDER = ConfigBuilder("spark.history.provider")
    .version("1.1.0")
    .doc("Name of the class implementing the application history backend.")
    .stringConf
    .createWithDefault("org.apache.spark.deploy.history.FsHistoryProvider")

  /** 是否开启Kerberos认证，访问安全HDFS集群时需要开启 */
  val KERBEROS_ENABLED = ConfigBuilder("spark.history.kerberos.enabled")
    .version("1.0.1")
    .doc("Indicates whether the history server should use kerberos to login. This is required " +
      "if the history server is accessing HDFS files on a secure Hadoop cluster.")
    .booleanConf
    .createWithDefault(false)

  /** Kerberos认证主体名称 */
  val KERBEROS_PRINCIPAL = ConfigBuilder("spark.history.kerberos.principal")
    .version("1.0.1")
    .doc(s"When ${KERBEROS_ENABLED.key}=true, specifies kerberos principal name for " +
      s" the History Server.")
    .stringConf
    .createOptional

  /** Kerberos认证keytab文件路径 */
  val KERBEROS_KEYTAB = ConfigBuilder("spark.history.kerberos.keytab")
    .version("1.0.1")
    .doc(s"When ${KERBEROS_ENABLED.key}=true, specifies location of the kerberos keytab file " +
      s"for the History Server.")
    .stringConf
    .createOptional

  /** 自定义执行器日志URL，用于对接外部日志服务 */
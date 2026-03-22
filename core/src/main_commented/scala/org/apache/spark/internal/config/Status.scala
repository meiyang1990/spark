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

import java.util.concurrent.TimeUnit

/**
 * Spark应用状态监控相关配置项定义，主要服务于Spark UI状态展示和指标采集
 */
private[spark] object Status {

  /**
   * 是否启用应用状态存储的异步跟踪更新
   */
  val ASYNC_TRACKING_ENABLED = ConfigBuilder("spark.appStateStore.asyncTracking.enable")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(true)

  /**
   * 活跃UI实体更新的周期时间间隔
   */
  val LIVE_ENTITY_UPDATE_PERIOD = ConfigBuilder("spark.ui.liveUpdate.period")
    .version("2.3.0")
    .timeConf(TimeUnit.NANOSECONDS)
    .createWithDefaultString("100ms")

  /**
   * 刷新过期UI数据的最小时间间隔，避免任务事件稀疏时UI数据 stale
   */
  val LIVE_ENTITY_UPDATE_MIN_FLUSH_PERIOD = ConfigBuilder("spark.ui.liveUpdate.minFlushPeriod")
    .doc("Minimum time elapsed before stale UI data is flushed. This avoids UI staleness when " +
      "incoming task events are not fired frequently.")
    .version("2.4.2")
    .timeConf(TimeUnit.NANOSECONDS)
    .createWithDefaultString("1s")

  /**
   * Spark UI保留的已完成Job的最大数量
   */
  val MAX_RETAINED_JOBS = ConfigBuilder("spark.ui.retainedJobs")
    .version("1.2.0")
    .intConf
    .createWithDefault(1000)

  /**
   * Spark UI保留的已完成Stage的最大数量
   */
  val MAX_RETAINED_STAGES = ConfigBuilder("spark.ui.retainedStages")
    .version("0.9.0")
    .intConf
    .createWithDefault(1000)

  /**
   * 每个Stage在Spark UI保留的已完成Task的最大数量
   */
  val MAX_RETAINED_TASKS_PER_STAGE = ConfigBuilder("spark.ui.retainedTasks")
    .version("2.0.1")
    .intConf
    .createWithDefault(100000)

  /**
   * Spark UI保留的已退出Executor的最大数量
   */
  val MAX_RETAINED_DEAD_EXECUTORS = ConfigBuilder("spark.ui.retainedDeadExecutors")
    .version("2.0.0")
    .intConf
    .createWithDefault(100)

  /**
   * DAG图保留的根RDD节点最大数量
   */
  val MAX_RETAINED_ROOT_NODES = ConfigBuilder("spark.ui.dagGraph.retainedRootRDDs")
    .version("2.1.0")
    .intConf
    .createWithDefault(Int.MaxValue)

  /**
   * 是否启用应用状态指标采集，向Dropwizard/Codahale上报运行应用状态指标
   */
  val METRICS_APP_STATUS_SOURCE_ENABLED =
    ConfigBuilder("spark.metrics.appStatusSource.enabled")
      .doc("Whether Dropwizard/Codahale metrics " +
        "will be reported for the status of the running spark app.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  /**
   * 活跃UI缓存应用信息的本地磁盘目录，不设置则全部保留在内存
   */
  val LIVE_UI_LOCAL_STORE_DIR = ConfigBuilder("spark.ui.store.path")
    .doc("Local directory where to cache application information for live UI. By default this is " +
      "not set, meaning all application information will be kept in memory.")
    .version("3.4.0")
    .stringConf
    .createOptional
}
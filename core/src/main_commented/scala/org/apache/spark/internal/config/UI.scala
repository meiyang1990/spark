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
 * Spark Web UI 相关配置项定义集合，包含UI服务、安全、显示、访问控制等所有UI相关配置
 */
private[spark] object UI {

  /** 是否在控制台显示进度条 */
  val UI_SHOW_CONSOLE_PROGRESS = ConfigBuilder("spark.ui.showConsoleProgress")
    .doc("When true, show the progress bar in the console.")
    .version("1.2.1")
    .booleanConf
    .createWithDefault(false)

  /** 控制台进度条更新时间间隔 */
  val UI_CONSOLE_PROGRESS_UPDATE_INTERVAL =
    ConfigBuilder("spark.ui.consoleProgress.update.interval")
      .version("2.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(200)

  /** 是否启用Spark Web UI服务 */
  val UI_ENABLED = ConfigBuilder("spark.ui.enabled")
    .doc("Whether to run the web UI for the Spark application.")
    .version("1.1.1")
    .booleanConf
    .createWithDefault(true)

  /** Spark Web UI服务监听端口 */
  val UI_PORT = ConfigBuilder("spark.ui.port")
    .doc("Port for your application's dashboard, which shows memory and workload data.")
    .version("0.7.0")
    .intConf
    .createWithDefault(4040)

  /** 应用到Spark Web UI的过滤器类名列表，逗号分隔 */
  val UI_FILTERS = ConfigBuilder("spark.ui.filters")
    .doc("Comma separated list of filter class names to apply to the Spark Web UI.")
    .version("1.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 允许嵌入UI的来源域，用于设置X-Frame-Options */
  val UI_ALLOW_FRAMING_FROM = ConfigBuilder("spark.ui.allowFramingFrom")
    .version("1.6.0")
    .stringConf
    .createOptional

  /** 是否启用Spark Master作为反向代理代理Worker和应用UI */
  val UI_REVERSE_PROXY = ConfigBuilder("spark.ui.reverseProxy")
    .doc("Enable running Spark Master as reverse proxy for worker and application UIs. " +
      "In this mode, Spark master will reverse proxy the worker and application UIs to enable " +
      "access without requiring direct access to their hosts. Use it with caution, as worker " +
      "and application UI will not be accessible directly, you will only be able to access them" +
      "through spark master/proxy public URL. This setting affects all the workers and " +
      "application UIs running in the cluster and must be set on all the workers, drivers " +
      " and masters.")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(false)

  /** 反向代理的访问URL */
  val UI_REVERSE_PROXY_URL = ConfigBuilder("spark.ui.reverseProxyUrl")
    .doc("This is the URL where your proxy is running. This URL is for proxy which is running " +
      "in front of Spark Master. This is useful when running proxy for authentication e.g. " +
      "OAuth proxy. Make sure this is a complete URL including scheme (http/https) and port to " +
      "reach your proxy.")
    .version("2.1.0")
    .stringConf
    .checkValue ({ s =>
      val words = s.split("/")
      !words.contains("proxy") && !words.contains("history") },
      "Cannot use the keyword 'proxy' or 'history' in reverse proxy URL. Spark UI relies on both " +
        "keywords for getting REST API endpoints from URIs.")
    .createOptional

  /** 是否允许从Web UI杀死作业和Stage */
  val UI_KILL_ENABLED = ConfigBuilder("spark.ui.killEnabled")
    .doc("Allows jobs and stages to be killed from the web UI.")
    .version("1.0.0")
    .booleanConf
    .createWithDefault(true)

  /** 是否在Stage和Executor页面显示Executor线程转储链接 */
  val UI_THREAD_DUMPS_ENABLED = ConfigBuilder("spark.ui.threadDumpsEnabled")
    .doc("Whether to show a link for executor thread dumps in Stages and Executor pages.")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(true)

  /** 是否为线程转储渲染火焰图 */
  val UI_FLAMEGRAPH_ENABLED = ConfigBuilder("spark.ui.threadDump.flamegraphEnabled")
    .doc("Whether to render the Flamegraph for executor thread dumps")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(true)

  /** 是否在Executor页面显示堆直方图链接 */
  val UI_HEAP_HISTOGRAM_ENABLED = ConfigBuilder("spark.ui.heapHistogramEnabled")
    .doc("Whether to show a link for executor heap histogram in Executor page.")
    .version("3.5.0")
    .booleanConf
    .createWithDefault(true)

  /** 是否在/metrics/executors/prometheus端点暴露Executor指标（内部配置） */
  val UI_PROMETHEUS_ENABLED = ConfigBuilder("spark.ui.prometheus.enabled")
    .internal()
    .doc("Expose executor metrics at /metrics/executors/prometheus. " +
      "For master/worker/driver metrics, you need to configure `conf/metrics.properties`.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(true)

  /** HTTP响应头X-XSS-Protection的值 */
  val UI_X_XSS_PROTECTION = ConfigBuilder("spark.ui.xXssProtection")
    .doc("Value for HTTP X-XSS-Protection response header")
    .version("2.3.0")
    .stringConf
    .createWithDefaultString("1; mode=block")

  /** 是否启用X-Content-Type-Options响应头设置为nosniff */
  val UI_X_CONTENT_TYPE_OPTIONS = ConfigBuilder("spark.ui.xContentTypeOptions.enabled")
    .doc("Set to 'true' for setting X-Content-Type-Options HTTP response header to 'nosniff'")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(true)

  /** HTTP Strict Transport Security响应头的值 */
  val UI_STRICT_TRANSPORT_SECURITY = ConfigBuilder("spark.ui.strictTransportSecurity")
    .doc("Value for HTTP Strict Transport Security Response Header")
    .version("2.3.0")
    .stringConf
    .createOptional

  /** HTTP请求头最大大小，单位字节 */
  val UI_REQUEST_HEADER_SIZE = ConfigBuilder("spark.ui.requestHeaderSize")
    .doc("Value for HTTP request header size in bytes.")
    .version("2.2.3")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("8k")

  /** 是否在UI页面显示事件时间线 */
  val UI_TIMELINE_ENABLED = ConfigBuilder("spark.ui.timelineEnabled")
    .doc("Whether to display event timeline data on UI pages.")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(true)

  /** 时间线展示的最大任务数 */
  val UI_TIMELINE_TASKS_MAXIMUM = ConfigBuilder("spark.ui.timeline.tasks.maximum")
    .version("1.4.0")
    .intConf
    .createWithDefault(1000)

  /** 时间线展示的最大作业数 */
  val UI_TIMELINE_JOBS_MAXIMUM = ConfigBuilder("spark.ui.timeline.jobs.maximum")
    .version("3.2.0")
    .intConf
    .createWithDefault(500)

  /** 时间线展示的最大Stage数 */
  val UI_TIMELINE_STAGES_MAXIMUM = ConfigBuilder("spark.ui.timeline.stages.maximum")
    .version("3.2.0")
    .intConf
    .createWithDefault(500)

  /** 时间线展示的最大Executor数 */
  val UI_TIMELINE_EXECUTORS_MAXIMUM = ConfigBuilder("spark.ui.timeline.executors.maximum")
    .version("3.2.0")
    .intConf
    .createWithDefault(250)

  /** 是否启用访问控制列表(ACLs) */
  val ACLS_ENABLE = ConfigBuilder("spark.acls.enable")
    .version("1.1.0")
    .booleanConf
    .createWithDefault(false)

  /** 允许查看UI的用户ACL列表 */
  val UI_VIEW_ACLS = ConfigBuilder("spark.ui.view.acls")
    .version("1.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 允许查看UI的用户组ACL列表 */
  val UI_VIEW_ACLS_GROUPS = ConfigBuilder("spark.ui.view.acls.groups")
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 管理员用户ACL列表 */
  val ADMIN_ACLS = ConfigBuilder("spark.admin.acls")
    .version("1.1.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 管理员用户组ACL列表 */
  val ADMIN_ACLS_GROUPS = ConfigBuilder("spark.admin.acls.groups")
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 允许修改的用户ACL列表 */
  val MODIFY_ACLS = ConfigBuilder("spark.modify.acls")
    .version("1.1.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 允许修改的用户组ACL列表 */
  val MODIFY_ACLS_GROUPS = ConfigBuilder("spark.modify.acls.groups")
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /** 用户组映射提供者类名 */
  val USER_GROUPS_MAPPING = ConfigBuilder("spark.user.groups.mapping")
    .version("2.0.0")
    .stringConf
    .createWithDefault("org.apache.spark.security.ShellBasedGroupsMappingProvider")

  /** HTTP重定向使用的代理地址 */
  val PROXY_REDIRECT_URI = ConfigBuilder("spark.ui.proxyRedirectUri")
    .doc("Proxy address to use when responding with HTTP redirects.")
    .version("3.0.0")
    .stringConf
    .createOptional

  /** 自定义Executor日志URL，用于支持外部日志服务 */
  val CUSTOM_EXECUTOR_LOG_URL = ConfigBuilder("spark.ui.custom.executor.log.url")
    .doc("Specifies custom spark executor log url for supporting external log service instead of " +
      "using cluster managers' application log urls in the Spark UI. Spark will support " +
      "some path variables via patterns which can vary on cluster manager. Please check the " +
      "documentation for your cluster manager to see which patterns are supported, if any. " +
      "This configuration replaces original log urls in event log, which will be also effective " +
      "when accessing the application on history server. The new log urls must be permanent, " +
      "otherwise you might have dead link for executor log urls.")
    .version("3.0.0")
    .stringConf
    .createOptional

  /** Master UI停用Worker端点的允许模式（内部配置） */
  val MASTER_UI_DECOMMISSION_ALLOW_MODE = ConfigBuilder("spark.master.ui.decommission.allow.mode")
    .doc("Specifies the behavior of the Master Web UI's /workers/kill endpoint. Possible choices" +
      " are: `LOCAL` means allow this endpoint from IP's that are local to the machine running" +
      " the Master, `DENY` means to completely disable this endpoint, `ALLOW` means to allow" +
      " calling this endpoint from any IP.")
    .internal()
    .version("3.1.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(Set("ALLOW", "LOCAL", "DENY"))
    .createWithDefault("LOCAL")

  /** Master UI页面的自定义标题 */
  val MASTER_UI_TITLE = ConfigBuilder("spark.master.ui.title")
    .version("4.0.0")
    .doc("Specifies the title of the Master UI page. If unset, `Spark Master at <MasterURL>` " +
      "is used by default.")
    .stringConf
    .createOptional

  /** Master UI展示环境变量的前缀列表，逗号分隔 */
  val MASTER_UI_VISIBLE_ENV_VAR_PREFIXES = ConfigBuilder("spark.master.ui.visibleEnvVarPrefixes")
    .doc("Comma-separated list of key-prefix strings to show environment variables")
    .version("4.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Seq.empty[String])

  /** 是否在SQL UI中将同一个根执行下的子执行分组展示 */
  val UI_SQL_GROUP_SUB_EXECUTION_ENABLED = ConfigBuilder("spark.ui.groupSQLSubExecutionEnabled")
    .doc("Whether to group sub executions together in SQL UI when they belong to the same " +
      "root execution")
    .version("3.4.0")
    .booleanConf
    .createWithDefault(true)

  /** UI服务Jetty停止超时时间（内部配置） */
  val UI_JETTY_STOP_TIMEOUT = ConfigBuilder("spark.ui.jettyStopTimeout")
    .internal()
    .doc("Timeout for Jetty servers started in UIs, such as SparkUI, HistoryUI, etc, to stop.")
    .version("4.0.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("30s")

  /** 是否在UI错误页面展示堆栈跟踪 */
  val UI_SHOW_ERROR_STACKS = ConfigBuilder("spark.ui.showErrorStacks")
    .doc("Whether to display stack traces in the UI error pages.")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(true)
}
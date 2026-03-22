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

import org.apache.spark.network.util.ByteUnit

/**
 * PySpark相关配置项定义，存储所有Python Worker和PySpark执行相关的配置参数
 */
private[spark] object Python {
  /** 是否允许复用Python Worker进程 */
  val PYTHON_WORKER_REUSE = ConfigBuilder("spark.python.worker.reuse")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(true)

  /** Python任务杀死操作的超时时间 */
  val PYTHON_TASK_KILL_TIMEOUT = ConfigBuilder("spark.python.task.killTimeout")
    .version("2.2.2")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("2s")

  /** 是否使用守护进程模式启动Python Worker */
  val PYTHON_USE_DAEMON = ConfigBuilder("spark.python.use.daemon")
    .version("2.3.0")
    .booleanConf
    .createWithDefault(true)

  /** 是否开启Python Worker的详细日志输出 */
  val PYTHON_LOG_INFO = ConfigBuilder("spark.executor.python.worker.log.details")
    .version("3.5.0")
    .booleanConf
    .createWithDefault(false)

  /** Python守护进程模块自定义路径，可选配置 */
  val PYTHON_DAEMON_MODULE = ConfigBuilder("spark.python.daemon.module")
    .version("2.4.0")
    .stringConf
    .createOptional

  /** Python Worker模块自定义路径，可选配置 */
  val PYTHON_WORKER_MODULE = ConfigBuilder("spark.python.worker.module")
    .version("2.4.0")
    .stringConf
    .createOptional

  /** PySpark Executor进程使用的内存大小限制，可选配置 */
  val PYSPARK_EXECUTOR_MEMORY = ConfigBuilder("spark.executor.pyspark.memory")
    .version("2.4.0")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  /** Python身份认证socket超时时间，内部配置 */
  val PYTHON_AUTH_SOCKET_TIMEOUT = ConfigBuilder("spark.python.authenticate.socketTimeout")
    .internal()
    .version("3.1.0")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("15s")

  /** 是否开启Python Worker故障处理器，用于捕获进程崩溃时的栈跟踪 */
  val PYTHON_WORKER_FAULTHANLDER_ENABLED = ConfigBuilder("spark.python.worker.faulthandler.enabled")
    .doc("When true, Python workers set up the faulthandler for the case when the Python worker " +
      "exits unexpectedly (crashes), and shows the stack trace of the moment the Python worker " +
      "crashes in the error message if captured successfully.")
    .version("3.2.0")
    .booleanConf
    .createWithDefault(false)

  /** 是否开启Python与JVM通信使用Unix域套接字 */
  val PYTHON_UNIX_DOMAIN_SOCKET_ENABLED = ConfigBuilder("spark.python.unix.domain.socket.enabled")
    .doc("When set to true, the Python driver uses a Unix domain socket for operations like " +
      "creating or collecting a DataFrame from local data, using accumulators, and executing " +
      "Python functions with PySpark such as Python UDFs. This configuration only applies " +
      "to Spark Classic and Spark Connect server.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(sys.env.get("PYSPARK_UDS_MODE").contains("true"))

  /** Unix域套接字文件存储目录，可选配置，内部配置 */
  val PYTHON_UNIX_DOMAIN_SOCKET_DIR = ConfigBuilder("spark.python.unix.domain.socket.dir")
    .doc("When specified, it uses the directory to create Unix domain socket files. " +
      "Otherwise, it uses the default location of the temporary directory set in " +
      s"'java.io.tmpdir' property. This is used when ${PYTHON_UNIX_DOMAIN_SOCKET_ENABLED.key} " +
      "is enabled.")
    .internal()
    .version("4.1.0")
    .stringConf
    // 检查路径长度是否满足Unix域套接字路径限制
    .checkValue(
      _.length <= (104 - (36 + 1 + 5 + 1)),
      s"The directory path should be lower than ${(104 - (36 + 1 + 5 + 1))}")
    .createOptional

  private val PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY = "spark.python.worker.idleTimeoutSeconds"
  private val PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY = "spark.python.worker.killOnIdleTimeout"

  /** Python Worker空闲超时时间，超时后记录日志但不杀死进程 */
  val PYTHON_WORKER_IDLE_TIMEOUT_SECONDS = ConfigBuilder(PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY)
    .doc("The time (in seconds) Spark will wait for activity " +
      "(e.g., data transfer or communication) from a Python worker before considering it " +
      "potentially idle or unresponsive. When the timeout is triggered, " +
      "Spark will log the network-related status for debugging purposes. " +
      "However, the Python worker will remain active and continue waiting for communication " +
      s"unless explicitly terminated via $PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY." +
      "The default is `0` that means no timeout.")
    .version("4.0.0")
    .timeConf(TimeUnit.SECONDS)
    .checkValue(_ >= 0, "The idle timeout should be 0 or positive.")
    .createWithDefault(0)

  /** 是否在Python Worker空闲超时时杀死进程 */
  val PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT = ConfigBuilder(PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT_KEY)
    .doc("Whether Spark should terminate the Python worker process when the idle timeout " +
      s"(as defined by $PYTHON_WORKER_IDLE_TIMEOUT_SECONDS_KEY) is reached. If enabled, " +
      "Spark will terminate the Python worker process in addition to logging the status.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(false)

  /** Python Worker定期转储栈跟踪的时间间隔，0表示禁用 */
  val PYTHON_WORKER_TRACEBACK_DUMP_INTERVAL_SECONDS =
    ConfigBuilder("spark.python.worker.tracebackDumpIntervalSeconds")
      .doc("The interval (in seconds) for Python workers to dump their tracebacks. " +
        "If it's positive, the Python worker will periodically dump the traceback into " +
        "its `stderr`. The default is `0` that means it is disabled.")
      .version("4.1.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ >= 0, "The interval should be 0 or positive.")
      .createWithDefault(0)

  /** 空闲Python Worker池的最大大小，可选配置，不设置则无界 */
  val PYTHON_FACTORY_IDLE_WORKER_MAX_POOL_SIZE =
    ConfigBuilder("spark.python.factory.idleWorkerMaxPoolSize")
      .doc("Maximum number of idle Python workers to keep. " +
        "If unset, the number is unbounded. " +
        "If set to a positive integer N, at most N idle workers are retained; " +
        "least-recently used workers are evicted first.")
      .version("4.1.0")
      .intConf
      .checkValue(_ > 0, "If set, the idle worker max size must be > 0.")
      .createOptional

  /** 是否在输出刷新失败时杀死Python Worker，默认开启便于任务重试 */
  val PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE =
    ConfigBuilder("spark.python.daemon.killWorkerOnFlushFailure")
      .doc("When enabled, exceptions raised during output flush operations in the Python " +
        "worker managed under Python daemon are not caught, causing the worker to terminate " +
        "with the exception. This allows Spark to detect the failure and launch a new worker " +
        "and retry the task. " +
        "When disabled, flush exceptions are caught and logged but the worker continues, " +
        "which could cause the worker to get stuck due to protocol mismatch.")
      .version("4.1.0")
      .booleanConf
      .createWithDefault(true)
}
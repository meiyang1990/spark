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

package org.apache.spark.api.python

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, Channels, SelectionKey, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files => JavaFiles, Path}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys.{COUNT, PYTHON_WORKER_IDLE_TIMEOUT, SIZE, TASK_NAME, TIME, TOTAL_TIME}
import org.apache.spark.internal.config.{BUFFER_SIZE, EXECUTOR_CORES}
import org.apache.spark.internal.config.Python._
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.resource.ResourceProfile.{EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util._


/**
 * 发送给Python工作进程的命令类型枚举
 */
private[spark] object PythonEvalType {
  val NON_UDF = 0

  val SQL_BATCHED_UDF = 100
  val SQL_ARROW_BATCHED_UDF = 101

  val SQL_SCALAR_PANDAS_UDF = 200
  val SQL_GROUPED_MAP_PANDAS_UDF = 201
  val SQL_GROUPED_AGG_PANDAS_UDF = 202
  val SQL_WINDOW_AGG_PANDAS_UDF = 203
  val SQL_SCALAR_PANDAS_ITER_UDF = 204
  val SQL_MAP_PANDAS_ITER_UDF = 205
  val SQL_COGROUPED_MAP_PANDAS_UDF = 206
  val SQL_MAP_ARROW_ITER_UDF = 207
  val SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE = 208
  val SQL_GROUPED_MAP_ARROW_UDF = 209
  val SQL_COGROUPED_MAP_ARROW_UDF = 210
  val SQL_TRANSFORM_WITH_STATE_PANDAS_UDF = 211
  val SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF = 212
  val SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF = 213
  val SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF = 214
  val SQL_GROUPED_MAP_ARROW_ITER_UDF = 215
  val SQL_GROUPED_MAP_PANDAS_ITER_UDF = 216
  val SQL_GROUPED_AGG_PANDAS_ITER_UDF = 217

  // Arrow UDFs
  val SQL_SCALAR_ARROW_UDF = 250
  val SQL_SCALAR_ARROW_ITER_UDF = 251
  val SQL_GROUPED_AGG_ARROW_UDF = 252
  val SQL_WINDOW_AGG_ARROW_UDF = 253
  val SQL_GROUPED_AGG_ARROW_ITER_UDF = 254

  val SQL_TABLE_UDF = 300
  val SQL_ARROW_TABLE_UDF = 301
  val SQL_ARROW_UDTF = 302

  /**
   * 将Python计算类型枚举值转换为字符串名称，用于日志和调试
   * @param pythonEvalType 计算类型整数编码
   * @return 类型名称字符串
   */
  def toString(pythonEvalType: Int): String = pythonEvalType match {
    case NON_UDF => "NON_UDF"
    case SQL_BATCHED_UDF => "SQL_BATCHED_UDF"
    case SQL_ARROW_BATCHED_UDF => "SQL_ARROW_BATCHED_UDF"
    case SQL_SCALAR_PANDAS_UDF => "SQL_SCALAR_PANDAS_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF => "SQL_GROUPED_MAP_PANDAS_UDF"
    case SQL_GROUPED_AGG_PANDAS_UDF => "SQL_GROUPED_AGG_PANDAS_UDF"
    case SQL_WINDOW_AGG_PANDAS_UDF => "SQL_WINDOW_AGG_PANDAS_UDF"
    case SQL_SCALAR_PANDAS_ITER_UDF => "SQL_SCALAR_PANDAS_ITER_UDF"
    case SQL_MAP_PANDAS_ITER_UDF => "SQL_MAP_PANDAS_ITER_UDF"
    case SQL_COGROUPED_MAP_PANDAS_UDF => "SQL_COGROUPED_MAP_PANDAS_UDF"
    case SQL_MAP_ARROW_ITER_UDF => "SQL_MAP_ARROW_ITER_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE => "SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE"
    case SQL_GROUPED_MAP_ARROW_UDF => "SQL_GROUPED_MAP_ARROW_UDF"
    case SQL_COGROUPED_MAP_ARROW_UDF => "SQL_COGROUPED_MAP_ARROW_UDF"
    case SQL_TABLE_UDF => "SQL_TABLE_UDF"
    case SQL_ARROW_TABLE_UDF => "SQL_ARROW_TABLE_UDF"
    case SQL_ARROW_UDTF => "SQL_ARROW_UDTF"
    case SQL_TRANSFORM_WITH_STATE_PANDAS_UDF => "SQL_TRANSFORM_WITH_STATE_PANDAS_UDF"
    case SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF =>
      "SQL_TRANSFORM_WITH_STATE_PANDAS_INIT_STATE_UDF"
    case SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF => "SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_UDF"
    case SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF =>
      "SQL_TRANSFORM_WITH_STATE_PYTHON_ROW_INIT_STATE_UDF"
    case SQL_GROUPED_MAP_ARROW_ITER_UDF => "SQL_GROUPED_MAP_ARROW_ITER_UDF"
    case SQL_GROUPED_MAP_PANDAS_ITER_UDF => "SQL_GROUPED_MAP_PANDAS_ITER_UDF"
    case SQL_GROUPED_AGG_PANDAS_ITER_UDF => "SQL_GROUPED_AGG_PANDAS_ITER_UDF"

    // Arrow UDFs
    case SQL_SCALAR_ARROW_UDF => "SQL_SCALAR_ARROW_UDF"
    case SQL_SCALAR_ARROW_ITER_UDF => "SQL_SCALAR_ARROW_ITER_UDF"
    case SQL_GROUPED_AGG_ARROW_UDF => "SQL_GROUPED_AGG_ARROW_UDF"
    case SQL_WINDOW_AGG_ARROW_UDF => "SQL_WINDOW_AGG_ARROW_UDF"
    case SQL_GROUPED_AGG_ARROW_ITER_UDF => "SQL_GROUPED_AGG_ARROW_ITER_UDF"
  }
}

/**
 * Python运行器的基础工具对象，提供故障日志处理、任务标识等公共能力
 */
private[spark] object BasePythonRunner extends Logging {

  /** Python故障处理器日志目录，用于存储崩溃时的Python栈轨迹
   */
  private[spark] lazy val faultHandlerLogDir = Utils.createTempDir(namePrefix = "faulthandler")

  /**
   * 根据进程ID生成故障日志路径
   * @param pid Python进程ID
   * @return 日志文件路径
   */
  private[spark] def faultHandlerLogPath(pid: Int): Path = {
    new File(faultHandlerLogDir, pid.toString).toPath
  }

  /**
   * 尝试读取Python进程崩溃时留下的故障日志
   * @param faultHandlerEnabled 是否启用故障处理器
   * @param pid Python进程ID
   * @return 读取到的错误日志内容，如果没有则返回None
   */
  private[spark] def tryReadFaultHandlerLog(
      faultHandlerEnabled: Boolean, pid: Option[Int]): Option[String] = {
    if (faultHandlerEnabled) {
      pid.map(faultHandlerLogPath).collect {
        case path if JavaFiles.exists(path) =>
          val error = String.join("\n", JavaFiles.readAllLines(path)) + "\n"
          JavaFiles.deleteIfExists(path)
          error
      }
    } else None
  }

  /**
   * 生成符合Spark规范的任务标识字符串，用于日志输出
   * @param context 任务上下文
   * @return 格式化的任务标识字符串
   */
  private[spark] def taskIdentifier(context: TaskContext): String = {
    s"task ${context.partitionId()}.${context.attemptNumber()} in stage ${context.stageId()} " +
    s"(TID ${context.taskAttemptId()})"
  }

  /**
   * 生成包含Python工作进程状态的结构化日志上下文
   * @param handle Python进程句柄
   * @param worker Python工作进程连接对象
   * @param hasInputs 是否还有待处理输入
   * @return 结构化日志消息上下文
   */
  private[spark] def pythonWorkerStatusMessageWithContext(
      handle: Option[ProcessHandle],
      worker: PythonWorker,
      hasInputs: Boolean): MessageWithContext = {
    log"handle.map(_.isAlive) = " +
    log"${MDC(LogKeys.PYTHON_WORKER_IS_ALIVE, handle.map(_.isAlive))}, " +
    log"channel.isConnected = " +
    log"${MDC(LogKeys.PYTHON_WORKER_CHANNEL_IS_CONNECTED, worker.channel.isConnected)}, " +
    log"channel.isBlocking = " +
    log"${
      MDC(LogKeys.PYTHON_WORKER_CHANNEL_IS_BLOCKING_MODE,
        worker.channel.isBlocking)
    }, " +
      (if (!worker.channel.isBlocking) {
        log"selector.isOpen = " +
        log"${MDC(LogKeys.PYTHON_WORKER_SELECTOR_IS_OPEN, worker.selector.isOpen)}, " +
        log"selectionKey.isValid = " +
        log"${
          MDC(LogKeys.PYTHON_WORKER_SELECTION_KEY_IS_VALID,
            worker.selectionKey.isValid)
        }, " +
        (Try(worker.selectionKey.interestOps()) match {
          case Success(ops) =>
            log"selectionKey.interestOps = " +
              log"${MDC(LogKeys.PYTHON_WORKER_SELECTION_KEY_INTERESTS, ops)}, "
          case _ => log""
        })
      } else log"") +
    log"hasInputs = ${MDC(LogKeys.PYTHON_WORKER_HAS_INPUTS, hasInputs)}"
  }
}

/**
 * 在Spark中运行Python UDF和mapPartition的基类，负责启动Python工作进程并完成数据交互
 *
 * @param funcs 链式Python函数列表，每个元素是一组从底到顶链式调用的函数
 * @param evalType Python计算类型，参见[PythonEvalType]
 * @param argOffsets 每个函数对应的参数偏移量数组
 * @param jobArtifactUUID 作业构件UUID，用于资源定位
 * @param metrics 性能指标累加器映射表
 */
private[spark] abstract class BasePythonRunner[IN, OUT](
    protected val funcs: Seq[ChainedPythonFunctions],
    protected val evalType: Int,
    protected val argOffsets: Array[Array[Int]],
    protected val jobArtifactUUID: Option[String],
    protected val metrics: Map[String, AccumulatorV2[Long, Long]])
  extends Logging {
  import BasePythonRunner._

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  private val conf = SparkEnv.get.conf
  protected val bufferSize: Int = conf.get(BUFFER_SIZE)
  protected val timelyFlushEnabled: Boolean = false
  protected val timelyFlushTimeoutNanos: Long = 0
  protected val authSocketTimeout = conf.get(PYTHON_AUTH_SOCKET_TIMEOUT)
  private val useDaemon = conf.get(PYTHON_USE_DAEMON)
  private val reuseWorker = conf.get(PYTHON_WORKER_REUSE)
  protected val faultHandlerEnabled: Boolean = conf.get(PYTHON_WORKER_FAULTHANLDER_ENABLED)
  protected val idleTimeoutSeconds: Long = conf.get(PYTHON_WORKER_IDLE_TIMEOUT_SECONDS)
  protected val killOnIdleTimeout: Boolean = conf.get(PYTHON_WORKER_KILL_ON_IDLE_TIMEOUT)
  protected val tracebackDumpIntervalSeconds: Long =
    conf.get(PYTHON_WORKER_TRACEBACK_DUMP_INTERVAL_SECONDS)
  protected val killWorkerOnFlushFailure: Boolean =
     conf.get(PYTHON_DAEMON_KILL_WORKER_ON_FLUSH_FAILURE)
  protected val hideTraceback: Boolean = false
  protected val simplifiedTraceback: Boolean = false

  protected def runnerConf: Map[String, String] = Map.empty
  protected def evalConf: Map[String, String] = Map.empty

  // All the Python functions should have the same exec, version and envvars.
  protected val envVars: java.util.Map[String, String] = funcs.head.funcs.head.envVars
  protected val pythonExec: String = funcs.head.funcs.head.pythonExec
  protected val pythonVer: String = funcs.head.funcs.head.pythonVer

  protected val batchSizeForPythonUDF: Int = 100

  // WARN: Both configurations, 'spark.python.daemon.module' and 'spark.python.worker.module' are
  // for very advanced users and they are experimental. This should be considered
  // as expert-only option, and shouldn't be used before knowing what it means exactly.

  // This configuration indicates the module to run the daemon to execute its Python workers.
  private val daemonModule =
    conf.get(PYTHON_DAEMON_MODULE).map { value =>
      logInfo(
        log"Python daemon module in PySpark is set to " +
        log"[${MDC(LogKeys.VALUE, value)}] in '${MDC(LogKeys.CONFIG,
          PYTHON_DAEMON_MODULE.key)}', using this to start the daemon up. Note that this " +
          log"configuration only has an effect when '${MDC(LogKeys.CONFIG2,
            PYTHON_USE_DAEMON.key)}' is enabled and the platform is not Windows.")
      value
    }.getOrElse("pyspark.daemon")

  // This configuration indicates the module to run each Python worker.
  private val workerModule =
    conf.get(PYTHON_WORKER_MODULE).map { value =>
      logInfo(
        log"Python worker module in PySpark is set to ${MDC(LogKeys.VALUE, value)} " +
        log"in ${MDC(LogKeys.CONFIG, PYTHON_WORKER_MODULE.key)}, " +
        log"using this to start the worker up. Note that this configuration only has " +
        log"an effect when ${MDC(LogKeys.CONFIG2, PYTHON_USE_DAEMON.key)} " +
        log"is disabled or the platform is Windows.")
      value
    }.getOrElse("pyspark.worker")

  // TODO: support accumulator in multiple UDF
  protected val accumulator: PythonAccumulator = funcs.head.funcs.head.accumulator

  // Python accumulator is always set in production except in tests. See SPARK-27893
  private val maybeAccumulator: Option[PythonAccumulator] = Option(accumulator)

  // 为屏障阶段任务开放ServerSocket，支持Python侧回调调用BarrierTaskContext方法
  private[spark] var serverSocketChannel: Option[ServerSocketChannel] = None

  // Authentication helper used when serving method calls via socket from Python side.
  private lazy val authHelper = new SocketAuthHelper(conf)

  // each python worker gets an equal part of the allocation. the worker pool will grow to the
  // number of concurrent tasks, which is determined by the number of cores in this executor.
  /**
   * 根据Executor总内存和核心数计算单个Python工作进程可使用的内存
   * @param mem Executor总的PySpark内存配置
   * @param cores Executor核心数
   * @return 单个工作进程分配到的内存大小，如果没有配置则返回None
   */
  private def getWorkerMemoryMb(mem: Option[Long], cores: Int): Option[Long] = {
    mem.map(_ / cores)
  }

  /**
   * 执行Python计算主入口，负责启动Python工作进程，启动输入输出线程，返回结果迭代器
   * @param inputIterator 输入数据迭代器
   * @param partitionIndex 分区索引
   * @param context 任务上下文
   * @return Python计算结果迭代器
   */
  def compute(
      inputIterator: Iterator[IN],
      partitionIndex: Int,
      context: TaskContext): Iterator[OUT] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
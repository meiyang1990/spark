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

package org.apache.spark.api.r

import java.io._
import java.net.{InetAddress, ServerSocket}
import java.util.Arrays

import scala.io.Source
import scala.util.Try

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.R._
import org.apache.spark.util.Utils

/**
 * 文件说明：SparkR R语言用户定义函数执行基类，负责在执行节点启动R进程，通过socket实现JVM与R进程的数据交互，为RDD和DataFrame上执行R UDF提供基础能力
 * 核心职责：封装R进程启动、socket连接管理、数据读写线程的通用逻辑，子类实现具体的读写逻辑处理不同输入输出类型
 * A helper class to run R UDFs in Spark.
 */
private[spark] abstract class BaseRRunner[IN, OUT](
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    numPartitions: Int,
    isDataFrame: Boolean,
    colNames: Array[String],
    mode: Int)
  extends Logging {
  protected var bootTime: Double = _
  protected var dataStream: DataInputStream = _

  /**
   * 计算函数，在分区上启动R进程执行用户定义函数，返回计算结果迭代器
   * @param inputIterator 输入分区数据迭代器
   * @param partitionIndex 分区索引
   * @return R进程输出的计算结果迭代器
   */
  def compute(
      inputIterator: Iterator[IN],
      partitionIndex: Int): Iterator[OUT] = {
    // 记录启动时间
    bootTime = System.currentTimeMillis / 1000.0

    // 创建本地ServerSocket，监听端口随机分配，预留2个连接用于输入输出分离
    val serverSocket = new ServerSocket(0, 2, InetAddress.getByName("localhost"))
    val listenPort = serverSocket.getLocalPort()

    // 启动R工作进程，处理标准输出/错误输出收集
    val errThread = BaseRRunner.createRWorker(listenPort)

    // We use two sockets to separate input and output, then it's easy to manage
    // the lifecycle of them to avoid deadlock.
    // TODO: optimize it to use one socket

    // 设置socket连接超时，防止R进程启动失败导致永久阻塞
    serverSocket.setSoTimeout(10000)
    dataStream = try {
      // 接受输入socket连接，用于JVM向R进程发送输入数据
      val inSocket = serverSocket.accept()
      BaseRRunner.authHelper.authClient(inSocket)
      // 启动写线程，将分区数据发送给R进程
      newWriterThread(inSocket.getOutputStream(), inputIterator, partitionIndex).start()

      // 接受输出socket连接，用于R进程向JVM返回计算结果
      val outSocket = serverSocket.accept()
      BaseRRunner.authHelper.authClient(outSocket)
      val inputStream = new BufferedInputStream(outSocket.getInputStream)
      new DataInputStream(inputStream)
    } finally {
      serverSocket.close()
    }

    // 创建结果迭代器，读取R进程返回的计算结果
    newReaderIterator(dataStream, errThread)
  }

  /**
   * 创建读取R进程输出结果的迭代器，由子类实现具体反序列化逻辑
   * @param dataStream R进程输出数据流
   * @param errThread 错误输出收集线程
   * @return 结果迭代器
   */
  protected def newReaderIterator(
      dataStream: DataInputStream, errThread: BufferedStreamThread): ReaderIterator

  /**
   * 创建写线程，将输入分区数据写入R进程，由子类实现具体序列化逻辑
   * @param output 输出流，指向R进程输入
   * @param iter 输入数据迭代器
   * @param partitionIndex 分区索引
   * @return 写线程对象
   */
  protected def newWriterThread(
      output: OutputStream,
      iter: Iterator[IN],
      partitionIndex: Int): WriterThread

  /**
   * R进程输出结果读取迭代器基类，定义通用的迭代器逻辑，子类实现具体的对象读取方法
   */
  abstract class ReaderIterator(
      stream: DataInputStream,
      errThread: BufferedStreamThread)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    // 流结束标记，为true表示已读取完所有结果
    protected var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
     * 从流中读取下一个结果对象，到达流末尾时返回null，由子类实现具体读取逻辑
     * @return 下一个结果对象，流结束返回null
     */
    protected def read(): OUT

    // 异常处理：捕获R进程异常退出，拼接R进程输出错误信息抛出SparkException
    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception =>
        var msg = "R unexpectedly exited."
        val lines = errThread.getLines()
        if (lines.trim().nonEmpty) {
          msg += s"\nR worker produced errors: $lines\n"
        }
        throw new SparkException(msg, e)
    }
  }

  /**
   * 向R进程写入输入数据的写线程基类，负责发送UDF元数据和输入数据，子类实现具体数据写入逻辑
   */
  abstract class WriterThread(
      output: OutputStream,
      iter: Iterator[IN],
      partitionIndex: Int)
    extends Thread("writer for R") {

    private val env = SparkEnv.get
    private val taskContext = TaskContext.get()
    private val bufferSize = System.getProperty(BUFFER_SIZE.key,
      BUFFER_SIZE.defaultValueString).toInt
    private val stream = new BufferedOutputStream(output, bufferSize)
    protected lazy val dataOut = new DataOutputStream(stream)
    protected lazy val printOut = new PrintStream(stream)

    override def run(): Unit = {
      try {
        // 设置线程上下文环境，绑定Spark环境和任务上下文
        SparkEnv.set(env)
        TaskContext.setTaskContext(taskContext)
        // 向R进程发送分区索引
        dataOut.writeInt(partitionIndex)

        // 发送反序列化器和序列化器名称
        SerDe.writeString(dataOut, deserializer)
        SerDe.writeString(dataOut, serializer)

        // 发送需要加载的R包字节数组
        dataOut.writeInt(packageNames.length)
        dataOut.write(packageNames)

        // 发送序列化后的R函数字节数组
        dataOut.writeInt(func.length)
        dataOut.write(func)

        // 发送广播变量数据，供R进程获取广播值
        dataOut.writeInt(broadcastVars.length)
        broadcastVars.foreach { broadcast =>
          // TODO(shivaram): Read a Long in R to avoid this cast
          dataOut.writeInt(broadcast.id.toInt)
          // TODO: Pass a byte array from R to avoid this cast ?
          val broadcastByteArr = broadcast.value.asInstanceOf[Array[Byte]]
          dataOut.writeInt(broadcastByteArr.length)
          dataOut.write(broadcastByteArr)
        }

        // 发送分区总数和运行模式
        dataOut.writeInt(numPartitions)
        dataOut.writeInt(mode)

        // 如果是DataFrame，发送列名
        if (isDataFrame) {
          SerDe.writeObject(dataOut, colNames, jvmObjectTracker = null)
        }

        // 发送输入数据存在标记，若不存在输入直接结束
        if (!iter.hasNext) {
          dataOut.writeInt(0)
        } else {
          dataOut.writeInt(1)
        }

        // 调用子类方法写入具体输入数据到流
        writeIteratorToStream(dataOut)

        stream.flush()
      } catch {
        // TODO: We should propagate this error to the task thread
        case e: Exception =>
          logError("R Writer thread got an exception", e)
      } finally {
        Try(output.close())
      }
    }

    /**
     * 将输入迭代器数据写入连接R进程的输出流，由子类实现具体序列化逻辑
     * @param dataOut 数据输出流
     */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit
  }
}

/**
 * R进程通信特殊标记常量定义，用于标识计时数据等特殊包
 */
private[spark] object SpecialLengths {
  val TIMING_DATA = -1
}

/**
 * R运行模式常量定义，区分RDD、DataFrame不同apply模式
 */
private[spark] object RRunnerModes {
  val RDD = 0
  val DATAFRAME_DAPPLY = 1
  val DATAFRAME_GAPPLY = 2
}

/**
 * R进程标准输出/错误输出缓冲线程，循环读取流内容并保存最近N行错误信息，用于异常时展示错误上下文
 */
private[spark] class BufferedStreamThread(
    in: InputStream,
    name: String,
    errBufferSize: Int) extends Thread(name) with Logging {
  // 循环缓冲区，保存最近读取的行
  val lines = new Array[String](errBufferSize)
  // 当前缓冲区写索引
  var lineIdx = 0
  override def run(): Unit = {
    for (line <- Source.fromInputStream(in).getLines()) {
      synchronized {
        lines(lineIdx) = line
        lineIdx = (lineIdx + 1) % errBufferSize
      }
      logInfo(line)
    }
  }

  /**
   * 获取收集到的所有非空行，按时间顺序拼接为字符串
   * @return 拼接后的错误输出字符串
   */
  def getLines(): String = synchronized {
    (0 until errBufferSize).filter { x =>
      lines((x + lineIdx) % errBufferSize) != null
    }.map { x =>
      lines((x + lineIdx) % errBufferSize)
    }.mkString("\n")
  }
}

/**
 * BaseRRunner伴生对象，提供R工作进程启动、进程管理、认证等通用工具方法
 */
private[r] object BaseRRunner {
  // Because forking processes from Java is expensive, we prefer to launch
  // a single R daemon (daemon.R) and tell it to fork new workers for our tasks.
  // This daemon currently only works on UNIX-based systems now, so we should
  // also fall back to launching workers (worker.R) directly.
  // 守护进程错误输出收集线程，复用同一个守护进程
  private[this] var errThread: BufferedStreamThread = _
  // 守护进程通信输出流，复用连接
  private[this] var daemonChannel: DataOutputStream = _

  private lazy val authHelper = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new RAuthHelper(conf)
  }

  /**
   * 启动线程读取R进程标准输出，转发到Spark日志并缓冲内容
   * @param proc R进程对象
   * @return 缓冲线程对象
   */
  private def startStdoutThread(proc: Process): BufferedStreamThread = {
    val BUFFER_SIZE = 100
    val thread = new BufferedStreamThread(proc.getInputStream, "stdout reader for R", BUFFER_SIZE)
    thread.setDaemon(true)
    thread.start()
    thread
  }

  /**
   * 根据R版本获取合适的启动参数，R >= 4.2使用--no-restore，否则使用--vanilla保证干净启动环境
   * @param rCommand R可执行命令路径
   * @return R启动参数字符串
   */
  private[r] def getROptions(rCommand: String): String = Try {
    val result = scala.sys.process.Process(Seq(rCommand, "--version")).!!
    "([0-9]+)\\.([0-9]+)\\.([0-9]+)".r.findFirstMatchIn(result).map { m =>
      val major = m.group(1).toInt
      val minor = m.group(2).toInt
      val shouldUseNoRestore = major > 4 || major == 4 && minor >= 2
      if (shouldUseNoRestore) "--no-restore" else "--vanilla"
    }.getOrElse("--vanilla")
  }.getOrElse("--vanilla")

  /**
   * 创建R工作进程，配置环境变量并启动对应脚本
   * @param port JVM监听端口，供R进程连接
   * @param script 要执行的R脚本名称（daemon.R/worker.R）
   * @return 错误输出缓冲线程
   */
  private def createRProcess(port: Int, script: String): BufferedStreamThread = {
    // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
    // but kept here for backward compatibility.
    val sparkConf = SparkEnv.get.conf
    var rCommand = sparkConf.get(SPARKR_COMMAND)
    rCommand = sparkConf.get(R_COMMAND).orElse(Some(rCommand)).get

    val rConnectionTimeout = sparkConf.get(R_BACKEND_CONNECTION_TIMEOUT)
    val rOptions = getROptions(rCommand)
    val rLibDir = RUtils.sparkRPackagePath(isDriver = false)
    val rExecScript = rLibDir(0) + "/SparkR/worker/" + script
    // 构建进程启动命令
    val pb = new ProcessBuilder(Arrays.asList(rCommand, rOptions, rExecScript))
    // 清除R_TESTS环境变量，避免干扰R工作进程加载文件
    pb.environment().put("R_TESTS", "")
    pb.environment().put("SPARKR_RLIBDIR", rLibDir.mkString(","))
    pb.environment().put("SPARKR_WORKER_PORT", port.toString)
    pb.environment().put("SPARKR_BACKEND_CONNECTION_TIMEOUT", rConnectionTimeout.toString)
    pb.environment().put("SPARKR_SPARKFILES_ROOT_DIR", SparkFiles.getRootDirectory())
    pb.environment().put("SPARKR_IS_RUNNING_ON_WORKER", "TRUE")
    pb.environment().put("SPARKR_WORKER_SECRET", authHelper.secret)
    // 合并标准错误到标准输出，统一读取
    pb.redirectErrorStream(true)
    val proc = pb.start()
    val errThread = startStdoutThread(proc)
    errThread
  }

  /**
   * 创建R工作进程，优先复用长期运行的R守护进程减少启动开销，Windows直接启动工作进程
   * @param port 工作进程需要连接的JVM端口
   * @return 错误输出缓冲线程
   */
  def createRWorker(port: Int): BufferedStreamThread = {
    val useDaemon = SparkEnv.get.conf.getBoolean("spark.sparkr.use.daemon", true)
    if (!Utils.isWindows && useDaemon) {
      synchronized {
        // 守护进程未启动则先启动
        if (daemonChannel == null) {
          // we expect one connections
          val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
          val daemonPort = serverSocket.getLocalPort
          errThread = createRProcess(daemonPort, "daemon.R")
          // the socket used to send out the input of task
          serverSocket.setSoTimeout(10000)
          val sock = serverSocket.accept()
          try {
            authHelper.authClient(sock)
            daemonChannel = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
          } finally {
            serverSocket.close()
          }
        }
        try {
          // 向守护进程发送新任务端口，守护进程会派生出新的工作进程连接该端口
          daemonChannel.writeInt(port)
          daemonChannel.flush()
        } catch {
          case e: IOException =>
            // 守护进程异常退出，清理状态后抛出异常，让Spark调度器重试任务
            daemonChannel.close()
            daemonChannel = null
            errThread = null
            throw e
        }
        errThread
      }
    } else {
      // 不使用守护进程，直接启动独立工作进程
      createRProcess(port, "worker.R")
    }
  }
}
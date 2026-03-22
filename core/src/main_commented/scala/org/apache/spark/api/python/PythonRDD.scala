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
import java.nio.channels.{Channels, SocketChannel}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{HOST, PORT, SOCKET_ADDRESS}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.Python.PYTHON_UNIX_DOMAIN_SOCKET_ENABLED
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.security.{SocketAuthHelper, SocketAuthServer, SocketFuncServer}
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._

/**
 * PySpark核心RDD实现，用于包装Python用户函数并在Spark集群执行计算
 * 负责将Python序列化后的函数交给Python工作进程执行，并返回字节数组形式的计算结果
 * 是PySpark连接JVM核心计算引擎和Python用户代码的核心桥梁
 *
 * @param parent 父RDD，提供输入数据
 * @param func 待执行的Python函数（已序列化）
 * @param preservePartitioning 是否保留原RDD的分区信息
 * @param isFromBarrier 是否来自障碍阶段，用于 barrier 执行模式
 */
private[spark] class PythonRDD(
    parent: RDD[_],
    func: PythonFunction,
    preservePartitioning: Boolean,
    isFromBarrier: Boolean = false)
  extends RDD[Array[Byte]](parent) {

  // 获取当前作业的工件UUID，用于作业隔离
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitioning) firstParent.partitioner else None
  }

  // 转换为JavaRDD格式，供Java层API调用
  val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    // 创建Python运行器，执行用户Python函数
    val runner = PythonRunner(func, jobArtifactUUID)
    runner.compute(firstParent.iterator(split, context), split.index, context)
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())
}

/**
 * Python函数的包装接口，包含在Python运行器中执行函数所需的全部上下文信息
 */
private[spark] trait PythonFunction {
  def command: Seq[Byte]
  def envVars: JMap[String, String]
  def pythonIncludes: JList[String]
  def pythonExec: String
  def pythonVer: String
  def broadcastVars: JList[Broadcast[PythonBroadcast]]
  def accumulator: PythonAccumulator
}

private[spark] object PythonFunction {
  // Python累加器类型别名，存储序列化后的字节数组
  type PythonAccumulator = CollectionAccumulator[Array[Byte]]
}

/**
 * 简单Python函数包装类，用于包装PySpark创建的单个Python函数
 */
private[spark] case class SimplePythonFunction(
    command: Seq[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: PythonAccumulator) extends PythonFunction {

  def this(
      command: Array[Byte],
      envVars: JMap[String, String],
      pythonIncludes: JList[String],
      pythonExec: String,
      pythonVer: String,
      broadcastVars: JList[Broadcast[PythonBroadcast]],
      accumulator: PythonAccumulator) = {
    this(command.toImmutableArraySeq,
      envVars, pythonIncludes, pythonExec, pythonVer, broadcastVars, accumulator)
  }
}

/**
 * 链式Python函数包装类，按从底到顶顺序存储多个Python函数用于链式执行
 * @param funcs 待链式执行的Python函数序列
 */
private[spark] case class ChainedPythonFunctions(funcs: Seq[PythonFunction])

/**
 * 用户Python代码执行抛出异常时的包装异常类，用于在JVM层传递Python异常信息
 */
private[spark] class PythonException(
    msg: String,
    cause: Throwable,
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext])
  extends RuntimeException(msg, cause) with SparkThrowable {

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      cause: Throwable = null,
      context: Array[QueryContext] = Array.empty,
      summary: String = "") = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      cause,
      Option(errorClass),
      messageParameters,
      context
    )
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

/**
 * 将Python shuffle返回的键值对重新组织为RDD，供后续 shuffle 阶段处理
 * 用于PySpark的shuffle操作，将Python返回的连续字节数组切分为键值对
 */
private class PairwiseRDD(prev: RDD[Array[Byte]]) extends RDD[(Long, Array[Byte])](prev) {
  override def getPartitions: Array[Partition] = prev.partitions
  override val partitioner: Option[Partitioner] = prev.partitioner
  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Array[Byte])] =
    // 每两个元素一组，第一个是分区ID（Long序列化后），第二个是实际数据
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => (Utils.deserializeLongValue(a), b)
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  // 转换为JavaPairRDD供Java层调用
  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

/**
 * PythonRDD伴生对象，提供Python RDD相关的工具方法，供PySpark调用
 * 包含RDD创建、Hadoop输入输出、数据序列化、Socket数据传输等能力
 */
private[spark] object PythonRDD extends Logging {

  // 记录每个工作进程已发送的广播变量ID，避免重复发送
  private val workerBroadcasts = new mutable.WeakHashMap[PythonWorker, mutable.Set[Long]]()

  // 认证工具类，用于服务端Socket连接认证
  private lazy val authHelper = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SocketAuthHelper(conf)
  }

  /**
   * 获取指定Python工作进程的已广播变量集合，用于管理广播变量生命周期
   * @param worker Python工作进程
   * @return 已广播变量ID集合
   */
  def getWorkerBroadcasts(worker: PythonWorker): mutable.Set[Long] = {
    synchronized {
      workerBroadcasts.getOrElseUpdate(worker, new mutable.HashSet[Long]())
    }
  }

  /**
   * 从键值对RDD中提取值部分，保留原分区信息
   * 用于PySpark在partitionBy后保持分区器不变
   * @param pair 输入键值对RDD
   * @return 仅包含值的JavaRDD
   */
  def valueOfPair(pair: JavaPairRDD[Long, Array[Byte]]): JavaRDD[Array[Byte]] = {
    pair.rdd.mapPartitions(it => it.map(_._2), true)
  }

  /**
   * Python端调用Spark runJob的适配器，执行作业并通过Socket返回结果
   * 相当于collect操作，但支持仅执行指定分区子集
   * @param sc Spark上下文
   * @param rdd 待执行RDD
   * @param partitions 待执行的分区索引列表
   * @return 返回包含服务端口、认证密钥、服务对象的三元组，供Python客户端连接获取结果
   */
  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: JArrayList[Int]): Array[Any] = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions.asScala.toSeq)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions.toImmutableArraySeq: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.asScala.mkString(",")}")
  }

  /**
   * 收集RDD所有元素，通过Socket返回给Python客户端
   * @param rdd 待收集RDD
   * @return 返回包含服务端口、认证密钥、服务对象的三元组
   */
  def collectAndServe[T](rdd: RDD[T]): Array[Any] = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  /**
   * 带作业组信息的收集并通过Socket返回方法，支持作业取消
   * @param rdd 待收集RDD
   * @param groupId 作业组ID
   * @param description 作业描述
   * @param interruptOnCancel 取消时是否中断作业
   * @return 返回包含服务端口、认证密钥、服务对象的三元组
   */
  def collectAndServeWithJobGroup[T](
      rdd: RDD[T],
      groupId: String,
      description: String,
      interruptOnCancel: Boolean): Array[Any] = {
    val sc = rdd.sparkContext
    sc.setJobGroup(groupId, description, interruptOnCancel)
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  /**
   * 将RDD转换为本地迭代器并通过Socket分分区返回给Python客户端
   * 支持预取分区，实现Python端的延迟迭代获取
   * @param rdd 源RDD
   * @param prefetchPartitions 是否预取下一个分区
   * @return 返回包含服务端口、认证密钥、服务对象的三元组
   */
  def toLocalIteratorAndServe[T](rdd: RDD[T], prefetchPartitions: Boolean = false): Array[Any] = {
    // 处理连接的回调函数
    val handleFunc = (sock: SocketChannel) => {
      val out = new DataOutputStream(Channels.newOutputStream(sock))
      val in = new DataInputStream(Channels.newInputStream(sock))
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // 创建每个分区的异步收集迭代器
        val collectPartitionIter = rdd.partitions.indices.iterator.map { i =>
          var result: Array[Any] = null
          // 异步提交单个分区收集任务
          rdd.sparkContext.submitJob(
            rdd,
            (iter: Iterator[Any]) => iter.toArray,
            Seq(i),
            (_, res: Array[Any]) => result = res,
            result)
        }
        val prefetchIter = collectPartitionIter.buffered

        var complete = false
        while (!complete) {
          // 读取客户端请求，0表示停止迭代，非0表示请求下一个分区
          if (in.readInt() == 0) {
            complete = true
          } else if (prefetchIter.hasNext) {
            // 获取下一个分区的异步结果
            val partitionFuture = prefetchIter.next()
            // 如果开启预取，提前触发下一个分区的任务提交
            if (prefetchPartitions) {
              prefetchIter.headOption
            }
            // 等待分区收集完成
            val partitionArray = ThreadUtils.awaitResult(partitionFuture, Duration.Inf)
            // 发送响应：1表示有数据返回
            out.writeInt(1)
            // 将分区数据写入输出流
            writeIteratorToStream(partitionArray.iterator, out)
            out.writeInt(SpecialLengths.END_OF_DATA_SECTION)
            out.flush()
          } else {
            // 发送响应：0表示没有更多分区
            out.writeInt(0)
            complete = true
          }
        }
      })(catchBlock = {
        // 发生错误，发送-1标识
        out.writeInt(-1)
      }, finallyBlock = {
        out.close()
        in.close()
      })
    }

    // 创建Socket服务端并启动
    val server = new SocketFuncServer(authHelper, "serve toLocalIterator", handleFunc)
    Array(server.connInfo, server.secret, server)
  }

  /**
   * 从文件读取RDD数据，反序列化为Python RDD
   */
  def readRDDFromFile(
      sc: JavaSparkContext,
      filename: String,
      parallelism: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromFile(sc, filename, parallelism)
  }

  /**
   * 从输入流读取RDD数据，反序列化为Python RDD
   */
  def readRDDFromInputStream(
      sc: SparkContext,
      in: InputStream,
      parallelism: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromInputStream(sc, in, parallelism)
  }

  /**
   * 创建Python广播对象，用于反序列化广播数据
   */
  def setupBroadcast(path: String): PythonBroadcast = {
    new PythonBroadcast(path)
  }

  /**
   * 将迭代器中下一个元素写入输出流，处理不同类型数据的序列化
   * @return 是否成功写入元素（false表示迭代器已耗尽）
   */
  def writeNextElementToStream[T](iter: Iterator[T], dataOut: DataOutputStream): Boolean = {

    def write(obj: Any): Unit = obj match {
      case null =>
        // 空值用特殊长度标识
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        // 字节数组先写长度再写数据
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        // 字符串按UTF-8编码写入
        writeUTF(str, dataOut)
      case stream: PortableDataStream =>
        // 可移植数据流转换为字节数组写入
        write(stream.toArray())
      case (key, value) =>
        // 键值对分别写入
        write(key)
        write(value)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }
    if (iter.hasNext) {
      write(iter.next())
      true
    } else {
      false
    }
  }

  /**
   * 将整个迭代器所有元素写入输出流
   */
  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream): Unit = {
    while (writeNextElementToStream(iter, dataOut)) {
      // Nothing.
    }
  }

  /**
   * 基于Hadoop SequenceFile创建Python RDD
   * @param sc JavaSpark上下文
   * @param path 输入文件路径
   * @param keyClassMaybeNull 键类名
   * @param valueClassMaybeNull 值类名
   * @param keyConverterClass 键转换器类名
   * @param valueConverterClass 值转换器类名
   * @param minSplits 最小分片数
   * @param batchSize 批序列化大小
   * @return
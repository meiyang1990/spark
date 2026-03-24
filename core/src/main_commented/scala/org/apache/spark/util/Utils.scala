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

package org.apache.spark.util

import java.io._
import java.lang.{Byte => JByte}
import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, PlatformManagedObject, ThreadInfo}
import java.lang.reflect.InvocationTargetException
import java.math.{MathContext, RoundingMode}
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.SecureRandom
import java.util.{HexFormat, Locale, Properties, Random, UUID}
import java.util.concurrent._
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.zip.{GZIPInputStream, ZipInputStream}

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.matching.Regex

import _root_.io.netty.channel.unix.Errors.NativeIoException
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Interners
import com.google.common.net.InetAddresses
import jakarta.ws.rs.core.UriBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.fs.audit.CommonAuditContext.currentAuditContext
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.ipc.{CallerContext => HadoopCallerContext}
import org.apache.hadoop.ipc.CallerContext.{Builder => HadoopCallerContextBuilder}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.RunJar
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.LoggerConfig
import org.slf4j.Logger

import org.apache.spark.{SPARK_VERSION, _}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.Executor.TASK_THREAD_NAME_PREFIX
import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Streaming._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.{Utils => CUtils}
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/** CallSite represents a place in user code. It can have a short and a long form. */
/**
 * 表示用户代码中调用Spark API的位置，包含短格式和长格式两种表示
 */
private[spark] case class CallSite(shortForm: String, longForm: String)

private[spark] object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
  val empty = CallSite("", "")
}

/**
 * Spark核心通用工具类，提供全模块共享的底层通用能力
 * 
 * 主要功能分类：
 * 1. 文件操作：下载、复制、解压、临时目录管理
 * 2. 网络工具：获取本地IP、端口绑定、URI编解码
 * 3. 类加载：动态加载类、获取调用栈信息
 * 4. 序列化辅助：序列化/反序列化流包装
 * 5. 资源管理：内存计算、大小格式化
 * 6. 并发工具：线程池创建、超时执行
 * 7. Hadoop集成：配置获取、安全认证
 * 8. 日志增强：结构化日志、堆栈跟踪格式化
 * 
 * 该类通过mixin多个trait组合功能，是Spark所有上层模块的公共依赖
 */
private[spark] object Utils
  extends Logging
  with SparkClassUtils
  with SparkEnvUtils
  with SparkErrorUtils
  with SparkFileUtils
  with SparkSerDeUtils
  with SparkStreamUtils
  with SparkStringUtils
  with SparkSystemUtils {

  private val sparkUncaughtExceptionHandler = new SparkUncaughtExceptionHandler
  @volatile private var cachedLocalDir: String = ""

  /**
   * 驱动端默认内存值，定义在此处方便全模块引用
   */
  val DEFAULT_DRIVER_MEM_MB = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  @volatile private var localRootDirs: Array[String] = null

  /** Scheme used for files that are locally available on worker nodes in the cluster. */
  /** 工作节点本地可用文件的URI协议 */
  val LOCAL_SCHEME = "local"

  private val weakStringInterner = Interners.newWeakInterner[String]()

  private val PATTERN_FOR_COMMAND_LINE_ARG = "-D(.+?)=(.+)".r

  private val COPY_BUFFER_LEN = 1024

  private val copyBuffer = ThreadLocal.withInitial[Array[Byte]](() => {
    new Array[Byte](COPY_BUFFER_LEN)
  })

  /**
   * 反序列化Long值（用于[[org.apache.spark.api.python.PythonPartitioner]]）
   */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /**
   * 使用指定序列化器通过嵌套流完成序列化
   */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /**
   * 使用指定序列化器通过嵌套流完成反序列化
   */
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /**
   * 弱引用字符串驻留，降低内存占用
   */
  def weakIntern(s: String): String = {
    weakStringInterner.intern(s)
  }

  /**
   * 在指定上下文类加载器下执行代码块，执行后恢复原类加载器
   */
  def withContextClassLoader[T](ctxClassLoader: ClassLoader)(fn: => T): T = {
    val oldClassLoader = Thread.currentThread().getContextClassLoader()
    try {
      Thread.currentThread().setContextClassLoader(ctxClassLoader)
      fn
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader)
    }
  }

  private def writeByteBufferImpl(bb: ByteBuffer, writer: (Array[Byte], Int, Int) => Unit): Unit = {
    if (bb.hasArray) {
      // 如果ByteBuffer有数组支撑，避免额外拷贝
      writer(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      // 回退到拷贝方式
      val buffer = {
        // 复用线程本地拷贝缓存
        copyBuffer.get()
      }
      val originalPosition = bb.position()
      var bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      while (bytesToCopy > 0) {
        bb.get(buffer, 0, bytesToCopy)
        writer(buffer, 0, bytesToCopy)
        bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      }
      bb.position(originalPosition)
    }
  }

  /**
   * 将ByteBuffer写入DataOutput的常用工具方法
   */
  def writeByteBuffer(bb: ByteBuffer, out: DataOutput): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  /**
   * 将ByteBuffer写入OutputStream的常用工具方法
   */
  def writeByteBuffer(bb: ByteBuffer, out: OutputStream): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  /**
   * JDK实现的chmod 700，设置文件仅所有者可读写执行
   *
   * @param file 需要修改权限的文件
   * @return 权限修改成功返回true，否则返回false
   */
  def chmod700(file: File): Boolean = {
    file.setReadable(false, false) &&
    file.setReadable(true, true) &&
    file.setWritable(false, false) &&
    file.setWritable(true, true) &&
    file.setExecutable(false, false) &&
    file.setExecutable(true, true)
  }

  /**
   * 在指定父目录下创建临时目录，JVM退出时自动删除
   */
  override def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
   * 创建Executor本地临时目录，Executor停止时保证一定会被清理
   * 仅在YARN模式下有特殊行为，应对硬关机时无法清理的场景
   */
  def createExecutorLocalTempDir(conf: SparkConf, namePrefix: String): File = {
    if (Utils.isRunningInYarnContainer(conf)) {
      // YARN容器内使用默认Java临时目录，该目录已经在容器目录内
      createTempDir(namePrefix = namePrefix)
    } else {
      createTempDir(getLocalDir(conf), namePrefix)
    }
  }

  /**
   * 将输入流的前maxSize字节拷贝到内存缓冲区，主要用于提前检查压缩文件损坏
   *
   * 返回一个新的输入流，包含原输入流全部数据：前maxSize字节从缓冲区读取，后续从原流读取
   * 实现提前检查压缩文件头损坏，避免后续计算到一半才失败
   *
   * @return 包含全部原始数据的输入流
   */
  def copyStreamUpTo(in: InputStream, maxSize: Long): InputStream = {
    var count = 0L
    val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
    val fullyCopied = tryWithSafeFinally {
      val bufSize = Math.min(8192L, maxSize)
      val buf = new Array[Byte](bufSize.toInt)
      var n = 0
      while (n != -1 && count < maxSize) {
        n = in.read(buf, 0, Math.min(maxSize - count, bufSize).toInt)
        if (n != -1) {
          out.write(buf, 0, n)
          count += n
        }
      }
      count < maxSize
    } {
      try {
        if (count < maxSize) {
          in.close()
        }
      } finally {
        out.close()
      }
    }
    if (fullyCopied) {
      out.toChunkedByteBuffer.toInputStream(dispose = true)
    } else {
      new SequenceInputStream( out.toChunkedByteBuffer.toInputStream(dispose = true), in)
    }
  }

  /**
   * 将文件名编码为URI可接受的原始路径，处理文件名中包含空格等非法URI字符的情况
   * 注意：文件名不能包含 / 或 \
   */
  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` 和 `localhost` 只是占位，避免URI将fileName解析为scheme或host。前缀"/"是必须的，因为URI不接受相对路径。后续会移除它
    encodeRelativeUnixPathToURIRawPath(fileName)
  }

  /**
   * 与[[encodeFileNameToURIRawPath]]功能相同，但处理相对Unix路径
   */
  def encodeRelativeUnixPathToURIRawPath(path: String): String = {
    require(!path.startsWith("/") && !path.contains("\\"))
    // `file` 和 `localhost` 只是占位，避免URI将fileName解析为scheme或host。前缀"/"是必须的，因为URI不接受相对路径。后续会移除它
    new URI("file", null, "localhost", -1, "/" + path, null, null).getRawPath.substring(1)
  }

  /**
   * 从URI原始路径解码得到文件名，如果URI路径以/结尾，返回最后一个/之前的名称
   */
  def decodeFileNameInURI(uri: URI): String = {
    val rawPath = uri.getRawPath
    val rawFileName = rawPath.split("/").last
    new URI("file:///" + rawFileName).getPath.substring(1)
  }

  /**
   * 从指定URL下载文件或目录到目标目录，支持HTTP、Hadoop兼容文件系统、本地文件系统多种方式
   * 仅Hadoop兼容文件系统支持下载目录
   * 如果useCache为true，会先尝试从同一应用多个Executor共享的本地缓存获取，主要用于Executor端，本地模式不使用缓存
   * 如果目标文件已存在且内容与请求文件不同，抛出SparkException
   * 如果shouldUntar为true，会将tar.gz/tgz文件解压到目标目录，这是遗留行为，推荐使用spark.archives配置或SparkContext.addArchive
   *
   * 这是Spark中最重要的文件分发方法之一，用于将依赖文件（jar、配置等）下载到Executor本地
   * 支持多种协议：HTTP/HTTPS、HDFS、S3、本地文件系统等
   * 通过文件锁机制保证多个Executor不会重复下载同一文件
   */
  def fetchFile(
      url: String,
      targetDir: File,
      conf: ReadOnlySparkConf,
      hadoopConf: Configuration,
      timestamp: Long,
      useCache: Boolean,
      shouldUntar: Boolean = true): File = {
    val fileName = decodeFileNameInURI(new URI(url))
    val targetFile = new File(targetDir, fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache", defaultValue = true)
    if (useCache && fetchCacheEnabled) {
      val cachedFileName = s"${url.hashCode}${timestamp}_cache"
      val lockFileName = s"${url.hashCode}${timestamp}_lock"
      // 第一次缓存本地目录，后续复用
      if (cachedLocalDir.isEmpty) {
        this.synchronized {
          if (cachedLocalDir.isEmpty) {
            cachedLocalDir = getLocalDir(conf)
          }
        }
      }
      val localDir = new File(cachedLocalDir)
      val lockFile = new File(localDir, lockFileName)
      val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel()
      // 仅一个Executor进入，FileLock仅用于控制多个Executor下载文件的同步，无论锁类型都安全
      val lock = lockFileChannel.lock()
      val cachedFile = new File(localDir, cachedFileName)
      try {
        if (!cachedFile.exists()) {
          doFetchFile(url, localDir, cachedFileName, conf, hadoopConf)
        }
      } finally {
        lock.release()
        lockFileChannel.close()
      }
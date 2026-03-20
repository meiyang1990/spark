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

package org.apache.spark.io

import java.io._
import java.util.Locale

import com.github.luben.zstd.{NoPool, RecyclingBufferPool, ZstdInputStreamNoFinalizer, ZstdOutputStreamNoFinalizer}
import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import com.ning.compress.lzf.parallel.PLZFOutputStream
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.xerial.snappy.{Snappy, SnappyInputStream, SnappyOutputStream}

import org.apache.spark.{SparkConf, SparkIllegalArgumentException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * CompressionCodec允许自定义选择不同的压缩实现，用于块存储中的数据压缩。
 *
 * @note 该编解码器的传输协议不保证在不同版本的Spark之间兼容。
 * 它仅用于单个Spark应用内部的压缩工具。
 */
@DeveloperApi
trait CompressionCodec {

  /** 将输出流包装为压缩输出流，写入的数据会被自动压缩 */
  def compressedOutputStream(s: OutputStream): OutputStream

  /** 创建支持连续写入的压缩输出流，默认实现直接委托给compressedOutputStream */
  private[spark] def compressedContinuousOutputStream(s: OutputStream): OutputStream = {
    compressedOutputStream(s)
  }

  /** 将输入流包装为压缩输入流，读取时会自动解压缩数据 */
  def compressedInputStream(s: InputStream): InputStream

  /** 创建支持连续读取的压缩输入流，默认实现直接委托给compressedInputStream */
  private[spark] def compressedContinuousInputStream(s: InputStream): InputStream = {
    compressedInputStream(s)
  }
}

/** CompressionCodec伴生对象，提供压缩编解码器的工厂方法和常量定义 */
private[spark] object CompressionCodec {

  /** 判断给定的编解码器是否支持序列化流的拼接（即多个压缩块可以连续拼接后仍能正确解压） */
  private[spark] def supportsConcatenationOfSerializedStreams(codec: CompressionCodec): Boolean = {
    (codec.isInstanceOf[SnappyCompressionCodec] || codec.isInstanceOf[LZFCompressionCodec]
      || codec.isInstanceOf[LZ4CompressionCodec] || codec.isInstanceOf[ZStdCompressionCodec])
  }

  val LZ4 = "lz4"
  val LZF = "lzf"
  val SNAPPY = "snappy"
  val ZSTD = "zstd"

  /** 压缩编解码器短名称到全限定类名的映射表 */
  private[spark] val shortCompressionCodecNames = Map(
    LZ4 -> classOf[LZ4CompressionCodec].getName,
    LZF -> classOf[LZFCompressionCodec].getName,
    SNAPPY -> classOf[SnappyCompressionCodec].getName,
    ZSTD -> classOf[ZStdCompressionCodec].getName)

  /** 从SparkConf中获取配置的压缩编解码器名称 */
  def getCodecName(conf: SparkConf): String = {
    conf.get(IO_COMPRESSION_CODEC)
  }

  /** 使用默认配置的编解码器名称创建CompressionCodec实例 */
  def createCodec(conf: SparkConf): CompressionCodec = {
    createCodec(conf, getCodecName(conf))
  }

  /**
   * 根据编解码器名称通过反射创建CompressionCodec实例。
   * 先从短名称映射表中查找对应的全限定类名，找不到则直接使用传入的名称作为类名；
   * 然后通过反射获取接受SparkConf参数的构造函数并实例化；
   * 如果类不存在或参数非法则抛出编解码器不可用的异常。
   */
  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    // 尝试将短名称转换为全限定类名，如果映射表中没有则直接使用原名称
    val codecClass =
      shortCompressionCodecNames.getOrElse(codecName.toLowerCase(Locale.ROOT), codecName)
    val codec = try {
      // 通过反射获取带SparkConf参数的构造函数
      val ctor =
        Utils.classForName[CompressionCodec](codecClass).getConstructor(classOf[SparkConf])
      // 使用构造函数创建编解码器实例
      Some(ctor.newInstance(conf))
    } catch {
      // 类未找到或参数非法时返回None
      case _: ClassNotFoundException | _: IllegalArgumentException => None
    }
    // 如果创建失败则抛出编解码器不可用异常
    codec.getOrElse(throw SparkCoreErrors.codecNotAvailableError(codecName))
  }

  /**
   * 获取编解码器的短名称。
   * 如果传入的已经是短名称则直接返回；
   * 否则从映射表中反查全限定类名对应的短名称，找不到则抛出异常。
   */
  def getShortName(codecName: String): String = {
    // 将编解码器名称转为小写
    val lowercasedCodec = codecName.toLowerCase(Locale.ROOT)
    // 如果短名称映射表已包含该名称，说明本身就是短名称，直接返回
    if (shortCompressionCodecNames.contains(lowercasedCodec)) {
      lowercasedCodec
    } else {
      // 从映射表中反向查找：根据全限定类名找到对应的短名称
      shortCompressionCodecNames
        .collectFirst { case (k, v) if v == codecName => k }
        // 找不到则抛出短名称未找到异常
        .getOrElse { throw new SparkIllegalArgumentException(
          errorClass = "CODEC_SHORT_NAME_NOT_FOUND",
          messageParameters = Map("codecName" -> codecName))}
    }
  }

  val FALLBACK_COMPRESSION_CODEC = SNAPPY
  val ALL_COMPRESSION_CODECS = shortCompressionCodecNames.values.toSeq
}

/**
 * :: DeveloperApi ::
 * LZ4压缩编解码器实现。块大小可通过`spark.io.compression.lz4.blockSize`配置。
 *
 * @note 该编解码器的传输协议不保证在不同版本Spark之间兼容，仅用于单个Spark应用内部。
 */
@DeveloperApi
class LZ4CompressionCodec(conf: SparkConf) extends CompressionCodec {

  // SPARK-28102: 缓存LZ4Factory和XXHashFactory的最快实例，避免JNI加载失败时
  // 重复调用fastestInstance()导致的性能问题（JNI加载路径会在静态同步方法中抛异常，引起锁竞争）
  @transient private[this] lazy val lz4Factory: LZ4Factory = LZ4Factory.fastestInstance()
  @transient private[this] lazy val xxHashFactory: XXHashFactory = XXHashFactory.fastestInstance()

  private[this] val defaultSeed: Int = 0x9747b28c // LZ4BlockOutputStream的默认哈希种子
  private[this] val blockSize = conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt

  /**
   * 创建LZ4压缩输出流：使用快速压缩器、基于xxHash的校验和，
   * 关闭syncFlush以获得更好的压缩率
   */
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val syncFlush = false
    new LZ4BlockOutputStream(
      s,
      blockSize,
      lz4Factory.fastCompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      syncFlush)
  }

  /**
   * 创建LZ4解压输入流：使用安全解压器（会进行边界检查）、xxHash校验和，
   * 设置遇到空块时不停止读取以支持流的拼接
   */
  override def compressedInputStream(s: InputStream): InputStream = {
    LZ4BlockInputStream.newBuilder()
      .withDecompressor(lz4Factory.safeDecompressor())
      .withChecksum(xxHashFactory.newStreamingHash32(defaultSeed).asChecksum)
      .withStopOnEmptyBlock(false)
      .build(s)
  }
}


/**
 * :: DeveloperApi ::
 * LZF压缩编解码器实现。
 *
 * @note 该编解码器的传输协议不保证在不同版本Spark之间兼容，仅用于单个Spark应用内部。
 */
@DeveloperApi
class LZFCompressionCodec(conf: SparkConf) extends CompressionCodec {
  private val parallelCompression = conf.get(IO_COMPRESSION_LZF_PARALLEL)

  /**
   * 创建LZF压缩输出流：如果启用了并行压缩则使用PLZFOutputStream，
   * 否则使用普通LZFOutputStream并设置flush时完成当前块
   */
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    if (parallelCompression) {
      new PLZFOutputStream(s)
    } else {
      new LZFOutputStream(s).setFinishBlockOnFlush(true)
    }
  }

  /** 创建LZF解压输入流 */
  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}


/**
 * :: DeveloperApi ::
 * Snappy压缩编解码器实现。块大小可通过`spark.io.compression.snappy.blockSize`配置。
 *
 * @note 该编解码器的传输协议不保证在不同版本Spark之间兼容，仅用于单个Spark应用内部。
 */
@DeveloperApi
class SnappyCompressionCodec(conf: SparkConf) extends CompressionCodec {

  // 在构造时尝试加载Snappy原生库，如果加载失败则将Error包装为IllegalArgumentException抛出
  try {
    Snappy.getNativeLibraryVersion
  } catch {
    case e: Error => throw new IllegalArgumentException(e)
  }
  private[this] val blockSize = conf.get(IO_COMPRESSION_SNAPPY_BLOCKSIZE).toInt

  /** 创建Snappy压缩输出流，使用配置的块大小 */
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new SnappyOutputStream(s, blockSize)
  }

  /** 创建Snappy解压输入流 */
  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}

/**
 * :: DeveloperApi ::
 * ZStandard压缩编解码器实现。详情参见 http://facebook.github.io/zstd/
 *
 * @note 该编解码器的传输协议不保证在不同版本Spark之间兼容，仅用于单个Spark应用内部。
 */
@DeveloperApi
class ZStdCompressionCodec(conf: SparkConf) extends CompressionCodec {

  private val bufferSize = conf.get(IO_COMPRESSION_ZSTD_BUFFERSIZE).toInt
  // zstd默认压缩级别为1，因为它是所有级别中最快的，同时具有相当高的压缩比
  private val level = conf.get(IO_COMPRESSION_ZSTD_LEVEL)
  private val strategy = conf.get(IO_COMPRESSION_ZSTD_STRATEGY)

  // 根据配置决定是否使用缓冲池回收：启用则使用RecyclingBufferPool复用缓冲区，否则使用NoPool不复用
  private val bufferPool = if (conf.get(IO_COMPRESSION_ZSTD_BUFFERPOOL_ENABLED)) {
    RecyclingBufferPool.INSTANCE
  } else {
    NoPool.INSTANCE
  }

  private val workers = conf.get(IO_COMPRESSION_ZSTD_WORKERS)

  /**
   * 创建Zstd压缩输出流，外层包装BufferedOutputStream以减少小数据量时JNI调用的开销。
   * 设置压缩级别、工作线程数，并可选地设置压缩策略。
   */
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val os = new ZstdOutputStreamNoFinalizer(s, bufferPool).setLevel(level).setWorkers(workers)
    // 如果配置了压缩策略则设置
    strategy.foreach(os.setStrategy)
    new BufferedOutputStream(os, bufferSize)
  }

  /**
   * SPARK-29322: 创建支持连续写入的Zstd压缩输出流。
   * 设置closeFrameOnFlush为true，使得flush时关闭当前帧，
   * 避免连续读取的输入流在读取未关闭帧时卡住。
   */
  override private[spark] def compressedContinuousOutputStream(s: OutputStream) = {
    val os = new ZstdOutputStreamNoFinalizer(s, bufferPool)
      .setLevel(level)
      .setWorkers(workers)
      .setCloseFrameOnFlush(true)
    new BufferedOutputStream(os, bufferSize)
  }

  /** 创建Zstd解压输入流，外层包装BufferedInputStream以减少小数据量时JNI调用的开销 */
  override def compressedInputStream(s: InputStream): InputStream = {
    new BufferedInputStream(new ZstdInputStreamNoFinalizer(s, bufferPool), bufferSize)
  }

  /**
   * SPARK-26283: 创建支持从未关闭帧中连续读取的Zstd解压输入流。
   * 设置setContinuous(true)允许从未关闭的帧中读取数据（例如读取zstd压缩的事件日志），
   * 默认isContinuous为false时读取未关闭帧会抛出截断错误异常。
   */
  override def compressedContinuousInputStream(s: InputStream): InputStream = {
    new BufferedInputStream(
      new ZstdInputStreamNoFinalizer(s, bufferPool).setContinuous(true), bufferSize)
  }
}

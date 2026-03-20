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

import java.io.InputStream
import java.util.Locale

import scala.collection.Seq

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.io.{CompressionCodec => SparkCompressionCodec}

/**
 * Hadoop 压缩编解码器查询和输入流创建工具。
 * 除了标准 Hadoop 编解码器外，还支持 Spark 的 Zstandard 编解码器
 * 并支持非标准扩展名如 `.zstd` 和 `.gzip`
 */
object HadoopCodecStreams {
  private val ZSTD_EXTENSIONS = Seq(".zstd", ".zst")

  // 根据文件扩展名获取解压编解码器
  def getDecompressionCodec(
    config: Configuration,
    file: Path): Option[CompressionCodec] = {
    val factory = new CompressionCodecFactory(config)
    Option(factory.getCodec(file)).orElse {
      // 尝试非标准扩展名如 .zstd 和 .gzip
      file.getName.toLowerCase() match {
        case name if name.endsWith(".zstd") =>
          Option(factory.getCodecByName(classOf[ZStandardCodec].getName))
        case name if name.endsWith(".gzip") =>
          Option(factory.getCodecByName(classOf[GzipCodec].getName))
        case _ => None
      }
    }
  }

  // 为 Zstd 文件创建解压输入流
  def createZstdInputStream(
    file: Path,
    inputStream: InputStream): Option[InputStream] = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)
    val fileName = file.getName.toLowerCase(Locale.ROOT)

    val isOpt = if (ZSTD_EXTENSIONS.exists(fileName.endsWith)) {
      Some(
        SparkCompressionCodec
          .createCodec(sparkConf, SparkCompressionCodec.ZSTD)
          .compressedInputStream(inputStream)
      )
    } else {
      None
    }
    isOpt
  }

  // 根据文件路径创建适配的解压输入流
  def createInputStream(
    config: Configuration,
    file: Path): InputStream = {
    val fs = file.getFileSystem(config)
    val inputStream: InputStream = fs.open(file)

    getDecompressionCodec(config, file)
      .map { codec =>
        try {
          codec.createInputStream(inputStream)
        } catch {
          case e: RuntimeException =>
            // 如果 Hadoop 不支持 Zstd，尝试使用 Spark 的 Zstd 编解码器
            createZstdInputStream(file, inputStream).getOrElse(throw e)
        }
      }.getOrElse(inputStream)
  }
}

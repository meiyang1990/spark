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

package org.apache.spark.paths

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * 文件路径的规范化表示。提供类型安全的路径处理方式。
 * Spark 使用多种方式表示路径：1. Hadoop Path.toString 2. Java URI.toString
 */
case class SparkPath private (private val underlying: String) {
  // 返回 URL 编码后的路径字符串
  def urlEncoded: String = underlying
  // 转换为 Java URI 对象
  def toUri: URI = new URI(underlying)
  // 转换为 Hadoop Path 对象
  def toPath: Path = new Path(toUri)
  // 返回原始路径字符串
  override def toString: String = underlying
}

object SparkPath {
  /**
   * 从 Hadoop 路径字符串创建 SparkPath
   * 调用者需确保字符串编码方式正确
   */
  def fromPathString(str: String): SparkPath = fromPath(new Path(str))
  // 从 Hadoop Path 对象创建 SparkPath
  def fromPath(path: Path): SparkPath = fromUri(path.toUri)
  // 从文件状态信息创建 SparkPath
  def fromFileStatus(fs: FileStatus): SparkPath = fromPath(fs.getPath)

  /**
   * 从 URL 编码字符串创建 SparkPath
   * 调用者需确保字符串是有效的 URL 编码格式
   */
  def fromUrlString(str: String): SparkPath = SparkPath(str)
  // 从 URI 对象创建 SparkPath
  def fromUri(uri: URI): SparkPath = fromUrlString(uri.toString)
}

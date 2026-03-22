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

package org.apache.spark.deploy.master

import java.io._
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils


/**
 * 文件级持久化引擎，用于Spark Standalone集群Master节点持久化应用和Worker元数据。
 * 将每个应用和Worker的状态单独存储为磁盘文件，当应用或Worker移除时自动删除对应文件。
 * 负责在Master故障恢复时从磁盘加载已有的应用和Worker信息。
 * 
 * @param dir 持久化存储目录，不存在则自动创建
 * @param serializer 对象序列化器，用于将状态对象序列化写入磁盘
 * @param codec 可选压缩编解码器，用于压缩持久化文件节省存储空间
 */
private[master] class FileSystemPersistenceEngine(
    val dir: String,
    val serializer: Serializer,
    val codec: Option[CompressionCodec] = None)
  extends PersistenceEngine with Logging {

  try {
    // 创建持久化存储目录
    Files.createDirectories(Paths.get(dir))
  } catch {
    // 处理目录已经是符号链接的特殊情况，在链接指向的真实路径创建目录
    case _: FileAlreadyExistsException if Files.isSymbolicLink(Paths.get(dir)) =>
      Files.createDirectories(Paths.get(dir).toRealPath())
  }

  /**
   * 持久化指定名称的对象到磁盘文件
   * @param name 对象名称，用作文件名
   * @param obj 要持久化的对象
   */
  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  /**
   * 删除指定名称对象的持久化文件
   * @param name 对象名称，对应要删除的文件名
   */
  override def unpersist(name: String): Unit = {
    val f = new File(dir + File.separator + name)
    if (!f.delete()) {
      logWarning(log"Error deleting ${MDC(PATH, f.getPath())}")
    }
  }

  /**
   * 读取所有名称前缀匹配的持久化对象
   * @param prefix 文件名前缀，用于过滤需要加载的对象类型
   * @tparam T 反序列化后的对象类型
   * @return 所有匹配的对象序列
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T]).toImmutableArraySeq
  }

  /**
   * 将对象序列化写入指定文件
   * @param file 目标输出文件
   * @param value 要序列化的对象
   */
  private def serializeIntoFile(file: File, value: AnyRef): Unit = {
    // 文件已存在抛出异常，避免覆盖已有持久化数据
    if (file.exists()) { throw new IllegalStateException("File already exists: " + file) }
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    var fileOut: OutputStream = new FileOutputStream(file)
    // 如果配置了压缩，对输出流进行压缩包装
    codec.foreach { c => fileOut = c.compressedOutputStream(fileOut) }
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      // 使用指定序列化器序列化对象并写入文件
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      // 确保流被正确关闭，避免资源泄漏
      if (out != null) {
        out.close()
      }
      fileOut.close()
    }
  }

  /**
   * 从指定文件反序列化读取对象
   * @param file 输入持久化文件
   * @param m 对象类型标签
   * @tparam T 反序列化后的对象类型
   * @return 反序列化得到的对象
   */
  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    var fileIn: InputStream = new FileInputStream(file)
    // 如果配置了压缩，对输入流进行解压缩包装
    codec.foreach { c => fileIn = c.compressedInputStream(new FileInputStream(file)) }
    var in: DeserializationStream = null
    try {
      // 使用指定序列化器反序列化读取对象
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      // 确保流被正确关闭，避免资源泄漏
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
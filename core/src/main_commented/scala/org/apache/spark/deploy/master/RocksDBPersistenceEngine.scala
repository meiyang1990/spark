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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.rocksdb._

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.Serializer

/**
 * 基于RocksDB实现的Spark Master状态持久化引擎，用于将集群元数据持久化到磁盘
 * 属于Spark Standalone集群模式下Master节点的持久化组件
 *
 * @param dir RocksDB存储目录，不存在则自动创建
 * @param serializer 用于对象序列化的Spark序列化器
 */
private[master] class RocksDBPersistenceEngine(
    val dir: String,
    val serializer: Serializer)
  extends PersistenceEngine with Logging {

  // 加载RocksDB本地库
  RocksDB.loadLibrary()

  // 创建RocksDB存储目录，处理符号链接场景
  private val path = try {
    Files.createDirectories(Paths.get(dir))
  } catch {
    case _: FileAlreadyExistsException if Files.isSymbolicLink(Paths.get(dir)) =>
      Files.createDirectories(Paths.get(dir).toRealPath())
  }

  /**
   * 块表配置：使用全量布隆过滤器，禁用索引压缩，优化点查询性能
   * 参考RocksDB文档：https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#full-filters-new-format
   */
  private val tableFormatConfig = new BlockBasedTableConfig()
    .setFilterPolicy(new BloomFilter(10.0D, false))
    .setEnableIndexCompression(false)
    .setIndexBlockRestartInterval(8)
    .setFormatVersion(5)

  /**
   * RocksDB全局选项配置：分层压缩策略优化空间与性能
   * 最低层使用ZSTD高压缩比减少磁盘占用，其他层使用LZ4平衡压缩比和速度
   * 参考RocksDB文档：https://github.com/facebook/rocksdb/wiki/Compression#configuration
   */
  private val options = new Options()
    .setCreateIfMissing(true)
    .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
    .setCompressionType(CompressionType.LZ4_COMPRESSION)
    .setTableFormatConfig(tableFormatConfig)

  // 打开RocksDB实例
  private val db: RocksDB = RocksDB.open(options, path.toString)

  /**
   * 持久化单个对象到RocksDB，以name作为key存储
   * @param name 持久化对象的key名称前缀
   * @param obj 需要持久化的对象
   */
  override def persist(name: String, obj: Object): Unit = {
    val serialized = serializer.newInstance().serialize(obj)
    // 处理支持数组的序列化结果直接写入
    if (serialized.hasArray) {
      db.put(name.getBytes(UTF_8), serialized.array())
    } else {
      // 不支持数组的则先复制到字节数组再写入
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      db.put(name.getBytes(UTF_8), bytes)
    }
  }

  /**
   * 从RocksDB删除指定对象
   * @param name 要删除对象的key
   */
  override def unpersist(name: String): Unit = {
    db.delete(name.getBytes(UTF_8))
  }

  /**
   * 读取所有key前缀匹配name的持久化对象并反序列化返回
   * @param name key前缀名称
   * @tparam T 返回对象的类型
   * @return 所有匹配对象的序列
   */
  override def read[T: ClassTag](name: String): Seq[T] = {
    val result = new ArrayBuffer[T]
    val iter = db.newIterator()
    try {
      // 定位到第一个以name为前缀的key
      iter.seek(name.getBytes(UTF_8))
      // 遍历所有前缀匹配的key，反序列化后加入结果
      while (iter.isValid && new String(iter.key()).startsWith(name)) {
        result.append(serializer.newInstance().deserialize[T](ByteBuffer.wrap(iter.value())))
        iter.next()
      }
    } finally {
      // 确保迭代器正确关闭
      iter.close()
    }
    result.toSeq
  }
}
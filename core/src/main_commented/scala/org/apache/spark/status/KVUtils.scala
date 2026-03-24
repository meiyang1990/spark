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

package org.apache.spark.status

import java.io.File
import java.nio.file.Files

import scala.annotation.meta.getter
import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.StreamReadConstraints
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.fusesource.leveldbjni.internal.NativeDB
import org.rocksdb.RocksDBException

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.{FsHistoryProvider, FsHistoryProviderMetadata}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.History
import org.apache.spark.internal.config.History.HYBRID_STORE_DISK_BACKEND
import org.apache.spark.internal.config.History.HybridStoreDiskBackend
import org.apache.spark.internal.config.History.HybridStoreDiskBackend._
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore._

/**
 * KV存储工具类，为Spark状态存储和历史服务器提供KV存储创建、序列化、遍历等通用能力
 * 提供统一的LevelDB/RocksDB后端抽象，支持不同序列化方式和异常处理
 */
private[spark] object KVUtils extends Logging {

  /** 用于注解构造函数参数，标记该参数作为KVStore的索引字段 */
  type KVIndexParam = KVIndex @getter

  /**
   * 根据配置和运行模式确定KV存储磁盘后端类型
   * @param conf Spark配置
   * @param live 是否是运行中应用的UI存储
   * @return 选中的磁盘后端类型
   */
  private def backend(conf: SparkConf, live: Boolean) = {
    if (live) {
      // 对于运行中UI的磁盘KV存储，目前固定使用ROCKSDB，相比LevelDB有读写性能提升
      HybridStoreDiskBackend.ROCKSDB
    } else {
      HybridStoreDiskBackend.withName(conf.get(HYBRID_STORE_DISK_BACKEND))
    }
  }

  /**
   * 根据配置和运行模式确定KV存储序列化器
   * @param conf Spark配置
   * @param live 是否是运行中应用的UI存储
   * @return 对应的序列化器实例
   */
  private def serializer(conf: SparkConf, live: Boolean) = {
    if (live) {
      // 对于运行中UI的磁盘KV存储，固定使用protobuf序列化器，默认JSON+Gzip序列化性能较低
      new KVStoreProtobufSerializer()
    } else {
      serializerForHistoryServer(conf)
    }
  }

  /**
   * 支持Scala类型序列化的KVStore序列化器，兼容Spark API序列化配置
   * 基于Jackson实现JSON序列化，添加Scala模块支持和合理的默认配置
   */
  private[spark] class KVStoreScalaSerializer extends KVStoreSerializer {

    // 注册Scala模块，支持Scala常见数据类型序列化
    mapper.registerModule(DefaultScalaModule)
    // 不序列化值为ABSENT的属性，减小存储体积
    mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_ABSENT)

    // SPARK-49872: 移除Jackson对JSON字符串长度的限制，支持大对象存储
    mapper.getFactory.setStreamReadConstraints(
      StreamReadConstraints.builder().maxStringLength(Int.MaxValue).build()
    )
  }

  /**
   * 打开或创建一个基于磁盘的KVStore
   *
   * @param path 存储路径
   * @param metadata 元数据，用于校验存储版本兼容性。新存储会写入该元数据
   * @param conf Spark配置，用于获取磁盘后端配置
   * @param live 是否是运行中应用的存储
   * @tparam M 元数据类型
   * @return 打开后的KVStore实例
   */
  def open[M: ClassTag](
      path: File,
      metadata: M,
      conf: SparkConf,
      live: Boolean): KVStore = {
    require(metadata != null, "Metadata is required.")

    val kvSerializer = serializer(conf, live)
    val db = backend(conf, live) match {
      case LEVELDB =>
        logWarning("The LEVELDB is deprecated. Please use ROCKSDB instead.")
        new LevelDB(path, kvSerializer)
      case ROCKSDB => new RocksDB(path, kvSerializer)
    }
    val dbMeta = db.getMetadata(classTag[M].runtimeClass)
    if (dbMeta == null) {
      // 新存储写入元数据
      db.setMetadata(metadata)
    } else if (dbMeta != metadata) {
      // 元数据不匹配，关闭存储抛出异常
      db.close()
      throw new MetadataMismatchException()
    }

    db
  }

  /**
   * 为历史服务器获取配置对应的序列化器
   * @param conf Spark配置
   * @return 对应序列化器实例
   */
  def serializerForHistoryServer(conf: SparkConf): KVStoreScalaSerializer = {
    History.LocalStoreSerializer.withName(conf.get(History.LOCAL_STORE_SERIALIZER)) match {
      case History.LocalStoreSerializer.JSON =>
        new KVStoreScalaSerializer()
      case History.LocalStoreSerializer.PROTOBUF =>
        new KVStoreProtobufSerializer()
      case other =>
        throw new IllegalArgumentException(s"Unrecognized KV store serializer $other")
    }
  }

  /**
   * 根据配置创建KVStore实例，支持磁盘存储和内存存储两种模式
   * 自动处理存储版本不兼容、数据损坏等异常，会清理旧数据后重新创建
   * @param storePath 磁盘存储路径，None则使用内存存储
   * @param live 是否是运行中应用的存储
   * @param conf Spark配置
   * @return 创建好的KVStore实例
   */
  def createKVStore(
      storePath: Option[File],
      live: Boolean,
      conf: SparkConf): KVStore = {
    storePath.map { path =>
      val diskBackend = backend(conf, live)
      // 根据后端类型选择不同的目录名，避免不同后端混用目录
      val dir = diskBackend match {
        case LEVELDB => "listing.ldb"
        case ROCKSDB => "listing.rdb"
      }

      val dbPath = Files.createDirectories(new File(path, dir).toPath()).toFile()
      // 设置权限为仅当前用户可读写执行
      Utils.chmod700(dbPath)

      val metadata = FsHistoryProviderMetadata(
        FsHistoryProvider.CURRENT_LISTING_VERSION,
        AppStatusStore.CURRENT_VERSION,
        conf.get(History.HISTORY_LOG_DIR))

      try {
        open(dbPath, metadata, conf, live)
      } catch {
        // 存储版本不兼容，删除整个目录重新创建
        case _: UnsupportedStoreVersionException | _: MetadataMismatchException =>
          logInfo("Detected incompatible DB versions, deleting...")
          path.listFiles().foreach(Utils.deleteRecursively)
          open(dbPath, metadata, conf, live)
        // 数据损坏，删除损坏的存储目录重新创建
        case dbExc @ (_: NativeDB.DBException | _: RocksDBException) =>
          logWarning(log"Failed to load disk store ${MDC(PATH, dbPath)} :", dbExc)
          Utils.deleteRecursively(dbPath)
          open(dbPath, metadata, conf, live)
      }
    }.getOrElse(new InMemoryStore())
  }

  /**
   * 将KVStoreView转换为Scala序列，应用过滤条件并限制返回最大数量
   * @param view KVStore视图
   * @param max 最大返回元素数量
   * @param filter 过滤函数
   * @tparam T 元素类型
   * @return 过滤后的序列
   */
  def viewToSeq[T](
      view: KVStoreView[T],
      max: Int)
      (filter: T => Boolean): Seq[T] = {
    val iter = view.closeableIterator()
    try {
      iter.asScala.filter(filter).take(max).toList
    } finally {
      iter.close()
    }
  }

  /**
   * 将KVStoreView指定区间的元素转换为Scala序列，应用过滤条件
   * @param view KVStore视图
   * @param from 起始位置
   * @param until 结束位置
   * @param filter 过滤函数
   * @tparam T 元素类型
   * @return 过滤后的区间序列
   */
  def viewToSeq[T](
      view: KVStoreView[T],
      from: Int,
      until: Int)(filter: T => Boolean): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.filter(filter).slice(from, until).toList
    }
  }

  /**
   * 将整个KVStoreView转换为Scala序列
   * @param view KVStore视图
   * @tparam T 元素类型
   * @return 包含所有元素的序列
   */
  def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.toList
    }
  }

  /**
   * 统计KVStoreView中满足条件的元素数量
   * @param view KVStore视图
   * @param countFunc 断言函数
   * @tparam T 元素类型
   * @return 满足条件的元素数量
   */
  def count[T](view: KVStoreView[T])(countFunc: T => Boolean): Int = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.count(countFunc)
    }
  }

  /**
   * 遍历KVStoreView所有元素，对每个元素执行指定函数
   * @param view KVStore视图
   * @param foreachFunc 遍历执行函数
   * @tparam T 元素类型
   */
  def foreach[T](view: KVStoreView[T])(foreachFunc: T => Unit): Unit = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.foreach(foreachFunc)
    }
  }

  /**
   * 对KVStoreView所有元素执行映射转换，结果转换为Scala序列
   * @param view KVStore视图
   * @param mapFunc 转换函数
   * @tparam T 输入元素类型
   * @tparam B 输出元素类型
   * @return 转换后的序列
   */
  def mapToSeq[T, B](view: KVStoreView[T])(mapFunc: T => B): Seq[B] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.map(mapFunc).toList
    }
  }

  /**
   * 对KVStoreView所有元素执行映射转换，对转换结果过滤并限制返回数量
   * @param view KVStore视图
   * @param max 最大返回元素数量
   * @param mapFunc 转换函数
   * @param filterFunc 对转换结果的过滤函数
   * @tparam T 输入元素类型
   * @tparam B 输出元素类型
   * @return 过滤转换后的序列
   */
  def mapToSeqWithFilter[T, B](
      view: KVStoreView[T],
      max: Int)
      (mapFunc: T => B)
      (filterFunc: B => Boolean): Seq[B] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.map(mapFunc).filter(filterFunc).take(max).toList
    }
  }

  /**
   * 获取KVStoreView中元素的总数量
   * @param view KVStore视图
   * @tparam T 元素类型
   * @return 元素总数量
   */
  def size[T](view: KVStoreView[T]): Int = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.size
    }
  }

  /**
   * 元数据不匹配异常，用于标记存储版本与当前不兼容
   */
  private[spark] class MetadataMismatchException extends Exception

}
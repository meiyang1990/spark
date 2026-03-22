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

package org.apache.spark.deploy.history

import java.util.Collection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import com.google.common.collect.Lists;

import org.apache.spark.util.kvstore._

/**
 * 文件概要：Spark历史服务器应用日志加载混合存储实现，结合内存存储和磁盘存储，加速事件日志重建过程
 * 核心功能：先使用内存存储重建应用状态，后台异步将数据转储到磁盘KV存储，转储完成后自动切换到磁盘存储
 */

/**
 * 混合KV存储实现，用于加速历史服务器事件日志加载流程
 * 设计思路：重建应用状态阶段先写入内存存储，恢复完成后后台异步转储到磁盘KV存储，转储完成后切换到磁盘存储
 * 使用场景：仅在应用日志重建加载阶段使用，减少加载过程中的磁盘IO，提升加载速度
 */
private[history] class HybridStore extends KVStore {

  private val inMemoryStore = new InMemoryStore()

  private var diskStore: KVStore = null

  // 标识当前是否使用内存存储，true表示用内存，false表示已切换到磁盘
  private val shouldUseInMemoryStore = new AtomicBoolean(true)

  // 标识存储是否已关闭，避免关闭后启动后台转储线程
  private val closed = new AtomicBoolean(false)

  // 负责将内存数据转储到磁盘的后台线程
  private var backgroundThread: Thread = null

  // 记录写入到内存存储的所有数据类型，用于后续转储
  // 包访问权限，用于测试
  private[history] val klassMap = new ConcurrentHashMap[Class[_], Boolean]

  override def getMetadata[T](klass: Class[T]): T = {
    getStore().getMetadata(klass)
  }

  override def setMetadata(value: Object): Unit = {
    getStore().setMetadata(value)
  }

  override def read[T](klass: Class[T], naturalKey: Object): T = {
    getStore().read(klass, naturalKey)
  }

  override def write(value: Object): Unit = {
    getStore().write(value)

    // 后台转储线程未启动时，记录新写入的数据类型
    if (backgroundThread == null) {
      klassMap.putIfAbsent(value.getClass(), true)
    }
  }

  override def delete(klass: Class[_], naturalKey: Object): Unit = {
    if (backgroundThread != null) {
      throw new IllegalStateException("delete() shouldn't be called after " +
        "the hybrid store begins switching to RocksDB")
    }

    getStore().delete(klass, naturalKey)
  }

  override def view[T](klass: Class[T]): KVStoreView[T] = {
    getStore().view(klass)
  }

  override def count(klass: Class[_]): Long = {
    getStore().count(klass)
  }

  override def count(klass: Class[_], index: String, indexedValue: Object): Long = {
    getStore().count(klass, index, indexedValue)
  }

  override def close(): Unit = {
    try {
      closed.set(true)
      // 如果后台转储线程还在运行，等待其完成后再关闭
      if (backgroundThread != null && backgroundThread.isAlive()) {
        backgroundThread.join()
      }
    } finally {
      inMemoryStore.close()
      if (diskStore != null) {
        diskStore.close()
      }
    }
  }

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
    if (backgroundThread != null) {
      throw new IllegalStateException("removeAllByIndexValues() shouldn't be " +
        "called after the hybrid store begins switching to RocksDB")
    }

    getStore().removeAllByIndexValues(klass, index, indexValues)
  }

  /**
   * 设置用于存储数据的磁盘KVStore实例
   * @param diskStore 磁盘KVStore实例
   */
  def setDiskStore(diskStore: KVStore): Unit = {
    this.diskStore = diskStore
  }

  /**
   * 触发从内存存储切换到磁盘存储，启动后台线程将内存数据转储到磁盘
   * 转储完成后自动切换底层存储到磁盘，关闭内存存储
   * @param listener 切换完成后的监听器，用于处理成功/失败回调
   * @param appId 对应应用ID，用于线程命名
   * @param attemptId 对应尝试ID，用于线程命名
   */
  def switchToDiskStore(
      listener: HybridStore.SwitchToDiskStoreListener,
      appId: String,
      attemptId: Option[String]): Unit = {
    if (closed.get) {
      return
    }

    backgroundThread = new Thread(() => {
      try {
        // 遍历所有写入过的数据类型，批量转储到磁盘存储
        for (klass <- klassMap.keys().asScala) {
          val values = Lists.newArrayList(
              inMemoryStore.view(klass).closeableIterator())
          diskStore match {
            case db: LevelDB => db.writeAll(values)
            case db: RocksDB => db.writeAll(values)
            case _ => throw new IllegalStateException("Unknown disk-based KVStore")
          }
        }
        // 转储成功回调，切换存储标识并关闭内存存储
        listener.onSwitchToDiskStoreSuccess()
        shouldUseInMemoryStore.set(false)
        inMemoryStore.close()
      } catch {
        case e: Exception =>
          // 转储失败回调
          listener.onSwitchToDiskStoreFail(e)
      }
    })
    backgroundThread.setDaemon(true)
    backgroundThread.setName(s"hybridstore-$appId-$attemptId")
    backgroundThread.start()
  }

  /**
   * 获取当前应该使用的底层KV存储实例
   * @return 当前生效的KV存储（内存或磁盘）
   * 包访问权限，用于测试
   */
  private[history] def getStore(): KVStore = {
    if (shouldUseInMemoryStore.get) {
      inMemoryStore
    } else {
      diskStore
    }
  }
}

/**
 * HybridStore伴生对象，定义切换磁盘存储的监听器接口
 */
private[history] object HybridStore {

  /**
   * 混合存储切换到磁盘存储的结果监听器接口
   * 定义转储成功和失败的回调方法
   */
  trait SwitchToDiskStoreListener {

    /**
     * 切换到磁盘存储成功的回调方法
     */
    def onSwitchToDiskStoreSuccess(): Unit

    /**
     * 切换到磁盘存储失败的回调方法
     * @param e 切换过程抛出的异常
     */
    def onSwitchToDiskStoreFail(e: Exception): Unit
  }
}
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

import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}

/**
 * 文件说明: 解决Guava缓存命运共享问题的非失败共享缓存工具
 *
 * 背景问题：SPARK-43300: Guava cache的命运共享行为可能导致意外的级联失败：
 * 当多个线程同时访问缓存中不存在的同一个key时，Guava cache会阻塞所有请求，只加载一次数据。
 * 如果加载失败，所有请求都会立即失败无法重试。因此单个请求失败会导致所有等待同一个key的无关请求全部失败。
 * 在Spark中，任务可能因为各种原因被任意取消，一个任务在填充缓存条目时被取消，会导致无关任务也出现伪失败，
 * 即使这些任务如果允许尝试的话本可以成功填充缓存。
 *
 * 本工具通过KeyLock对相同key的请求进行同步，使得请求可以单独执行、单独失败，和串行请求行为一致。
 *
 * Guava Cache有多种添加缓存条目的方式，本工具没有完整实现Guava Cache和LoadingCache接口，
 * 只暴露了部分常用API，以便在编译期控制允许的缓存操作。
 */
private[spark] object NonFateSharingCache {
  /**
   * 根据传入的Guava Cache构建非失败共享缓存，自动识别是否为LoadingCache返回对应类型
   * @param cache 原始Guava Cache实例
   * @return 非失败共享缓存实例，如果输入是LoadingCache则返回对应子类实例
   */
  def apply[K, V](cache: Cache[K, V]): NonFateSharingCache[K, V] = cache match {
    case loadingCache: LoadingCache[K, V] => apply(loadingCache)
    case _ => new NonFateSharingCache(cache)
  }

  /**
   * 根据传入的Guava LoadingCache构建非失败共享加载缓存
   * @param loadingCache 原始Guava LoadingCache实例
   * @return 非失败共享加载缓存实例
   */
  def apply[K, V](loadingCache: LoadingCache[K, V]): NonFateSharingLoadingCache[K, V] =
    new NonFateSharingLoadingCache(loadingCache)

  /**
   * 根据加载函数和缓存大小配置构建非失败共享加载缓存
   * 说明：SPARK-44064 添加此方法，避免非核心模块直接使用Guava Cache类型调用其他apply方法，
   * 防止在使用shaded核心模块时导致非核心模块Maven测试失败。当有更多需求时可重构为更通用的实现，或在不再支持Maven测试时移除。
   * @param loadingFunc 缓存值加载函数
   * @param maximumSize 缓存最大容量，0表示不设置
   * @return 非失败共享加载缓存实例
   */
  def apply[K, V](loadingFunc: K => V, maximumSize: Long = 0L): NonFateSharingLoadingCache[K, V] = {
    require(loadingFunc != null)
    val builder = CacheBuilder.newBuilder().asInstanceOf[CacheBuilder[K, V]]
    if (maximumSize > 0L) {
      builder.maximumSize(maximumSize)
    }
    new NonFateSharingLoadingCache(builder.build[K, V](new CacheLoader[K, V] {
      override def load(k: K): V = loadingFunc.apply(k)
    }))
  }

  /**
   * 根据容量、过期配置构建空的非失败共享缓存
   * @param maximumSize 缓存最大容量
   * @param expireAfterAccessTime 访问后过期时间
   * @param expireAfterAccessTimeUnit 过期时间单位
   * @return 非失败共享缓存实例
   */
  def apply[K, V](
      maximumSize: Long,
      expireAfterAccessTime: Long,
      expireAfterAccessTimeUnit: TimeUnit): NonFateSharingCache[K, V] = {
    val builder = CacheBuilder.newBuilder().asInstanceOf[CacheBuilder[K, V]]
    if (maximumSize > 0L) {
      builder.maximumSize(maximumSize)
    }
    if(expireAfterAccessTime > 0) {
      builder.expireAfterAccess(expireAfterAccessTime, expireAfterAccessTimeUnit)
    }
    new NonFateSharingCache(builder.build[K, V]())
  }
}

/**
 * 非失败共享缓存包装类，对Guava Cache进行包装，通过Key锁避免多线程同时加载同一个key导致的命运共享问题
 * @param cache 被包装的原始Guava Cache实例
 * @tparam K 缓存key类型
 * @tparam V 缓存值类型
 */
private[spark] class NonFateSharingCache[K, V](protected val cache: Cache[K, V]) {

  // key级别的锁实例，用于对同一个key的加载操作进行互斥
  protected val keyLock = new KeyLock[K]

  /**
   * 获取缓存值，如果key不存在则通过传入的加载器加载
   * @param key 缓存key
   * @param valueLoader 值加载器
   * @return 缓存值
   */
  def get(key: K, valueLoader: Callable[_ <: V]): V = keyLock.withLock(key) {
    cache.get(key, valueLoader)
  }

  /**
   * 仅当key存在于缓存中时获取其值，否则返回null
   * @param key 缓存key
   * @return 缓存值，不存在则返回null
   */
  def getIfPresent(key: Any): V = cache.getIfPresent(key)

  /**
   * 从缓存中移除指定key
   * @param key 要移除的key
   */
  def invalidate(key: Any): Unit = cache.invalidate(key)

  /**
   * 清空缓存中所有条目
   */
  def invalidateAll(): Unit = cache.invalidateAll()

  /**
   * 返回缓存当前条目数量
   * @return 缓存大小
   */
  def size(): Long = cache.size()
}

/**
 * 非失败共享加载缓存，针对Guava LoadingCache的子类实现，支持自动加载不存在的key
 * @param loadingCache 被包装的原始Guava LoadingCache实例
 * @tparam K 缓存key类型
 * @tparam V 缓存值类型
 */
private[spark] class NonFateSharingLoadingCache[K, V](
  protected val loadingCache: LoadingCache[K, V]) extends NonFateSharingCache[K, V](loadingCache) {

  /**
   * 获取缓存值，如果key不存在则自动通过LoadingCache的加载逻辑加载
   * @param key 缓存key
   * @return 缓存值
   */
  def get(key: K): V = keyLock.withLock(key) {
    loadingCache.get(key)
  }
}
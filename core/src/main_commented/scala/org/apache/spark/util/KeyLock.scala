// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain copy of the License at
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

import java.util.concurrent.ConcurrentHashMap

/**
 * 基于Key粒度的细粒度锁实现，保证相同Key对应的操作同一时间只能有一个执行
 * 
 * 核心设计：使用ConcurrentHashMap维护每个Key对应的锁对象，通过对象锁实现同步，
 * 相同Key的操作会互斥执行，不同Key的操作可以并行，相比全局锁提升并发性能。
 * 
 * @tparam K 锁标识Key的类型，必须正确实现equals和hashCode方法，因为会作为内部Map的键
 */
private[spark] class KeyLock[K] {

  // 存储每个Key对应的锁对象，并发安全
  private val lockMap = new ConcurrentHashMap[K, AnyRef]()

  /**
   * 获取指定Key对应的锁，若锁被占用则阻塞等待直到获取成功
   * @param key 要获取锁的Key标识
   */
  private def acquireLock(key: K): Unit = {
    while (true) {
      // 原子性尝试放入新锁，若Key不存在则成功获取锁
      val lock = lockMap.putIfAbsent(key, new Object)
      if (lock == null) return
      // Key已存在，对已有锁对象同步等待
      lock.synchronized {
        // 持有锁后再次检查Key是否还是对应当前锁对象，避免竞态条件
        while (lockMap.get(key) eq lock) {
          lock.wait()
        }
      }
    }
  }

  /**
   * 释放指定Key对应的锁，并唤醒所有等待该锁的线程
   * @param key 要释放锁的Key标识
   */
  private def releaseLock(key: K): Unit = {
    // 从映射表中移除当前Key的锁
    val lock = lockMap.remove(key)
    // 通知所有等待该锁的线程可以竞争获取锁了
    lock.synchronized {
      lock.notifyAll()
    }
  }

  /**
   * 在指定Key的锁保护下执行给定函数，保证相同Key的函数同一时间只有一个执行
   * @param key 锁标识Key，相同Key保证互斥访问
   * @param func 需要在锁保护下执行的函数
   * @return 函数执行的返回结果
   */
  def withLock[T](key: K)(func: => T): T = {
    if (key == null) {
      throw new NullPointerException("key must not be null")
    }
    acquireLock(key)
    try {
      func
    } finally {
      releaseLock(key)
    }
  }
}
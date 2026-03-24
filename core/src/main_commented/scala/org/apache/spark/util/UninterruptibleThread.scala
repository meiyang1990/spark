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

import javax.annotation.concurrent.GuardedBy

/**
 * 不可被中断的特殊线程实现，提供runUninterruptible方法支持在不被Thread.interrupt()中断的区域执行代码
 * 如果在不可中断区域执行过程中收到中断请求，不会立即设置中断状态，而是会延迟到退出不可中断区域后再处理
 * 
 * 注意：runUninterruptibly只能从当前线程自身调用，不支持其他线程调用
 */
private[spark] class UninterruptibleThread(
    target: Runnable,
    name: String) extends Thread(target, name) {

  def this(name: String) = {
    this(null, name)
  }

  /**
   * 管理不可中断状态和延迟中断请求的内部锁类
   */
  private class UninterruptibleLock {
    /**
     * 标记当前线程是否处于不可中断状态，如果处于该状态，中断请求会被延迟直到退出不可中断状态
     */
    @GuardedBy("uninterruptibleLock")
    private var uninterruptible = false

    /**
     * 标记是否需要在退出不可中断区域后执行中断
     */
    @GuardedBy("uninterruptibleLock")
    private var shouldInterruptThread = false

    /**
     * 标记是否需要等待interrupt()调用完成后再继续执行
     */
    @GuardedBy("uninterruptibleLock")
    private var awaitInterruptThread = false

    /**
     * 更新不可中断状态并返回旧状态值
     * @param value 新的不可中断状态值
     * @return 更新前的不可中断状态值
     */
    def getAndSetUninterruptible(value: Boolean): Boolean = synchronized {
      val uninterruptible = this.uninterruptible
      this.uninterruptible = value
      uninterruptible
    }

    /**
     * 设置是否需要延迟中断标志
     */
    def setShouldInterruptThread(value: Boolean): Unit = synchronized {
      shouldInterruptThread = value
    }

    /**
     * 设置是否需要等待中断调用标志
     */
    def setAwaitInterruptThread(value: Boolean): Unit = synchronized {
      awaitInterruptThread = value
    }

    /**
     * 检查是否有待处理的中断请求
     * @return true表示需要等待中断调用，false表示无待处理中断
     */
    def isInterruptPending: Boolean = synchronized {
      // 清除当前线程已设置的中断状态，并合并到延迟中断标志中
      shouldInterruptThread = Thread.interrupted() || shouldInterruptThread
      // 只有不存在延迟中断且需要等待中断时返回true
      !shouldInterruptThread && awaitInterruptThread
    }

    /**
     * 恢复不可中断状态为可中断，并处理延迟的中断请求
     */
    def recoverInterrupt(): Unit = synchronized {
      uninterruptible = false
      if (shouldInterruptThread) {
        shouldInterruptThread = false
        // 恢复中断状态，执行真正的中断
        UninterruptibleThread.super.interrupt()
      }
    }

    /**
     * 检查当前是否可以安全执行中断操作
     * @return true表示当前不在不可中断区域，可以安全中断；false表示不可中断
     */
    def isInterruptible: Boolean = synchronized {
      shouldInterruptThread = uninterruptible
      // 在释放锁后、调用父类interrupt()之前，可能有新的runUninterruptibly调用进入
      // 使用awaitInterruptThread标志避免这种场景下不可中断区域被中断，解决SPARK-53394
      if (!shouldInterruptThread && !awaitInterruptThread && !isInterrupted) {
        awaitInterruptThread = true
        true
      } else {
        false
      }
    }
  }

  /** 管理不可中断状态和延迟中断的监视器实例 */
  private val uninterruptibleLock = new UninterruptibleLock

  /**
   * 在当前线程的不可中断区域执行指定代码块，执行过程中不会被中断，中断请求会被延迟到执行完成后处理
   * 
   * 注意：此方法只能从当前线程自身调用
   * @param f 需要执行的代码块
   * @return 代码块执行结果
   */
  def runUninterruptibly[T](f: => T): T = {
    // 检查是否是当前线程自身调用
    if (Thread.currentThread() != this) {
      throw new IllegalStateException(s"Call runUninterruptibly in a wrong thread. " +
        s"Expected: $this but was ${Thread.currentThread()}")
    }

    // 如果已经处于不可中断状态，直接执行即可，不需要额外处理
    if (uninterruptibleLock.getAndSetUninterruptible(true)) {
      return f
    }

    // 等待所有待处理的中断请求完成
    while (uninterruptibleLock.isInterruptPending) {
      try {
        Thread.sleep(100)
      } catch {
        // 睡眠过程中被中断，标记需要延迟中断
        case _: InterruptedException => uninterruptibleLock.setShouldInterruptThread(true)
      }
    }

    try {
      f
    } finally {
      // 恢复中断状态，处理所有延迟的中断请求
      uninterruptibleLock.recoverInterrupt()
    }
  }

  /**
   * 重写中断方法，如果当前线程处于不可中断状态则延迟中断，否则立即执行中断
   */
  override def interrupt(): Unit = {
    // 检查是否可以安全中断
    if (uninterruptibleLock.isInterruptible) {
      try {
        super.interrupt()
      } finally {
        // 清除等待中断标志
        uninterruptibleLock.setAwaitInterruptThread(false)
      }
    }
  }
}
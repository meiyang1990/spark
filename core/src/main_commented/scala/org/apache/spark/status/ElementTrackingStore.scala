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

import java.util.Collection
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status._
import org.apache.spark.status.ElementTrackingStore._
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.kvstore._

/**
 * 文件说明：KVStore封装实现，用于跟踪特定类型元素数量，达到阈值时触发对应操作，
 * 常用于Spark UI和事件日志的数据填充，支持异步任务处理和状态刷新机制
 *
 * 核心功能：
 * 1. 跟踪特定类型元素数量，达到阈值时触发清理等自定义动作
 * 2. 提供异步工作线程池处理耗时任务，支持配置同步执行保证确定性
 * 3. 提供刷新触发机制，在关闭存储前通知监听器刷新内部状态到存储
 */
private[spark] class ElementTrackingStore(store: KVStore, conf: SparkConf) extends KVStore {

  /**
   * 带防重复触发 latch 的触发器集合，确保同一时间只有一个触发任务在执行
   * @param triggers 待触发的触发器列表
   */
  private class LatchedTriggers(val triggers: Seq[Trigger[_]]) {
    // 标记是否有触发任务正在等待执行
    private val pending = new AtomicBoolean(false)

    /**
     * 仅当没有待执行触发任务时，触发一次所有触发器，避免重复排队
     * @param f 触发器执行逻辑
     * @return 排队结果：已排队/跳过排队
     */
    def fireOnce(f: Seq[Trigger[_]] => Unit): WriteQueueResult = {
      if (pending.compareAndSet(false, true)) {
        doAsync {
          pending.set(false)
          f(triggers)
        }
        WriteQueued
      } else {
        WriteSkippedQueue
      }
    }

    /**
     * 添加新触发器，返回新的LatchedTriggers实例
     * @param addlTrigger 待添加的触发器
     * @return 包含新触发器的新实例
     */
    def :+(addlTrigger: Trigger[_]): LatchedTriggers = {
      new LatchedTriggers(triggers :+ addlTrigger)
    }
  }

  // 按元素类型分类存储触发器集合
  private val triggers = new HashMap[Class[_], LatchedTriggers]()
  // 存储刷新触发回调列表，在关闭存储前执行
  private val flushTriggers = new ListBuffer[() => Unit]()
  // 异步任务执行线程池，根据配置选择异步单线程或同步执行器
  private val executor: ExecutorService = if (conf.get(ASYNC_TRACKING_ENABLED)) {
    ThreadUtils.newDaemonSingleThreadExecutor("element-tracking-store-worker")
  } else {
    ThreadUtils.sameThreadExecutorService()
  }

  // 标记存储是否已停止接收新任务
  @volatile private var stopped = false

  /**
   * 注册类型元素数量触发器，当该类型元素数量达到阈值时触发对应动作
   * @param klass 要监控的元素类型
   * @param threshold 触发动作的元素数量阈值
   * @param action 达到阈值时执行的动作，参数为当前存储中该类型的元素总数
   */
  def addTrigger(klass: Class[_], threshold: Long)(action: Long => Unit): Unit = {
    val newTrigger = Trigger(threshold, action)
    triggers.get(klass) match {
      case None =>
        triggers(klass) = new LatchedTriggers(Seq(newTrigger))
      case Some(latchedTrigger) =>
        triggers(klass) = latchedTrigger :+ newTrigger
    }
  }

  /**
   * 注册存储刷新前执行的回调，通常在关闭存储前调用，用于刷新中间状态到存储
   * （例如历史服务器回放正在运行的应用事件日志场景）
   * @param action 刷新时要执行的动作
   */
  def onFlush(action: => Unit): Unit = {
    flushTriggers += { () => action }
  }

  /**
   * 将动作提交给异步线程池执行，当ASYNC_TRACKING_ENABLED配置为false时改为同步执行
   * @param fn 要执行的动作
   */
  def doAsync(fn: => Unit): Unit = {
    executor.submit(new Runnable() {
      override def run(): Unit = Utils.tryLog { fn }
    })
  }

  override def read[T](klass: Class[T], naturalKey: Any): T = store.read(klass, naturalKey)

  override def write(value: Any): Unit = store.write(value)

  /**
   * 将元素写入存储，并可选择检查是否触发对应类型的数量阈值触发器
   * @param value 待写入的元素
   * @param checkTriggers 是否检查并触发阈值触发器
   * @return 排队结果：已排队/跳过排队
   */
  def write(value: Any, checkTriggers: Boolean): WriteQueueResult = {
    write(value)

    if (checkTriggers && !stopped) {
      triggers.get(value.getClass).map { latchedList =>
        latchedList.fireOnce { list =>
          // 获取当前该类型元素总数
          val count = store.count(value.getClass)
          // 遍历所有触发器，对超过阈值的执行对应动作
          list.foreach { t =>
            if (count > t.threshold) {
              t.action(count)
            }
          }
        }
      }.getOrElse(WriteSkippedQueue)
    } else {
      WriteSkippedQueue
    }
  }

  def removeAllByIndexValues[T](klass: Class[T], index: String, indexValues: Iterable[_]): Boolean =
    removeAllByIndexValues(klass, index, indexValues.asJavaCollection)

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
    store.removeAllByIndexValues(klass, index, indexValues)
  }

  override def delete(klass: Class[_], naturalKey: Any): Unit = store.delete(klass, naturalKey)

  override def getMetadata[T](klass: Class[T]): T = store.getMetadata(klass)

  override def setMetadata(value: Any): Unit = store.setMetadata(value)

  override def view[T](klass: Class[T]): KVStoreView[T] = store.view(klass)

  override def count(klass: Class[_]): Long = store.count(klass)

  override def count(klass: Class[_], index: String, indexedValue: Any): Long = {
    store.count(klass, index, indexedValue)
  }

  override def close(): Unit = {
    close(true)
  }

  /**
   * 关闭存储，可选择是否同时关闭底层封装的KVStore
   * @param closeParent 是否关闭底层KVStore
   */
  def close(closeParent: Boolean): Unit = synchronized {
    if (stopped) {
      return
    }

    // 标记停止，拒绝后续新触发任务
    stopped = true
    // 关闭线程池，等待5秒超时
    ThreadUtils.shutdown(executor, FiniteDuration(5, TimeUnit.SECONDS))

    // 执行所有刷新触发回调，捕获记录异常不中断流程
    flushTriggers.foreach { trigger =>
      Utils.tryLog(trigger())
    }

    // 根据配置选择是否关闭底层KVStore
    if (closeParent) {
      store.close()
    }
  }

  /** 判断底层存储是否为内存存储实现 */
  def usingInMemoryStore: Boolean = store.isInstanceOf[InMemoryStore]

  /**
   * 单个触发器定义
   * @param threshold 元素数量阈值
   * @param action 达到阈值后执行的动作，参数为当前元素总数
   */
  private case class Trigger[T](
      threshold: Long,
      action: Long => Unit)

}

/**
 * ElementTrackingStore伴生对象，定义写操作排队结果密封特质
 */
private[spark] object ElementTrackingStore {
  /**
   * 写操作排队结果密封特质，仅用于辅助测试单次触发执行的正确性
   * 正常流程下write()结果未被使用
   */
  sealed trait WriteQueueResult

  /** 触发器已成功加入异步队列 */
  object WriteQueued extends WriteQueueResult
  /** 已有触发器在排队，跳过本次排队 */
  object WriteSkippedQueue extends WriteQueueResult
}
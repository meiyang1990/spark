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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

import org.apache.spark.{ComplexFutureAction, FutureAction, JobSubmitter}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{RDD_LIMIT_INITIAL_NUM_PARTITIONS, RDD_LIMIT_SCALE_UP_FACTOR}
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * 文件级注释：RDD异步动作扩展类集合
 * 本文件提供RDD的异步Action操作扩展，通过隐式转换为RDD添加非阻塞版本的动作方法，
 * 允许用户并行提交多个Spark作业，支持异步回调和作业取消，提升作业调度灵活性。
 */

/**
 * 异步RDD操作封装类，为RDD提供一系列异步版本的Action操作
 * 
 * 设计目的：标准RDD的Action操作（如count、collect）都是阻塞调用，会等待作业完成才返回。
 * 异步版本返回FutureAction对象，允许调用后立即返回，作业在后台异步执行，支持：
 * 1. 非阻塞执行，允许Driver继续处理其他任务
 * 2. 并行提交多个Spark作业，提升整体吞吐量
 * 3. 取消正在运行的作业
 * 4. 使用Scala Future的回调机制处理计算结果
 * 
 * 使用方式：通过隐式转换将AsyncRDDActions的方法附加到RDD对象上，导入隐式后即可直接调用
 * 
 * @param self 要扩展异步能力的原RDD对象
 * @tparam T RDD元素类型
 */
class AsyncRDDActions[T: ClassTag](self: RDD[T]) extends Serializable with Logging {

  /**
   * 异步计算RDD元素总数
   * 
   * @return 包裹元素总数的FutureAction对象，可用于获取结果或取消作业
   */
  def countAsync(): FutureAction[Long] = self.withScope {
    // 使用原子变量累加各分区计数，保证并发安全
    val totalCount = new AtomicLong
    self.context.submitJob(
      self,
      // 单个分区计数函数
      (iter: Iterator[T]) => {
        var result = 0L
        while (iter.hasNext) {
          result += 1L
          iter.next()
        }
        result
      },
      // 需要计算的所有分区列表
      Range(0, self.partitions.length),
      // 每个分区结果回调：累加计数到原子变量
      (index: Int, data: Long) => totalCount.addAndGet(data),
      // 最终结果获取函数
      totalCount.get())
  }

  /**
   * 异步收集RDD所有元素到Driver端
   * 
   * 注意：和同步collect一样，会将所有分区数据拉取到Driver内存，大数据集可能导致OOM
   * 
   * @return 包裹所有元素序列的FutureAction对象
   */
  def collectAsync(): FutureAction[Seq[T]] = self.withScope {
    // 按分区索引存储各分区结果数组
    val results = new Array[Array[T]](self.partitions.length)
    self.context.submitJob[T, Array[T], Seq[T]](self, _.toArray, Range(0, self.partitions.length),
      // 将分区结果按索引存入对应位置
      (index, data) => results(index) = data, results.flatten.toImmutableArraySeq)
  }

  /**
   * 异步获取RDD的前num个元素
   * 
   * 优化策略：采用增量分批次扫描分区，不需要扫描所有分区即可获得足够元素：
   * 1. 初始仅扫描少量分区，快速拿到结果
   * 2. 如果结果不足，根据已有元素数量估算需要扫描的新分区数，扩大扫描范围
   * 3. 如果之前扫描未找到元素，使用指数放大快速扩大扫描范围
   * 这种策略对数据分布不均匀的RDD非常高效，减少不必要的分区扫描
   * 
   * @param num 需要获取的元素数量
   * @return 包裹前num个元素序列的FutureAction对象
   */
  def takeAsync(num: Int): FutureAction[Seq[T]] = self.withScope {
    // 保存当前调用位置信息，用于Spark UI显示
    val callSite = self.context.getCallSite()
    // 克隆当前本地属性，避免异步执行时被其他作业修改
    val localProperties = Utils.cloneProperties(self.context.getLocalProperties)
    // 异步回调使用的共享线程池
    implicit val executionContext = AsyncRDDActions.futureExecutionContext
    // 存储收集到的结果元素
    val results = new ArrayBuffer[T]
    // RDD总分区数
    val totalParts = self.partitions.length

    // 分区数放大因子，配置最小值为2
    val scaleUpFactor = Math.max(self.conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)

    /**
     * 递归函数：持续分批次扫描分区，直到收集到足够元素或扫描完所有分区
     * 使用非阻塞Future回调机制，异步处理每批作业结果，并根据结果决定是否继续扫描
     * 
     * @param partsScanned 已经扫描过的分区数量
     * @param jobSubmitter 作业提交器，用于提交子作业
     * @return 包裹最终结果序列的Future对象
     */
    def continue(partsScanned: Int)(implicit jobSubmitter: JobSubmitter): Future[Seq[T]] =
      // 终止条件：已收集足够元素，或已经扫描完所有分区
      if (results.size >= num || partsScanned >= totalParts) {
        Future.successful(results.toSeq)
      } else {
        // 计算本次批次需要扫描的分区数量，初始使用配置值
        var numPartsToTry = self.conf.get(RDD_LIMIT_INITIAL_NUM_PARTITIONS)
        if (partsScanned > 0) {
          // 非首次扫描，根据已有结果调整扫描策略
          if (results.isEmpty) {
            // 之前扫描没有找到元素，指数放大扫描范围
            numPartsToTry = partsScanned * scaleUpFactor
          } else {
            // 根据已有元素密度估算需要的分区数，额外放大1.5倍保证能拿到足够元素
            numPartsToTry = Math.max(1,
              (1.5 * num * partsScanned / results.size).toInt - partsScanned)
            // 单次放大不超过配置的最大倍数
            numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
          }
        }

        // 还需要收集的元素数量
        val left = num - results.size
        // 本次批次要扫描的分区索引范围
        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts))

        // 存储本次扫描各分区的结果
        val buf = new Array[Array[T]](p.size)
        // 恢复调用位置和本地属性，保证异步作业上下文正确
        self.context.setCallSite(callSite)
        self.context.setLocalProperties(localProperties)
        // 提交本次扫描作业，每个分区只取left个元素即可
        val job = jobSubmitter.submitJob(self,
          (it: Iterator[T]) => it.take(left).toArray,
          p,
          (index: Int, data: Array[T]) => buf(index) = data,
          ())
        // 作业完成后，合并结果到结果缓冲区，递归继续扫描
        job.flatMap { _ =>
          buf.foreach(results ++= _.take(num - results.size))
          continue(partsScanned + p.size)
        }
      }

    new ComplexFutureAction[Seq[T]](continue(0)(_))
  }

  /**
   * 异步对RDD每个元素应用用户提供的函数
   * 通常用于副作用操作，例如将每个元素写入外部存储，不返回计算结果
   * 
   * @param f 作用于每个元素的函数
   * @return 空结果的FutureAction对象
   */
  def foreachAsync(f: T => Unit): FutureAction[Unit] = self.withScope {
    // 清理闭包中的不可序列化引用，保证作业可序列化提交
    val cleanF = self.context.clean(f)
    self.context.submitJob[T, Unit, Unit](self, _.foreach(cleanF), Range(0, self.partitions.length),
      (index, data) => (), ())
  }

  /**
   * 异步对RDD每个分区应用用户提供的函数
   * 适合批量处理的副作用操作，例如批量写入数据库，减少连接创建开销
   * 
   * @param f 作用于分区迭代器的函数
   * @return 空结果的FutureAction对象
   */
  def foreachPartitionAsync(f: Iterator[T] => Unit): FutureAction[Unit] = self.withScope {
    self.context.submitJob[T, Unit, Unit](self, f, Range(0, self.partitions.length),
      (index, data) => (), ())
  }
}

/**
 * AsyncRDDActions伴生对象，提供异步回调使用的共享线程池
 */
private object AsyncRDDActions {
  // 创建守护线程缓存池，用于执行Future的回调任务，最大线程数128
  val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncRDDActions-future", 128))
}
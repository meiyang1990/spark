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
 * 异步 RDD 操作集合
 * 
 * 通过隐式转换为 RDD 提供异步版本的 Action 操作
 * 
 * 【设计目的】
 * 标准的 RDD Action（如 count、collect）是阻塞操作，调用后会等待作业完成
 * 异步版本返回 FutureAction，允许：
 * - 非阻塞执行：调用后立即返回，作业在后台执行
 * - 并行提交多个作业
 * - 支持取消正在运行的作业
 * - 使用 Future 的回调机制处理结果
 * 
 * 【使用方式】
 * import org.apache.spark.rdd.RDD._  // 导入隐式转换
 * val future = rdd.countAsync()
 * // 做其他事情...
 * val result = future.get()  // 获取结果（阻塞）
 * 
 * 【实现机制】
 * 内部使用 SparkContext.submitJob 提交异步作业
 * 返回的 FutureAction 可用于监控进度、获取结果或取消作业
 */
class AsyncRDDActions[T: ClassTag](self: RDD[T]) extends Serializable with Logging {

  /**
   * 异步计算 RDD 中元素的数量
   * 
   * 使用 AtomicLong 累加各分区的计数结果
   * 
   * @return FutureAction[Long]，包含元素总数
   */
  def countAsync(): FutureAction[Long] = self.withScope {
    // 使用原子长整型累加各分区的结果
    val totalCount = new AtomicLong
    self.context.submitJob(
      self,
      // 分区内计数函数
      (iter: Iterator[T]) => {
        var result = 0L
        while (iter.hasNext) {
          result += 1L
          iter.next()
        }
        result
      },
      // 处理所有分区
      Range(0, self.partitions.length),
      // 结果处理器：将每个分区的计数累加到 totalCount
      (index: Int, data: Long) => totalCount.addAndGet(data),
      // 最终结果
      totalCount.get())
  }

  /**
   * 异步收集 RDD 的所有元素
   * 
   * 注意：与同步版本 collect() 相同，会将所有数据拉取到 Driver
   * 对于大数据集可能导致 OOM
   * 
   * @return FutureAction[Seq[T]]，包含所有元素
   */
  def collectAsync(): FutureAction[Seq[T]] = self.withScope {
    // 为每个分区分配一个数组槽位
    val results = new Array[Array[T]](self.partitions.length)
    self.context.submitJob[T, Array[T], Seq[T]](self, _.toArray, Range(0, self.partitions.length),
      // 按分区索引存储结果
      (index, data) => results(index) = data, results.flatten.toImmutableArraySeq)
  }

  /**
   * 异步获取 RDD 的前 num 个元素
   * 
   * 【优化策略】
   * 不是一次性扫描所有分区，而是递归地扫描部分分区：
   * 1. 初始只扫描少量分区
   * 2. 如果结果不足，根据比例估算需要扫描的分区数并扩大范围
   * 3. 如果之前扫描没有结果，使用指数放大（scaleUpFactor）
   * 
   * 这种策略对于数据分布不均匀的 RDD 特别有效
   * 
   * @param num 需要获取的元素数量
   * @return FutureAction[Seq[T]]，包含前 num 个元素
   */
  def takeAsync(num: Int): FutureAction[Seq[T]] = self.withScope {
    // 保存调用位置，用于 UI 显示
    val callSite = self.context.getCallSite()
    // 克隆本地属性，避免并发修改
    val localProperties = Utils.cloneProperties(self.context.getLocalProperties)
    // 用于异步处理子任务聚合的线程池
    implicit val executionContext = AsyncRDDActions.futureExecutionContext
    val results = new ArrayBuffer[T]
    val totalParts = self.partitions.length

    // 扫描分区数的放大因子，最小为 2
    val scaleUpFactor = Math.max(self.conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)

    /**
     * 递归扫描分区直到收集到足够的元素
     * 
     * 使用非阻塞的 Future 回调机制，异步处理每批作业的结果
     * 并根据需要触发下一批作业
     */
    def continue(partsScanned: Int)(implicit jobSubmitter: JobSubmitter): Future[Seq[T]] =
      // 终止条件：已收集足够元素或已扫描完所有分区
      if (results.size >= num || partsScanned >= totalParts) {
        Future.successful(results.toSeq)
      } else {
        // 计算本次迭代要尝试扫描的分区数
        var numPartsToTry = self.conf.get(RDD_LIMIT_INITIAL_NUM_PARTITIONS)
        if (partsScanned > 0) {
          // 根据之前的扫描结果调整扫描策略
          if (results.isEmpty) {
            // 之前没有找到任何数据，使用指数放大
            numPartsToTry = partsScanned * scaleUpFactor
          } else {
            // 根据已有结果插值估算需要的分区数，并放大 50% 以确保足够
            numPartsToTry = Math.max(1,
              (1.5 * num * partsScanned / results.size).toInt - partsScanned)
            // 限制最大放大倍数
            numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
          }
        }

        val left = num - results.size
        // 计算本次要扫描的分区范围
        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts))

        val buf = new Array[Array[T]](p.size)
        // 恢复调用位置和本地属性（可能被其他作业修改）
        self.context.setCallSite(callSite)
        self.context.setLocalProperties(localProperties)
        // 提交扫描作业，每个分区只取 left 个元素
        val job = jobSubmitter.submitJob(self,
          (it: Iterator[T]) => it.take(left).toArray,
          p,
          (index: Int, data: Array[T]) => buf(index) = data,
          ())
        // 作业完成后，合并结果并递归继续
        job.flatMap { _ =>
          buf.foreach(results ++= _.take(num - results.size))
          continue(partsScanned + p.size)
        }
      }

    new ComplexFutureAction[Seq[T]](continue(0)(_))
  }

  /**
   * 异步对 RDD 的每个元素应用函数
   * 
   * 用于副作用操作（如写入外部存储），不返回结果
   * 
   * @param f 应用于每个元素的函数
   * @return FutureAction[Unit]
   */
  def foreachAsync(f: T => Unit): FutureAction[Unit] = self.withScope {
    // 清理闭包，确保可序列化
    val cleanF = self.context.clean(f)
    self.context.submitJob[T, Unit, Unit](self, _.foreach(cleanF), Range(0, self.partitions.length),
      (index, data) => (), ())
  }

  /**
   * 异步对 RDD 的每个分区应用函数
   * 
   * 用于需要批量处理的副作用操作（如批量写入数据库）
   * 
   * @param f 应用于每个分区迭代器的函数
   * @return FutureAction[Unit]
   */
  def foreachPartitionAsync(f: Iterator[T] => Unit): FutureAction[Unit] = self.withScope {
    self.context.submitJob[T, Unit, Unit](self, f, Range(0, self.partitions.length),
      (index, data) => (), ())
  }
}

/**
 * AsyncRDDActions 的伴生对象
 * 
 * 提供共享的线程池用于异步操作的回调处理
 */
private object AsyncRDDActions {
  // 守护线程池：用于 Future 的回调执行
  // 最大 128 个线程，避免创建过多线程
  val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("AsyncRDDActions-future", 128))
}

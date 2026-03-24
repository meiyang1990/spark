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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * 文件级注释：
 * 集合大小估算跟踪工具，为需要估算内存占用的集合提供增量采样估算能力
 * 通过指数退避采样策略降低SizeEstimator的调用频率，分摊估算开销
 * 核心设计：利用最近两次采样结果外推当前集合大小，实现O(1)快速估算
 */

/**
 * 集合大小估算跟踪接口，为集合提供增量内存大小估算能力
 * 采用指数退避采样策略，减少昂贵的SizeEstimator调用次数，分摊估算开销
 * 仅Spark内部私有接口，用于需要动态跟踪内存占用的集合实现
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * 采样间隔指数增长系数，控制采样频率
   * 例如系数为2时，采样时机为1, 2, 4, 8...次更新后
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /** 存储历史采样样本，仅保留最近两个样本用于外推计算 */
  private val samples = new mutable.Queue[Sample]

  /** 平均每次更新带来的字节增量，基于最近两次采样计算得到 */
  private var bytesPerUpdate: Double = _

  /** 上次重置采样后，集合累计的插入/更新操作总次数 */
  private var numUpdates: Long = _

  /** 下一次采样需要达到的更新次数 */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * 重置所有采样数据，重新开始采样
   * 当集合大小发生剧烈变化后需要调用此方法重置估算状态
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  /**
   * 每次集合更新后需要调用的回调方法，判断是否需要进行新采样
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * 采集当前集合大小样本，更新估算参数
   */
  private def takeSample(): Unit = {
    // 添加新样本：估算当前大小并记录当前更新次数
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // 只保留最近两个样本，超出则移除最早的样本
    if (samples.size > 2) {
      samples.dequeue()
    }
    // 根据两个样本计算平均每次更新的字节增量
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // 样本不足2个时，默认增量为0
      case _ => 0
    }
    bytesPerUpdate = math.max(0, bytesDelta)
    // 计算下一次采样的更新次数，按指数增长
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  /**
   * 估算当前集合占用的字节数，O(1)时间复杂度
   * @return 估算的集合大小，单位字节
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    // 从上次采样后外推计算增量
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    // 总大小 = 上次采样大小 + 外推增量
    (samples.last.size + extrapolatedDelta).toLong
  }
}

/**
 * 样本数据容器，存储一次采样的大小和对应更新次数
 */
private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
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

package org.apache.spark.rdd.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.PeriodicCheckpointer

/**
 * 文件路径: core/src/main/scala/org/apache/spark/rdd/util/PeriodicRDDCheckpointer.scala
 *
 * RDD周期性检查点工具类，自动管理RDD的持久化、检查点和旧数据清理，常用于长流迭代计算场景
 *
 * 核心功能：
 * 1. 自动对新生成的RDD执行持久化，并维护最多3个持久化RDD，自动取消过期RDD持久化节省空间
 * 2. 按照指定间隔自动对RDD执行检查点，自动删除旧检查点文件
 * 3. 适合迭代式计算、增量流式计算中，持续生成新RDD并需要定期保存检查点的场景
 *
 * 使用说明：
 * 用户需要在新RDD生成后、物化之前调用update()方法。update()会完成以下操作：
 *  - 持久化新RDD（若未持久化），加入持久化队列
 *  - 清理持久化队列，保证最多保留3个持久化RDD，自动取消旧RDD持久化
 *  - 如果开启了检查点且达到检查点间隔：
 *     - 对当前RDD执行检查点，加入检查点队列
 *     - 删除更早的检查点文件
 *
 * 注意事项：
 *  - 该类不应该被拷贝，拷贝后会导致检查点管理冲突
 *  - 删除旧检查点文件后，旧RDD的isCheckpointed仍会返回true，但文件已不存在
 *
 * @param checkpointInterval  执行检查点的间隔，每生成多少个RDD执行一次检查点
 * @param sc                  Spark上下文实例
 * @param storageLevel        RDD持久化存储级别
 * @tparam T  RDD元素类型
 */
private[spark] class PeriodicRDDCheckpointer[T](
    checkpointInterval: Int,
    sc: SparkContext,
    storageLevel: StorageLevel)
  extends PeriodicCheckpointer[RDD[T]](checkpointInterval, sc) {
  // 检查存储级别不能为NONE，必须持久化RDD
  require(storageLevel != StorageLevel.NONE)

  /** 构造方法，默认使用MEMORY_ONLY存储级别 */
  def this(checkpointInterval: Int, sc: SparkContext) =
    this(checkpointInterval, sc, StorageLevel.MEMORY_ONLY)

  /**
   * 对目标RDD执行检查点
   * @param data 要检查点的RDD
   */
  override protected def checkpoint(data: RDD[T]): Unit = data.checkpoint()

  /**
   * 检查目标RDD是否已经完成检查点
   * @param data 待检查的RDD
   * @return 是否已经检查点
   */
  override protected def isCheckpointed(data: RDD[T]): Boolean = data.isCheckpointed

  /**
   * 持久化RDD，仅当RDD未持久化时才执行持久化
   * @param data 要持久化的RDD
   */
  override protected def persist(data: RDD[T]): Unit = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      data.persist(storageLevel)
    }
  }

  /**
   * 取消RDD持久化，释放占用的存储空间
   * @param data 要取消持久化的RDD
   */
  override protected def unpersist(data: RDD[T]): Unit = data.unpersist()

  /**
   * 获取RDD检查点文件路径
   * @param data 已检查点的RDD
   * @return 检查点文件路径迭代器
   */
  override protected def getCheckpointFiles(data: RDD[T]): Iterable[String] = {
    data.getCheckpointFile.map(x => x)
  }
}
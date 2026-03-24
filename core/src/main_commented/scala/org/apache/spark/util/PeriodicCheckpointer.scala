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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._


/**
 * 周期性检查点抽象工具，用于RDD及派生类型（如Graph、DataFrame）的持久化与检查点管理。
 * 自动处理持久化、检查点生成，以及旧数据的取消持久化和检查点文件清理。
 * 
 * 使用方式：用户在新的数据集生成后、物化前调用update()方法，之后由本工具自动完成持久化和检查点管理。
 * 调用update()后会执行以下操作：
 *  - 持久化新数据集，加入持久化队列
 *  - 取消多余旧数据集的持久化，保持队列中最多保留3个持久化数据集
 *  - 如果启用检查点且达到检查点间隔：
 *     - 对新数据集生成检查点，加入检查点队列
 *     - 删除更早的旧检查点文件
 * 
 * 注意事项：
 *  - 本类不应被拷贝，否则不同拷贝会在检查点管理上产生冲突
 *  - 旧检查点文件被删除后，旧数据集的isCheckpointed仍会返回true，使用时需注意
 * 
 * @param checkpointInterval  生成检查点的间隔，设置为-1则禁用检查点
 * @param sc  本检查点工具所使用的Spark上下文
 * @tparam T  数据集类型，如RDD[Double]
 */
private[spark] abstract class PeriodicCheckpointer[T](
    val checkpointInterval: Int,
    val sc: SparkContext) extends Logging {

  // 已完成检查点的数据集FIFO队列
  private val checkpointQueue = mutable.Queue[T]()

  // 已持久化的数据集FIFO队列
  private val persistedQueue = mutable.Queue[T]()

  // update()方法被调用的次数，用于判断是否达到检查点间隔
  private var updateCount = 0

  /**
   * 使用新数据集更新检查点状态，处理持久化和检查点生成。
   * 应在数据集物化前调用，确保持久化和检查点能正确生效。
   *
   * @param newData  谱系中生成的新数据集
   */
  def update(newData: T): Unit = {
    persist(newData)
    persistedQueue.enqueue(newData)
    // 保持最多3个持久化数据集，在用户于物化前调用update的语义下，最多保留2个有效计算数据集，支撑迭代计算
    while (persistedQueue.size > 3) {
      val dataToUnpersist = persistedQueue.dequeue()
      unpersist(dataToUnpersist)
    }
    updateCount += 1

    // 持久化完成后处理检查点
    if (checkpointInterval != -1 && (updateCount % checkpointInterval) == 0
      && sc.getCheckpointDir.nonEmpty) {
      // 先添加新检查点，再删除旧检查点
      checkpoint(newData)
      checkpointQueue.enqueue(newData)
      // 删除最新检查点之前的旧检查点
      var canDelete = true
      while (checkpointQueue.size > 1 && canDelete) {
        // 仅当下一个检查点已经成功生成后，才删除最早的检查点
        if (isCheckpointed(checkpointQueue(1))) {
          removeCheckpointFile()
        } else {
          canDelete = false
        }
      }
    }
  }

  /** 对数据集生成检查点 */
  protected def checkpoint(data: T): Unit

  /** 判断数据集是否已经完成检查点 */
  protected def isCheckpointed(data: T): Boolean

  /**
   * 持久化数据集，实现需要检查数据集当前存储级别，避免重复持久化
   */
  protected def persist(data: T): Unit

  /** 取消数据集的持久化 */
  protected def unpersist(data: T): Unit

  /** 获取给定数据集对应的所有检查点文件路径 */
  protected def getCheckpointFiles(data: T): Iterable[String]

  /**
   * 取消所有队列中持久化数据集的持久化
   */
  def unpersistDataSet(): Unit = {
    while (persistedQueue.nonEmpty) {
      val dataToUnpersist = persistedQueue.dequeue()
      unpersist(dataToUnpersist)
    }
  }

  /**
   * 任务结束时调用，删除所有剩余检查点文件
   */
  def deleteAllCheckpoints(): Unit = {
    while (checkpointQueue.nonEmpty) {
      removeCheckpointFile()
    }
  }

  /**
   * 任务结束时调用，删除除最后一个检查点外的所有旧检查点文件
   * 可能队列为空不存在任何检查点
   */
  def deleteAllCheckpointsButLast(): Unit = {
    while (checkpointQueue.size > 1) {
      removeCheckpointFile()
    }
  }

  /**
   * 获取当前所有检查点文件，常与deleteAllCheckpointsButLast配合使用
   */
  def getAllCheckpointFiles: Array[String] = {
    checkpointQueue.flatMap(getCheckpointFiles).toArray
  }

  /**
   * 出队最早的检查点数据集，删除其对应的检查点文件。
   * 删除失败仅打印警告，不会抛出异常中断流程。
   */
  private def removeCheckpointFile(): Unit = {
    val old = checkpointQueue.dequeue()
    // Spark不会自动删除旧检查点，需要手动删除
    getCheckpointFiles(old).foreach(
      PeriodicCheckpointer.removeCheckpointFile(_, sc.hadoopConfiguration))
  }
}

/**
 * PeriodicCheckpointer的伴生对象，提供公共工具方法
 */
private[spark] object PeriodicCheckpointer extends Logging {

  /**
   * 删除指定检查点文件，删除失败仅记录警告不抛出异常
   */
  def removeCheckpointFile(checkpointFile: String, conf: Configuration): Unit = {
    try {
      val path = new Path(checkpointFile)
      val fs = path.getFileSystem(conf)
      fs.delete(path, true)
    } catch {
      case _: Exception =>
        logWarning(log"PeriodicCheckpointer could not remove old checkpoint file: " +
          log"${MDC(FILE_NAME, checkpointFile)}")
    }
  }
}
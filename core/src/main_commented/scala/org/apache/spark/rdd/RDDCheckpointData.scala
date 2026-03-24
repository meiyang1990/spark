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

import scala.reflect.ClassTag

import org.apache.spark.Partition

/**
 * 管理RDD检查点状态转换的枚举类
 * 状态流转：[ 已初始化 --> 检查点进行中 --> 检查点已完成 ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * RDD检查点数据抽象类，存储与RDD检查点相关的所有信息
 * 每个RDD实例对应一个该类实例，负责管理关联RDD的检查点流程，
 * 在检查点完成后提供更新后的分区、迭代器和优先位置信息，截断原有RDD依赖 lineage
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // 关联RDD的检查点当前状态
  protected var cpState = Initialized

  // 存储检查点数据的CheckpointRDD
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   * 检查当前RDD的检查点数据是否已经持久化完成
   * @return 检查点已完成返回true，否则返回false
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * 执行检查点持久化操作，将当前RDD内容物化并持久化到存储
   * 在RDD上第一个动作执行完成后立即调用该方法
   */
  final def checkpoint(): Unit = {
    // 通过原子切换状态防止多个线程并发对同一个RDD执行检查点
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }

    // 执行具体检查点逻辑，由子类实现
    val newRDD = doCheckpoint()

    // 更新状态并截断RDD依赖 lineage
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed
      rdd.markCheckpointed()
    }
  }

  /**
   * 物化当前RDD并持久化其内容，钩子方法
   * 子类需要重写该方法实现自定义检查点行为
   * @return 检查点过程创建的CheckpointRDD
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   * 获取存储检查点数据的CheckpointRDD
   * 仅当检查点状态为Checkpointed时有值
   * @return Some(CheckpointRDD) 检查点完成，否则None
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * 获取检查点RDD的分区数组，仅用于测试
   * @return 检查点RDD的分区数组，未完成检查点返回空数组
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * 用于同步检查点操作的全局锁
 */
private[spark] object RDDCheckpointData
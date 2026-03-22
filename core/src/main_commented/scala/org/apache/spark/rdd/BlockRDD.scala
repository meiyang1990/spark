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

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.storage.{BlockId, BlockManager}

/**
 * BlockRDD的分区实现，每个分区对应一个存储块
 * @param blockId 对应存储块的ID
 * @param idx 分区索引
 */
private[spark] class BlockRDDPartition(val blockId: BlockId, idx: Int) extends Partition {
  val index = idx
}

/**
 * 基于已存储块构建的RDD，每个分区对应Spark块存储系统中的一个数据块
 * 通常用于将已经存储在Spark存储系统中的已有数据，转换为可进行后续计算的RDD
 * 常用于检查点恢复、广播变量处理、以及已有块数据复用场景
 * 
 * @param sc Spark上下文
 * @param blockIds 构成该RDD的所有数据块ID数组
 * @tparam T RDD中元素的类型
 */
private[spark]
class BlockRDD[T: ClassTag](sc: SparkContext, @transient val blockIds: Array[BlockId])
  extends RDD[T](sc, Nil) {

  // 预计算每个块对应的优先位置（节点位置信息）
  @transient lazy val _locations = BlockManager.blockIdsToLocations(blockIds, SparkEnv.get)
  // 标记该RDD是否有效，移除块后会变为无效
  @volatile private var _isValid = true

  /**
   * 生成BlockRDD的所有分区，每个块对应一个分区
   * @return 分区数组
   */
  override def getPartitions: Array[Partition] = {
    assertValid()
    blockIds.indices.map { i =>
      new BlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
    }.toArray
  }

  /**
   * 读取指定分区对应存储块中的数据，生成计算迭代器
   * @param split 待计算的分区
   * @param context 任务上下文
   * @return 分区数据的迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    // 从块管理器获取对应数据块
    blockManager.get[T](blockId) match {
      case Some(block) => block.data.asInstanceOf[Iterator[T]]
      case None =>
        throw SparkCoreErrors.rddBlockNotFoundError(blockId, id)
    }
  }

  /**
   * 获取指定分区的优先位置（数据所在节点），用于任务调度本地化
   * @param split 目标分区
   * @return 数据所在节点主机名列表
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    assertValid()
    _locations(split.asInstanceOf[BlockRDDPartition].blockId)
  }

  /**
   * 移除该BlockRDD对应的所有数据块，此操作不可逆，移除后数据无法恢复
   * 调用后该RDD将变为无效状态
   */
  private[spark] def removeBlocks(): Unit = {
    blockIds.foreach { blockId =>
      sparkContext.env.blockManager.master.removeBlock(blockId)
    }
    _isValid = false
  }

  /**
   * 检查该BlockRDD是否可用，数据块被移除后返回false
   * @return 是否有效
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * 断言该RDD处于有效状态，无效则抛出异常
   */
  private[spark] def assertValid(): Unit = {
    if (!isValid) {
      throw SparkCoreErrors.blockHaveBeenRemovedError(toString)
    }
  }

  /**
   * 获取所有块对应的位置信息映射，供内部使用
   * @return 块ID到节点位置列表的映射
   */
  protected def getBlockIdLocations(): Map[BlockId, Seq[String]] = {
    _locations
  }
}
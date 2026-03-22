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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.util.Utils

/**
 * 笛卡尔积操作的分区实现，每个笛卡尔分区对应两个父RDD各一个分区的组合
 * @param idx 当前分区索引
 * @param rdd1 第一个父RDD
 * @param rdd2 第二个父RDD
 * @param s1Index 第一个父RDD对应分区索引
 * @param s2Index 第二个父RDD对应分区索引
 */
private[spark]
class CartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 序列化任务时更新父分区引用，确保获取最新的分区对象
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

/**
 * 对两个RDD执行笛卡尔积操作的RDD实现，生成所有元素对的组合
 * @param sc Spark上下文
 * @param rdd1 第一个输入RDD
 * @param rdd2 第二个输入RDD
 * @tparam T 第一个RDD元素类型
 * @tparam U 第二个RDD元素类型
 */
private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var rdd2 : RDD[U])
  extends RDD[(T, U)](sc, Nil)
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // 生成两个RDD分区的笛卡尔积组合
    val partitionNum: Long = numPartitionsInRdd2.toLong * rdd1.partitions.length
    // 检查分区数是否超过整数最大值限制
    if (partitionNum > Int.MaxValue) {
      throw SparkCoreErrors.tooManyArrayElementsError(partitionNum, Int.MaxValue)
    }
    val array = new Array[Partition](partitionNum.toInt)
    // 遍历所有父分区组合，生成对应的笛卡尔分区
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    // 合并两个父分区的偏好位置，去重后返回，优化数据本地性
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    // 对两个父分区内的所有元素生成笛卡尔积组合
    for (x <- rdd1.iterator(currSplit.s1, context);
         y <- rdd2.iterator(currSplit.s2, context)) yield (x, y)
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    // 对第一个RDD使用窄依赖，每个笛卡尔分区只依赖第一个RDD的一个分区
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    // 对第二个RDD使用窄依赖，每个笛卡尔分区只依赖第二个RDD的一个分区
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    // 清空对父RDD的引用，帮助GC回收内存
    rdd1 = null
    rdd2 = null
  }
}
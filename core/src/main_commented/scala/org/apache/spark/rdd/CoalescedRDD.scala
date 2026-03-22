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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * 表示合并后的RDD分区，维护合并后分区与父RDD原分区的关联关系
 * @param index 当前合并分区的索引
 * @param rdd 当前分区所属的CoalescedRDD
 * @param parentsIndices 被合并到当前分区的父RDD分区索引列表
 * @param preferredLocation 当前分区的优先位置
 */
private[spark] case class CoalescedRDDPartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int],
    @transient preferredLocation: Option[String] = None) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_)).toImmutableArraySeq

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 任务序列化时更新父分区引用
    parents = parentsIndices.map(rdd.partitions(_)).toImmutableArraySeq
    oos.defaultWriteObject()
  }

  /**
   * 计算当前合并分区中，与优先位置匹配的父分区占比，衡量数据本地性
   * @return 0到1之间的本地性分数，分数越高表示本地性越好
   */
  def localFraction: Double = {
    val loc = parents.count { p =>
      val parentPreferredLocations = rdd.context.getPreferredLocs(rdd, p.index).map(_.host)
      preferredLocation.exists(parentPreferredLocations.contains)
    }
    if (parents.isEmpty) 0.0 else loc.toDouble / parents.size.toDouble
  }
}

/**
 * 表示合并分区后的RDD，将父RDD的多个小分区合并为更少的大分区，减少小任务开销
 * 使用PartitionCoalescer实现分区分配，兼顾分区大小平衡和数据本地性
 * @param prev 需要合并分区的父RDD
 * @param maxPartitions 合并后目标分区数（必须为正）
 * @param partitionCoalescer 用于分区合并的分区分配器实现
 */
private[spark] class CoalescedRDD[T: ClassTag](
    @transient var prev: RDD[T],
    maxPartitions: Int,
    partitionCoalescer: Option[PartitionCoalescer] = None)
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  require(maxPartitions > 0 || maxPartitions == prev.partitions.length,
    s"Number of partitions ($maxPartitions) must be positive.")
  if (partitionCoalescer.isDefined) {
    require(partitionCoalescer.get.isInstanceOf[Serializable],
      "The partition coalescer passed in must be serializable.")
  }

  override def getPartitions: Array[Partition] = {
    // 使用默认或指定的分区分配器
    val pc = partitionCoalescer.getOrElse(new DefaultPartitionCoalescer())

    // 为每个分配后的分组生成一个CoalescedRDDPartition
    pc.coalesce(maxPartitions, prev).zipWithIndex.map {
      case (pg, i) =>
        val ids = pg.partitions.map(_.index).toArray
        CoalescedRDDPartition(i, prev, ids, pg.prefLoc)
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    // 依次迭代当前合并分区包含的所有父分区，聚合结果
    partition.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentPartition =>
      firstParent[T].iterator(parentPartition, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    // 使用窄依赖，避免shuffle
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices.toImmutableArraySeq
    })
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }

  /**
   * 返回分区的优先位置，选择父分区中最多偏好的节点
   * @param partition 需要获取优先位置的分区
   * @return 优先位置列表
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[CoalescedRDDPartition].preferredLocation.toSeq
  }
}

/**
 * 默认分区合并分配器，实现将父RDD分区分配到更少的目标分区，兼顾平衡和本地性
 * 
 * 算法目标：
 * (1) 平衡各目标分区包含的父分区数量，尽量均匀
 * (2) 保证数据本地性，让每个目标分区的偏好节点尽可能覆盖更多父分区偏好
 * (3) 算法高效，时间复杂度为O(n)
 * (4) 均衡分布偏好节点，避免多个分区都集中到同一个节点
 * 
 * 基于"power-of-two random bins-balls"随机负载均衡算法，增加本地性权衡，通过balanceSlack参数平衡本地性和负载均衡
 * @param balanceSlack 本地性与负载均衡的权衡系数，0代表完全偏向负载均衡，1代表完全偏向本地性，默认0.10允许最多10%的不平衡换取更好本地性
 */
private class DefaultPartitionCoalescer(val balanceSlack: Double = 0.10)
  extends PartitionCoalescer {

  // 按照分区数量对分区组排序
  implicit object partitionGroupOrdering extends Ordering[PartitionGroup] {
    override def compare(o1: PartitionGroup, o2: PartitionGroup): Int =
      java.lang.Integer.compare(o1.numPartitions, o2.numPartitions)
  }


  // 固定随机种子保证算法结果可复现
  val rnd = new scala.util.Random(7919)

  // 存储所有目标分区组，每个组对应一个合并后的分区
  val groupArr = ArrayBuffer[PartitionGroup]()

  // 根据主机名索引该主机对应的所有分区组，用于快速查找
  val groupHash = mutable.Map[String, ArrayBuffer[PartitionGroup]]()

  // 记录已经分配的父分区，避免一个父分区被分配到多个目标组
  val initialHash = mutable.Set[Partition]()

  var noLocality = true  // 标记父RDD是否没有任何分区有优先位置信息

  // 获取当前最新的分区优先位置（从DAGScheduler获取，而非静态信息）
  def currPrefLocs(part: Partition, prev: RDD[_]): Seq[String] = {
    prev.context.getPreferredLocs(prev, part.index).map(tl => tl.host)
  }

  /**
   * 分类存储父RDD分区的位置信息，将分区分为有偏好位置和无偏好位置两类
   * @param prev 父RDD
   */
  private class PartitionLocations(prev: RDD[_]) {

    // 存储没有偏好位置的父分区
    val partsWithoutLocs = ArrayBuffer[Partition]()
    // 存储有偏好位置的父分区，保存(主机名, 分区)对
    val partsWithLocs = ArrayBuffer[(String, Partition)]()

    getAllPrefLocs(prev)

    // 获取所有父分区的偏好位置并分类
    def getAllPrefLocs(prev: RDD[_]): Unit = {
      val tmpPartsWithLocs = mutable.LinkedHashMap[Partition, Seq[String]]()
      // 一次性获取所有分区的位置信息，该操作开销较大
      prev.partitions.foreach(p => {
          val locs = currPrefLocs(p, prev)
          if (locs.nonEmpty) {
            tmpPartsWithLocs.put(p, locs)
          } else {
            partsWithoutLocs += p
          }
        }
      )
      // 每个分区最多取前3个偏好位置，生成(主机,分区)对
      for (x <- 0 to 2) {
        tmpPartsWithLocs.foreach { parts =>
          val p = parts._1
          val locs = parts._2
          if (locs.size > x) partsWithLocs += ((locs(x), p))
        }
      }
    }
  }

  /**
   * 获取指定主机上负载最轻的分区组
   * @param key 主机名
   * @return 该主机上负载最轻的分区组，不存在则返回None
   */
  def getLeastGroupHash(key: String): Option[PartitionGroup] =
    groupHash.get(key).filter(_.nonEmpty).map(_.min)

  /**
   * 将父分区添加到目标分区组，标记为已分配
   * @param part 待分配的父分区
   * @param pgroup 目标分区组
   * @return 是否成功分配（未分配过才会成功）
   */
  def addPartToPGroup(part: Partition, pgroup: PartitionGroup): Boolean = {
    if (!initialHash.contains(part)) {
      pgroup.partitions += pgroup
      initialHash += part
      true
    } else { false }
  }

  /**
   * 初始化指定数量的目标分区组，如果有偏好位置则给每个分区组分配一个唯一偏好主机
   * 使用coupon collector算法估计遍历次数，保证尽可能覆盖不同的偏好主机
   * @param targetLen 目标分区组数量
   * @param partitionLocs 分区位置信息
   */
  def setupGroups(targetLen: Int, partitionLocs: PartitionLocations): Unit = {
    // 没有带位置信息的分区，直接创建空分组
    if (partitionLocs.partsWithLocs.isEmpty) {
      (1 to targetLen).foreach(_ => groupArr += new PartitionGroup())
      return
    }

    noLocality = false
    // 根据coupon collector理论，估计覆盖大部分偏好主机需要的遍历次数
    val expectedCoupons2 = 2 * (math.log(targetLen)*targetLen + targetLen + 0.5).toInt
    var numCreated = 0
    var tries = 0

    // 遍历找到尽可能多的不同偏好主机创建分组
    val numPartsToLookAt = math.min(expectedCoupons2, partitionLocs.partsWithLocs.length)
    while (numCreated < targetLen && tries < numPartsToLookAt) {
      val (nxt_replica, nxt_part) = partitionLocs.partsWithLocs(tries)
      tries += 1
      if (!groupHash.contains(nxt_replica)) {
        val pgroup = new PartitionGroup(Some(nxt_replica))
        groupArr += pgroup
        addPartToPGroup(nxt_part, pgroup)
        groupHash.put(nxt_replica, ArrayBuffer(pgroup))
        numCreated += 1
      }
    }
    // 如果不够目标数量，从已有主机中随机选择创建重复分组
    while (numCreated < targetLen) {
      val (nxt_replica, nxt_part) = partitionLocs.partsWithLocs(
        rnd.nextInt(partitionLocs.partsWithLocs.length))
      val pgroup = new PartitionGroup(Some(nxt_replica))
      groupArr += pgroup
      groupHash.getOrElseUpdate(nxt_replica, ArrayBuffer()) += pgroup
      addPartToPGroup(nxt_part, pgroup)
      numCreated += 1
    }
  }

  /**
   * 为父分区选择目标分组，结合本地性和power-of-two负载均衡策略
   * @param p 待分配的父分区
   * @param prev 父RDD
   * @param balanceSlack 平衡系数，允许不平衡换取本地性
   * @param partitionLocs 分区位置信息
   * @return 选择的目标分区组
   */
  def pickBin(
      p: Partition,
      prev: RDD[_],
      balanceSlack: Double,
      partitionLocs: PartitionLocations): PartitionGroup = {
    // 计算允许的不平衡阈值
    val slack = (balanceSlack * prev.partitions.length).toInt
    // 获取父分区偏好主机上负载最轻的分组
    val pref = currPrefLocs(p, prev).flatMap(getLeastGroupHash)
    val prefPart = if (pref.isEmpty) None else Some(pref.min)
    // 随机选两个分组，选择负载轻的那个，这就是power-of-two策略
    val r1 = rnd.nextInt(groupArr.size)
    val r2 = rnd.nextInt(groupArr.size)
    val minPowerOfTwo = {
      if (groupArr(r1).numPartitions < groupArr(r2).numPartitions) {
        groupArr(r1)
      }
      else {
        groupArr(r2)
      }
    }
    if (prefPart.isEmpty) {
      // 没有偏好位置，直接使用负载均衡结果
      return minPowerOfTwo
    }

    val prefPartActual = prefPart.get

    // 如果本地分组负载比最轻负载超过阈值，则优先负载均衡，否则优先本地性
    if (minPowerOfTwo.numPartitions + slack <= prefPartActual.numPartitions) {
      minPowerOfTwo
    } else {
      prefPartActual
    }
  }

  /**
   * 将所有父分区分配到初始化好的目标分组中
   * @param maxPartitions 目标分区数
   * @param prev 父RDD
   * @param balanceSlack 平衡系数
   * @param partitionLocs 分区位置信息
   */
  def throwBalls(
      maxPartitions: Int,
      prev: RDD[_],
      balanceSlack: Double, partitionLocs: PartitionLocations): Unit = {
    if (noLocality) {
      // 没有位置信息，不需要随机化，直接按顺序切分
      if (maxPartitions > groupArr.size) {
        for ((p, i) <- prev.partitions.zipWithIndex) {
          groupArr(i).partitions += p
        }
      } else {
        for (i <- 0 until maxPartitions) {
          val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
          val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
          (rangeStart until rangeEnd).foreach{ j => groupArr(i).partitions += prev.partitions(j) }
        }
      }
    } else {
      // 保证每个分组至少有一个分区，先用带位置的分区填充空分组
      val partIter = partitionLocs.partsWithLocs.iterator
      groupArr.filter(pg => pg.numPartitions == 0).foreach { pg =>
        while (partIter.hasNext && pg.numPartitions == 0) {
          val (_, nxt_part) = partIter.next()
          if (!initialHash.contains(nxt_part)) {
            pg.partitions += nxt_part
            initialHash += nxt_part
          }
        }
      }

      // 如果还没填满，用不带位置的分区填充
      val partNoLocIter = partitionLocs.partsWithoutLocs.iterator
      groupArr.filter(pg => pg.numPartitions == 0).foreach { pg =>
        while (partNoLocIter.hasNext && pg.numPartitions == 0) {
          val nxt_part = partNoLocIter.next()
          if (!initialHash.contains(nxt_part)) {
            pg.partitions += nxt_part
            initialHash += nxt_part
          }
        }
      }

      // 剩余未分配的分区逐个选择目标分组
      for (p <- prev.partitions if (!initialHash.contains(p))) {
        pickBin(p, prev, balanceSlack, partitionLocs).partitions += p
      }
    }
  }

  def getPartitions: Array[PartitionGroup] = groupArr.filter( pg => pg.numPartitions > 0).toArray

  /**
   * 运行分区合并算法，返回分配好的分区组数组
   * @param maxPartitions 目标分区数
   * @param prev 父RDD
   * @return 分配完成的分区组数组，每个组对应一个合并分区
   */
  def coalesce(maxPartitions: Int, prev: RDD[_]): Array[PartitionGroup] = {
    val partitionLocs = new PartitionLocations(prev)
    // 初始化目标分组（桶）
    setupGroups(math.min(prev.partitions.length, maxPartitions), partitionLocs)
    // 分配父分区（球）到目标分组
    throwBalls(maxPartitions, prev, balanceSlack, partitionLocs)
    // 过滤掉空分组，返回结果
    getPartitions
  }
}
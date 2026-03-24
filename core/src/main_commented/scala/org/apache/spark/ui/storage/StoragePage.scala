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

package org.apache.spark.ui.storage

import scala.collection.SortedMap
import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.status.{AppStatusStore, StreamBlockData}
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.ui.storage.ToolTips._
import org.apache.spark.util.Utils

/**
 * Spark存储页面，展示集群当前持久化存储的RDD和流接收块的存储信息
 * 为用户提供集群存储状态的可视化监控能力
 */
private[ui] class StoragePage(parent: SparkUITab, store: AppStatusStore) extends WebUIPage("") {

  /**
   * 渲染存储页面主内容
   * @param request HTTP请求对象
   * @return 生成的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val content = rddTable(request, store.rddList()) ++
      receiverBlockTables(store.streamBlocksList())
    UIUtils.headerSparkPage(request, "Storage", content, parent)
  }

  /**
   * 生成RDD存储信息表格HTML
   * @param request HTTP请求对象
   * @param rdds 待展示的RDD存储信息列表
   * @return RDD表格HTML节点序列
   */
  private[storage] def rddTable(
      request: HttpServletRequest,
      rdds: Seq[v1.RDDStorageInfo]): Seq[Node] = {
    if (rdds.isEmpty) {
      // 没有持久化RDD时不显示表格
      Nil
    } else {
      <div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-rdds"
            aria-expanded="true" aria-controls="aggregated-rdds"
            data-collapse-name="collapse-aggregated-rdds">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>RDDs ({rdds.length})</a>
          </h4>
        </span>
        <div class="collapsible-table collapse show" id="aggregated-rdds">
          {UIUtils.listingTable(
            rddHeader,
            rddRow(request, _: v1.RDDStorageInfo),
            rdds,
            id = Some("storage-by-rdd-table"),
            tooltipHeaders = tooltips)}
        </div>
      </div>
    }
  }

  /** RDD表格表头字段定义 */
  private val rddHeader = Seq(
    "ID",
    "RDD Name",
    "Storage Level",
    "Cached Partitions",
    "Fraction Cached",
    "Size in Memory",
    "Size on Disk")

  /** RDD表格各表头的提示信息 */
  val tooltips = Seq(
    None,
    Some(RDD_NAME),
    Some(STORAGE_LEVEL),
    Some(CACHED_PARTITIONS),
    Some(FRACTION_CACHED),
    Some(SIZE_IN_MEMORY),
    Some(SIZE_ON_DISK))

  /**
   * 渲染单个RDD对应的HTML表格行
   * @param request HTTP请求对象
   * @param rdd RDD存储信息对象
   * @return RDD行HTML节点序列
   */
  private def rddRow(request: HttpServletRequest, rdd: v1.RDDStorageInfo): Seq[Node] = {
    // scalastyle:off
    <tr>
      <td>{rdd.id}</td>
      <td>
        <a href={"%s/storage/rdd/?id=%s".format(
          UIUtils.prependBaseUri(request, parent.basePath), rdd.id)}>
          {rdd.name}
        </a>
      </td>
      <td>{rdd.storageLevel}
      </td>
      <td>{rdd.numCachedPartitions.toString}</td>
      <td>{"%.2f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}</td>
      <td sorttable_customkey={rdd.memoryUsed.toString}>{Utils.bytesToString(rdd.memoryUsed)}</td>
      <td sorttable_customkey={rdd.diskUsed.toString} >{Utils.bytesToString(rdd.diskUsed)}</td>
    </tr>
    // scalastyle:on
  }

  /**
   * 生成流接收块存储信息表格HTML
   * @param blocks 流块存储信息列表
   * @return 流块表格HTML节点序列
   */
  private[storage] def receiverBlockTables(blocks: Seq[StreamBlockData]): Seq[Node] = {
    if (blocks.isEmpty) {
      // 没有流接收块时不显示表格
      Nil
    } else {
      // 按块名称分组后排序
      val sorted = blocks.groupBy(_.name).toSeq.sortBy(_._1)

      <div>
        <h4>Receiver Blocks</h4>
        {executorMetricsTable(blocks)}
        {streamBlockTable(sorted)}
      </div>
    }
  }

  /**
   * 生成按Executor聚合的流块指标表格HTML
   * @param blocks 所有流块信息列表
   * @return 聚合指标表格HTML节点序列
   */
  private def executorMetricsTable(blocks: Seq[StreamBlockData]): Seq[Node] = {
    // 按ExecutorId分组，生成每个Executor的流存储汇总信息
    val blockManagers = SortedMap(blocks.groupBy(_.executorId).toSeq: _*)
      .map { case (id, blocks) =>
        new ExecutorStreamSummary(blocks)
      }

    <div>
      <h5>Aggregated Block Metrics by Executor</h5>
      {UIUtils.listingTable(executorMetricsTableHeader, executorMetricsTableRow, blockManagers,
        id = Some("storage-by-executor-stream-blocks"))}
    </div>
  }

  /** Executor流指标表格表头字段定义 */
  private val executorMetricsTableHeader = Seq(
    "Executor ID",
    "Address",
    "Total Size in Memory",
    "Total Size on Disk",
    "Stream Blocks")

  /**
   * 渲染单个Executor流汇总信息对应的HTML表格行
   * @param status Executor流存储汇总对象
   * @return 表格行HTML节点序列
   */
  private def executorMetricsTableRow(status: ExecutorStreamSummary): Seq[Node] = {
    <tr>
      <td>
        {status.executorId}
      </td>
      <td>
        {status.location}
      </td>
      <td sorttable_customkey={status.totalMemSize.toString}>
        {Utils.bytesToString(status.totalMemSize)}
      </td>
      <td sorttable_customkey={status.totalDiskSize.toString}>
        {Utils.bytesToString(status.totalDiskSize)}
      </td>
      <td>
        {status.numStreamBlocks.toString}
      </td>
    </tr>
  }

  /**
   * 生成单个流块信息表格HTML
   * @param blocks 按名称分组后的流块列表
   * @return 流块表格HTML节点序列
   */
  private def streamBlockTable(blocks: Seq[(String, Seq[StreamBlockData])]): Seq[Node] = {
    if (blocks.isEmpty) {
      Nil
    } else {
      <div>
        <h5>Blocks</h5>
        {UIUtils.listingTable(
          streamBlockTableHeader,
          streamBlockTableRow,
          blocks,
          id = Some("storage-by-block-table"),
          sortable = false)}
      </div>
    }
  }

  /** 流块表格表头字段定义 */
  private val streamBlockTableHeader = Seq(
    "Block ID",
    "Replication Level",
    "Location",
    "Storage Level",
    "Size")

  /**
   * 渲染单个流块分组对应的HTML表格行（处理多副本情况）
   * @param block 按名称分组后的流块元组(块名称, 副本列表)
   * @return 表格行HTML节点序列
   */
  private def streamBlockTableRow(block: (String, Seq[StreamBlockData])): Seq[Node] = {
    val replications = block._2
    assert(replications.nonEmpty) // 分组结果必然非空，此处为防御性断言
    if (replications.size == 1) {
      streamBlockTableSubrow(block._1, replications.head, replications.size, true)
    } else {
      // 多副本时生成多行，第一行合并ID和副本数单元格
      streamBlockTableSubrow(block._1, replications.head, replications.size, true) ++
        replications.tail.flatMap(streamBlockTableSubrow(block._1, _, replications.size, false))
    }
  }

  /**
   * 渲染流块单个副本对应的子表格行
   * @param blockId 流块ID
   * @param block 流块存储信息
   * @param replication 副本数
   * @param firstSubrow 是否是第一个副本行
   * @return 子行HTML节点序列
   */
  private def streamBlockTableSubrow(
      blockId: String,
      block: StreamBlockData,
      replication: Int,
      firstSubrow: Boolean): Seq[Node] = {
    val (storageLevel, size) = streamBlockStorageLevelDescriptionAndSize(block)

    <tr>
      {
        if (firstSubrow) {
          // 第一行合并跨所有副本行的单元格
          <td rowspan={replication.toString}>
            {block.name}
          </td>
          <td rowspan={replication.toString}>
            {replication.toString}
          </td>
        }
      }
      <td>{block.hostPort}</td>
      <td>{storageLevel}</td>
      <td>{Utils.bytesToString(size)}</td>
    </tr>
  }

  /**
   * 根据流块存储信息生成存储级别描述和对应大小
   * @param block 流块存储信息
   * @return (存储级别描述, 占用大小)元组
   */
  private[storage] def streamBlockStorageLevelDescriptionAndSize(
      block: StreamBlockData): (String, Long) = {
    if (block.useDisk) {
      ("Disk", block.diskSize)
    } else if (block.useMemory && block.deserialized) {
      ("Memory", block.memSize)
    } else if (block.useMemory && !block.deserialized) {
      ("Memory Serialized", block.memSize)
    } else {
      throw new IllegalStateException(s"Invalid Storage Level: ${block.storageLevel}")
    }
  }

}

/**
 * 单个Executor的流存储汇总信息容器，聚合该Executor上所有流块的存储统计
 * @param blocks 该Executor上的所有流块
 */
private class ExecutorStreamSummary(blocks: Seq[StreamBlockData]) {

  def executorId: String = blocks.head.executorId

  def location: String = blocks.head.hostPort

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def numStreamBlocks: Int = blocks.size

}
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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8

import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ExecutorSummary, RDDDataDistribution, RDDPartitionInfo}
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/**
 * RDD存储详情页面
 * 负责展示指定RDD的存储分布、缓存使用情况和分区存储详情
 * @param parent 所属的存储标签页
 * @param store 应用状态存储，用于查询RDD存储信息
 */
private[ui] class RDDPage(parent: SparkUITab, store: AppStatusStore) extends WebUIPage("rdd") {

  /**
   * 渲染RDD存储详情页面
   * @param request HTTP请求对象
   * @return 生成的HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    // 获取分区列表当前页码
    val blockPage = Option(request.getParameter("block.page")).map(_.toInt).getOrElse(1)

    val rddId = parameterId.toInt
    val rddStorageInfo = try {
      // 从状态存储查询RDD存储信息
      store.rdd(rddId)
    } catch {
      case _: NoSuchElementException =>
        // RDD不存在时返回未找到页面，不抛出异常
        return UIUtils.headerSparkPage(request, "RDD Not Found", Seq.empty[Node], parent)
    }

    // 生成执行器数据分布表格
    val workerTable = UIUtils.listingTable(workerHeader, workerRow,
      rddStorageInfo.dataDistribution.get, id = Some("rdd-storage-by-worker-table"))

    // 生成分区分页表格
    val blockTableHTML = try {
      val _blockTable = new BlockPagedTable(
        request,
        "block",
        UIUtils.prependBaseUri(request, parent.basePath) + s"/storage/rdd/?id=${rddId}",
        rddStorageInfo.partitions.get,
        store.executorList(true))
      _blockTable.table(blockPage)
    } catch {
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        <div class="alert alert-danger">{e.getMessage}</div>
    }

    // 排序后自动滚动到分区表格区域的JS脚本
    val jsForScrollingDownToBlockTable =
      <script nonce={CspNonce.get}>
        {
          Unparsed {
            """
              |$(function() {
              |  if (/.*&block.sort=.*$/.test(location.search)) {
              |    var topOffset = $("#blocks-section").offset().top;
              |    $("html,body").animate({scrollTop: topOffset}, 200);
              |  }
              |});
            """.stripMargin
          }
        }
      </script>

    // 页面主体内容组装
    val content =
      <div class="row">
        <div class="col-12">
          <ul class="list-unstyled">
            <li>
              <strong>Storage Level:</strong>
              {rddStorageInfo.storageLevel}
            </li>
            <li>
              <strong>Cached Partitions:</strong>
              {rddStorageInfo.numCachedPartitions}
            </li>
            <li>
              <strong>Total Partitions:</strong>
              {rddStorageInfo.numPartitions}
            </li>
            <li>
              <strong>Memory Size:</strong>
              {Utils.bytesToString(rddStorageInfo.memoryUsed)}
            </li>
            <li>
              <strong>Disk Size:</strong>
              {Utils.bytesToString(rddStorageInfo.diskUsed)}
            </li>
          </ul>
        </div>
      </div>

      <div class="row">
        <div class="col-12">
          <h4>
            Data Distribution on {rddStorageInfo.dataDistribution.map(_.size).getOrElse(0)}
            Executors
          </h4>
          {workerTable}
        </div>
      </div>

      <div>
        <h4 id="blocks-section">
          {rddStorageInfo.partitions.map(_.size).getOrElse(0)} Partitions
        </h4>
        {blockTableHTML ++ jsForScrollingDownToBlockTable}
      </div>;

    // 包装为标准Spark UI页面结构返回
    UIUtils.headerSparkPage(
      request, "RDD Storage Info for " + rddStorageInfo.name, content, parent)
  }

  /** 执行器分布表格表头定义 */
  private def workerHeader = Seq(
    "Host",
    "On Heap Memory Usage",
    "Off Heap Memory Usage",
    "Disk Usage")

  /**
   * 渲染执行器分布表格的一行
   * @param worker RDD在单个执行器上的数据分布信息
   * @return HTML行节点
   */
  private def workerRow(worker: RDDDataDistribution): Seq[Node] = {
    <tr>
      <td>{worker.address}</td>
      <td>
        {Utils.bytesToString(worker.onHeapMemoryUsed.getOrElse(0L))}
        ({Utils.bytesToString(worker.onHeapMemoryRemaining.getOrElse(0L))} Remaining)
      </td>
      <td>
        {Utils.bytesToString(worker.offHeapMemoryUsed.getOrElse(0L))}
        ({Utils.bytesToString(worker.offHeapMemoryRemaining.getOrElse(0L))} Remaining)
      </td>
      <td>{Utils.bytesToString(worker.diskUsed)}</td>
    </tr>
  }
}

/**
 * 分区表格行数据结构
 * @param blockName 分区块名称
 * @param storageLevel 存储级别
 * @param memoryUsed 内存使用量
 * @param diskUsed 磁盘使用量
 * @param executors 存储该分区的执行器地址列表
 */
private[ui] case class BlockTableRowData(
    blockName: String,
    storageLevel: String,
    memoryUsed: Long,
    diskUsed: Long,
    executors: String)

/**
 * 分区表格分页数据源
 * 负责对RDD分区数据进行排序和分页切分
 * @param rddPartitions RDD分区信息列表
 * @param pageSize 每页显示行数
 * @param sortColumn 排序列名
 * @param desc 是否降序排序
 * @param executorIdToAddress 执行器ID到地址的映射表
 */
private[ui] class BlockDataSource(
    rddPartitions: collection.Seq[RDDPartitionInfo],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean,
    executorIdToAddress: Map[String, String]) extends PagedDataSource[BlockTableRowData](pageSize) {

  // 将分区转换为行数据并按指定规则排序
  private val data = rddPartitions.map(blockRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): collection.Seq[BlockTableRowData] = {
    // 截取当前页需要的数据片段
    data.slice(from, to)
  }

  /** 将RDD分区信息转换为表格行数据 */
  private def blockRow(rddPartition: RDDPartitionInfo): BlockTableRowData = {
    BlockTableRowData(
      rddPartition.blockName,
      rddPartition.storageLevel,
      rddPartition.memoryUsed,
      rddPartition.diskUsed,
      rddPartition.executors
        .map { id => executorIdToAddress.getOrElse(id, id) }
        .sorted
        .mkString(" "))
  }

  /**
   * 根据排序列和排序方向生成排序器
   * @param sortColumn 排序列名
   * @param desc 是否降序
   * @return 排序器实例
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[BlockTableRowData] = {
    val ordering: Ordering[BlockTableRowData] = sortColumn match {
      case "Block Name" => Ordering.by(_.blockName)
      case "Storage Level" => Ordering.by(_.storageLevel)
      case "Size in Memory" => Ordering.by(_.memoryUsed)
      case "Size on Disk" => Ordering.by(_.diskUsed)
      case "Executors" => Ordering.by(_.executors)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}

/**
 * RDD分区分页表格
 * 负责渲染可排序、分页的RDD分区存储信息表格
 * @param request HTTP请求对象
 * @param rddTag 表格标识
 * @param basePath 基础路径
 * @param rddPartitions RDD分区信息列表
 * @param executorSummaries 执行器摘要列表
 */
private[ui] class BlockPagedTable(
    request: HttpServletRequest,
    rddTag: String,
    basePath: String,
    rddPartitions: collection.Seq[RDDPartitionInfo],
    executorSummaries: Seq[ExecutorSummary]) extends PagedTable[BlockTableRowData] {

  // 从请求中解析排序、分页参数
  private val (sortColumn, desc, pageSize) = getTableParameters(request, rddTag, "Block Name")

  override def tableId: String = "rdd-storage-by-block-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable"

  override def pageSizeFormField: String = s"$rddTag.pageSize"

  override def pageNumberFormField: String = s"$rddTag.page"

  // 初始化分页数据源
  override val dataSource: BlockDataSource = new BlockDataSource(
    rddPartitions,
    pageSize,
    sortColumn,
    desc,
    executorSummaries.map { ex => (ex.id, ex.hostPort) }.toMap)

  override def pageLink(page: Int): String = {
    // 生成指定页的链接，保留当前排序参数
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    basePath +
      s"&$pageNumberFormField=$page" +
      s"&block.sort=$encodedSortColumn" +
      s"&block.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def goButtonFormPath: String = {
    // 生成跳转按钮的表单路径，保留排序参数
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    s"$basePath&block.sort=$encodedSortColumn&block.desc=$desc"
  }

  override def headers: Seq[Node] = {
    // 定义表格表头，所有列都支持排序
    val blockHeaders: Seq[(String, Boolean, Option[String])] = Seq(
      "Block Name",
      "Storage Level",
      "Size in Memory",
      "Size on Disk",
      "Executors").map(x => (x, true, None))

    // 验证排序列有效性
    isSortColumnValid(blockHeaders, sortColumn)

    // 生成可排序表头
    headerRow(blockHeaders, desc, pageSize, sortColumn, basePath, rddTag, "block")
  }

  override def row(block: BlockTableRowData): Seq[Node] = {
    // 渲染单个分区表格行
    <tr>
      <td>{block.blockName}</td>
      <td>{block.storageLevel}</td>
      <td>{Utils.bytesToString(block.memoryUsed)}</td>
      <td>{Utils.bytesToString(block.diskUsed)}</td>
      <td>{block.executors}</td>
    </tr>
  }
}
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

package org.apache.spark.ui

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._
import scala.xml.{Node, Unparsed}

import com.google.common.base.Splitter
import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.util.Utils

/**
 * 分页表格数据源抽象基类，为Spark WebUI分页表格提供数据切片能力
 *
 * @param pageSize 每页展示的行数
 * @tparam T 表格行数据类型
 */
private[spark] abstract class PagedDataSource[T](val pageSize: Int) {

  /**
   * 获取所有数据的总条数
   */
  protected def dataSize: Int

  /**
   * 切片获取指定范围的数据
   *
   * @param from 起始索引（包含）
   * @param to 结束索引（不包含）
   */
  protected def sliceData(from: Int, to: Int): collection.Seq[T]

  /**
   * 获取指定页码对应的分页数据
   *
   * @param page 目标页码
   * @return 分页结果，包含总页数和当前页数据
   */
  def pageData(page: Int): PageData[T] = {
    // 当pageSize<=0时，将所有数据展示在同一页
    val pageTableSize = if (pageSize <= 0) {
      dataSize
    } else {
      pageSize
    }
    val totalPages = (dataSize + pageTableSize - 1) / pageTableSize

    val pageToShow = if (page <= 0) {
      1
    } else if (page > totalPages) {
      totalPages
    } else {
      page
    }

    val (from, to) = ((pageToShow - 1) * pageSize, dataSize.min(pageToShow * pageTableSize))

    PageData(totalPages, sliceData(from, to))
  }

}

/**
 * PagedDataSource返回的分页数据封装，包含总页数和当前页数据
 *
 * @param totalPage 总页数
 * @param data 当前页数据集合
 * @tparam T 表格行数据类型
 */
private[ui] case class PageData[T](totalPage: Int, data: collection.Seq[T])

/**
 * Spark WebUI分页表格抽象特征，用于生成分页HTML表格和分页导航控件
 *
 * @tparam T 表格行数据类型
 */
private[spark] trait PagedTable[T] {

  def tableId: String

  def tableCssClass: String

  def pageSizeFormField: String

  def pageNumberFormField: String

  def dataSource: PagedDataSource[T]

  def headers: Seq[Node]

  def row(t: T): Seq[Node]

  /**
   * 生成指定页码的完整分页表格HTML（包含上下分页导航）
   *
   * @param page 目标页码
   * @return 完整表格HTML节点序列
   */
  def table(page: Int): Seq[Node] = {
    val _dataSource = dataSource
    try {
      val PageData(totalPages, data) = _dataSource.pageData(page)

      // 修正页码范围，确保在合法区间内
      val pageToShow = if (page <= 0) {
        1
      } else if (page > totalPages) {
        totalPages
      } else {
        page
      }
      // pageSize<=0时显示全部数据，此时每页大小为数据总条数
      val pageSize = if (_dataSource.pageSize <= 0) {
        data.size
      } else {
        _dataSource.pageSize
      }

      // 生成顶部和底部分页导航
      val pageNaviTop = pageNavigation(pageToShow, pageSize, totalPages, tableId + "-top")
      val pageNaviBottom = pageNavigation(pageToShow, pageSize, totalPages, tableId + "-bottom")

      <div>
        {pageNaviTop}
        <table class={tableCssClass} id={tableId}>
          {headers}
          <tbody>
            {data.map(row)}
          </tbody>
        </table>
        {pageNaviBottom}
      </div>
    } catch {
      // 页码越界异常处理，返回第一页并展示错误信息
      case e: IndexOutOfBoundsException =>
        val PageData(totalPages, _) = _dataSource.pageData(1)
        <div>
          {pageNavigation(1, _dataSource.pageSize, totalPages)}
          <div class="alert alert-danger">
            <p>Error while rendering table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>
        </div>
    }
  }

  /**
   * 生成分页导航HTML，包含页码组、前后翻页和页码跳转表单
   *
   * @param page 当前页码
   * @param pageSize 每页行数
   * @param totalPages 总页数
   * @param navigationId 导航控件ID
   * @return 分页导航HTML节点序列
   */
  private[ui] def pageNavigation(
      page: Int,
      pageSize: Int,
      totalPages: Int,
      navigationId: String = tableId): Seq[Node] = {
    // 分页导航每页显示10个页码按钮
    val groupSize = 10
    val firstGroup = 0
    val lastGroup = (totalPages - 1) / groupSize
    val currentGroup = (page - 1) / groupSize
    val startPage = currentGroup * groupSize + 1
    val endPage = totalPages.min(startPage + groupSize - 1)
    // 生成当前页码组内的所有页码按钮
    val pageTags = (startPage to endPage).map { p =>
      if (p == page) {
        // 当前页禁用点击
        <li class="page-item disabled"><a href="#" class="page-link">{p}</a></li>
      } else {
        <li class="page-item"><a href={Unparsed(pageLink(p))} class="page-link">{p}</a></li>
      }
    }

    // 提取保留的其他查询参数，转为隐藏表单域
    val hiddenFormFields = {
      if (goButtonFormPath.contains('?')) {
        val queryString = goButtonFormPath.split("\\?", 2)(1)
        val search = queryString.split("#")(0)
        Splitter
          .on('&')
          .trimResults()
          .omitEmptyStrings()
          .withKeyValueSeparator("=")
          .split(search)
          .asScala
          .filter { case (k, _) => k != pageSizeFormField}
          .filter { case (k, _) => k != pageNumberFormField}
          .map { case (k, v) => (k, URLDecoder.decode(v, UTF_8.name())) }
          .map { case (k, v) =>
            <input type="hidden" name={k} value={v} />
          }
      } else {
        Seq.empty
      }
    }

    <div class="d-flex justify-content-between align-items-center">
      <div class="d-flex align-items-center">
        <span class="pe-1">Page: </span>
        <ul class="pagination mb-0">
          {// 不在第一组时显示跳转到上一组按钮
          if (currentGroup > firstGroup) {
          <li class="page-item">
            <a href={Unparsed(pageLink(startPage - groupSize))} class="page-link"
               aria-label="Previous Group">
              <span aria-hidden="true">
                &lt;&lt;
              </span>
            </a>
          </li>
          }}
          {// 不是第一页时显示跳转到上一页按钮
          if (page > 1) {
          <li class="page-item">
          <a href={Unparsed(pageLink(page - 1))} class="page-link" aria-label="Previous">
            <span aria-hidden="true">
              &lt;
            </span>
          </a>
          </li>
          }}
          {pageTags}
          {// 不是最后一页时显示跳转到下一页按钮
          if (page < totalPages) {
          <li class="page-item">
            <a href={Unparsed(pageLink(page + 1))} class="page-link" aria-label="Next">
              <span aria-hidden="true">&gt;</span>
            </a>
          </li>
          }}
          {// 不在最后一组时显示跳转到下一组按钮
          if (currentGroup < lastGroup) {
          <li class="page-item">
            <a href={Unparsed(pageLink(startPage + groupSize))} class="page-link"
               aria-label="Next Group">
              <span aria-hidden="true">
                &gt;&gt;
              </span>
            </a>
          </li>
        }}
        </ul>
      </div>
      <div>
        <form id={s"form-$navigationId-page"}
              method="get"
              action={Unparsed(goButtonFormPath)}
              class="d-flex align-items-center gap-1 mb-0">
          {hiddenFormFields}
          <label class="text-nowrap">{totalPages} Pages. Jump to</label>
          <input type="text"
                 name={pageNumberFormField}
                 id={s"form-$navigationId-page-no"}
                 value={page.toString}
                 class="form-control form-control-sm"
                 style="width: 60px;" />

          <label class="text-nowrap">. Show </label>
          <input type="text"
                 id={s"form-$navigationId-page-size"}
                 name={pageSizeFormField}
                 value={pageSize.toString}
                 class="form-control form-control-sm"
                 style="width: 60px;" />
          <label class="text-nowrap">items in a page.</label>

          <button type="submit" class="btn btn-outline-secondary btn-sm">Go</button>
        </form>
      </div>
    </div>
  }

  /**
   * 生成跳转到指定页码的链接地址
   *
   * @param page 目标页码
   * @return 跳转链接字符串
   */
  def pageLink(page: Int): String

  /**
   * 获取页码跳转表单的提交路径
   *
   * @return 表单提交路径字符串
   */
  def goButtonFormPath: String

  /**
   * 提取当前页面中不属于当前表格的其他查询参数，用于拼接跳转链接
   *
   * @param request HTTP请求对象
   * @param tableTag 当前表格标签
   * @return 拼接后的其他参数字符串
   */
  def getParameterOtherTable(request: HttpServletRequest, tableTag: String): String = {
    request.getParameterMap.asScala
      .filterNot(_._1.startsWith(tableTag))
      .map(parameter => parameter._1 + "=" + parameter._2(0))
      .mkString("&")
  }

  /**
   * 从HTTP请求中提取当前表格的排序和分页参数
   *
   * @param request HTTP请求对象
   * @param tableTag 当前表格标签
   * @param defaultSortColumn 默认排序列名
   * @return (排序列名, 是否降序, 每页行数)元组
   */
  def getTableParameters(
      request: HttpServletRequest,
      tableTag: String,
      defaultSortColumn: String): (String, Boolean, Int) = {
    val parameterSortColumn = request.getParameter(s"$tableTag.sort")
    val parameterSortDesc = request.getParameter(s"$tableTag.desc")
    val parameterPageSize = request.getParameter(s"$tableTag.pageSize")
    // 解码URL参数，无参数时使用默认排序列
    val sortColumn = Option(parameterSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse(defaultSortColumn)
    // 无排序方向参数时，默认排序列使用降序，其他列使用升序
    val desc = Option(parameterSortDesc).map(_.toBoolean).getOrElse(
      sortColumn == defaultSortColumn
    )
    // 无页大小参数时默认每页100条
    val pageSize = Option(parameterPageSize).map(_.toInt).getOrElse(100)

    (sortColumn, desc, pageSize)
  }

  /**
   * 验证排序参数中指定的列名是否合法，非法则抛出异常
   *
   * @param headerInfo 表头信息序列
   * @param sortColumn 待验证排序列名
   */
  def isSortColumnValid(
      headerInfo: Seq[(String, Boolean, Option[String])],
      sortColumn: String): Unit = {
    if (!headerInfo.filter(_._2).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }
  }

  /**
   * 生成排序表头HTML，包含排序链接和方向箭头
   *
   * @param headerInfo 表头信息序列，每个元素为(列名, 是否可排序, 提示信息)
   * @param desc 当前是否降序排序
   * @param pageSize 每页行数
   * @param sortColumn 当前排序列名
   * @param parameterPath 跳转链接参数路径
   * @param tableTag 当前表格标签
   * @param headerId 表头ID
   * @return 表头HTML节点序列
   */
  def headerRow(
      headerInfo: Seq[(String, Boolean, Option[String])],
      desc: Boolean,
      pageSize: Int,
      sortColumn: String,
      parameterPath: String,
      tableTag: String,
      headerId: String): Seq[Node] = {
    val row: Seq[Node] = {
      headerInfo.map { case (header, sortable, tooltip) =>
        if (header == sortColumn) {
          // 当前排序列，生成反转排序方向的链接
          val headerLink = Unparsed(
            parameterPath +
              s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$tableTag.desc=${!desc}" +
              s"&$tableTag.pageSize=$pageSize" +
              s"#$headerId")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // 降序显示下箭头，升序显示上箭头

          <th>
            <a href={headerLink}>
              {UIUtils.tooltipSpan(
                <xml:group>{header}&nbsp;{Unparsed(arrow)}</xml:group>,
                tooltip.getOrElse(""))}
            </a>
          </th>
        } else {
          if (sortable) {
            // 可排序列，生成点击排序链接
            val headerLink = Unparsed(
              parameterPath +
                s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
                s"&$tableTag.pageSize=$pageSize" +
                s"#$headerId")

            <th>
              <a href={headerLink}>
                {UIUtils.tooltipSpan(<xml:group>{header}</xml:group>,
                  tooltip.getOrElse(""))}
              </a>
            </th>
          } else {
            // 不可排序列，仅展示文本
            <th>
              {UIUtils.tooltipSpan(<xml:group>{header}</xml:group>,
                tooltip.getOrElse(""))}
            </th>
          }
        }
      }
    }
    <thead>
      <tr>{row}</tr>
    </thead>
  }
}
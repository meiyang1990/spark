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

import java.{util => ju}
import java.lang.{Long => JLong}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.xml.{Node, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.ui.UIUtils.formatImportJavaScript

/**
 * 文件说明: Spark Web UI 图形数据生成工具类，为时间线图和直方图提供HTML和JavaScript生成能力
 * 用于在Spark监控页面展示流式任务等时间序列数据的可视化
 *
 * 一个辅助类，为时间线和直方图生成所需的JavaScript和HTML代码
 *
 * @param timelineDivId 时间线容器div的HTML id属性
 * @param histogramDivId 直方图容器div的HTML id属性
 * @param data 绘图所需的(x,y)数据点序列
 * @param minX X轴最小值
 * @param maxX X轴最大值
 * @param minY Y轴最小值
 * @param maxY Y轴最大值
 * @param unitY Y轴单位
 * @param batchInterval 如果不为None，会在图中绘制批次间隔参考线
 */
private[spark] class GraphUIData(
    timelineDivId: String,
    histogramDivId: String,
    data: Seq[(Long, Double)],
    minX: Long,
    maxX: Long,
    minY: Double,
    maxY: Double,
    unitY: String,
    batchInterval: Option[Double] = None) {

  private var dataJavaScriptName: String = _

  /**
   * 生成绘图数据的JavaScript变量定义，添加到JsCollector中
   * @param jsCollector JavaScript代码收集器
   */
  def generateDataJs(jsCollector: JsCollector): Unit = {
    val jsForData = data.map { case (x, y) =>
      s"""{"x": $x, "y": $y}"""
    }.mkString("[", ",", "]")
    dataJavaScriptName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $dataJavaScriptName = $jsForData;")
  }

  /**
   * 生成时间线图所需的HTML容器并注册前端绘图代码
   * @param jsCollector JavaScript代码收集器
   * @return 时间线容器HTML节点序列
   */
  def generateTimelineHtml(jsCollector: JsCollector): Seq[Node] = {
    jsCollector.addImports("/static/streaming-page.js", "registerTimeline")
    jsCollector.addPreparedStatement(s"registerTimeline($minY, $maxY);")
    jsCollector.addImports("/static/streaming-page.js", "drawTimeline")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawTimeline(" +
          s"'#$timelineDivId', $dataJavaScriptName, $minX, $maxX, $minY, $maxY, '$unitY'," +
          s" ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawTimeline('#$timelineDivId', $dataJavaScriptName, $minX, $maxX, $minY, $maxY," +
          s" '$unitY');")
    }
    <div id={timelineDivId}></div>
  }

  /**
   * 生成直方图所需的HTML容器并注册前端绘图代码
   * @param jsCollector JavaScript代码收集器
   * @return 直方图容器HTML节点序列
   */
  def generateHistogramHtml(jsCollector: JsCollector): Seq[Node] = {
    val histogramData = s"$dataJavaScriptName.map(function(d) { return d.y; })"
    jsCollector.addImports("/static/streaming-page.js", "registerHistogram")
    jsCollector.addPreparedStatement(s"registerHistogram($histogramData, $minY, $maxY);")
    jsCollector.addImports("/static/streaming-page.js", "drawHistogram")
    if (batchInterval.isDefined) {
      jsCollector.addStatement(
        "drawHistogram(" +
          s"'#$histogramDivId', $histogramData, $minY, $maxY, '$unitY', ${batchInterval.get}" +
          ");")
    } else {
      jsCollector.addStatement(
        s"drawHistogram('#$histogramDivId', $histogramData, $minY, $maxY, '$unitY');")
    }
    <div id={histogramDivId}></div>
  }

  /**
   * 生成堆积面积图所需HTML和JavaScript数据，用于结构化流统计数据可视化
   * @param jsCollector JavaScript代码收集器
   * @param values 按批次时间分组的不同操作类型数据，格式为(批次时间, Map(操作标签 -> 耗时))
   * @return 堆积面积图容器HTML节点序列
   */
  def generateAreaStackHtmlWithData(
      jsCollector: JsCollector,
      values: Array[(Long, ju.Map[String, JLong])]): Seq[Node] = {
    // 提取所有操作类型标签
    val operationLabels = values.flatMap(_._2.keySet().asScala).toSet
    // 补全缺失批次数据，保证时间轴连续
    val durationDataPadding = UIUtils.durationDataPadding(values)
    // 格式化数据为JavaScript数组格式
    val jsForData = durationDataPadding.map { case (x, y) =>
      val s = y.toSeq.sortBy(_._1).map(e => s""""${e._1}": "${e._2}"""").mkString(",")
      s"""{x: "${UIUtils.formatBatchTime(x, 1, showYYYYMMSS = false)}", $s}"""
    }.mkString("[", ",", "]")
    // 格式化操作标签为JavaScript数组格式
    val jsForLabels = operationLabels.toSeq.sorted.mkString("[\"", "\",\"", "\"]")

    // 生成数据变量名并添加到收集器
    dataJavaScriptName = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $dataJavaScriptName = $jsForData;")
    val labels = jsCollector.nextVariableName
    jsCollector.addPreparedStatement(s"var $labels = $jsForLabels;")
    // 导入绘图函数并注册绘图语句
    jsCollector.addImports("/static/structured-streaming-page.js", "drawAreaStack")
    jsCollector.addStatement(
      s"drawAreaStack('#$timelineDivId', $labels, $dataJavaScriptName)")
    // 返回容器div
    <div id={timelineDivId}></div>
  }
}

/**
 * JavaScript代码收集器，用于收集页面渲染所需的JavaScript代码，在DOM加载完成后统一执行
 * 负责管理JavaScript变量命名、脚本导入和语句排序，保证前端代码正确执行
 */
private[spark] class JsCollector(req: HttpServletRequest) {

  private var variableId = 0

  /**
   * 获取下一个未使用的JavaScript变量名，自动编号避免命名冲突
   * @return 自动生成的变量名，格式为v+数字
   */
  def nextVariableName: String = {
    variableId += 1
    "v" + variableId
  }

  /**
   * 预处理语句集合，在主语句之前执行
   */
  private val preparedStatements = ArrayBuffer[String]()

  /**
   * 主语句集合，在预处理语句之后执行
   */
  private val statements = ArrayBuffer[String]()

  private val imports = mutable.Set[String]()

  /**
   * 添加预处理JavaScript语句，在绘图逻辑之前执行
   * @param js 要添加的JavaScript代码
   */
  def addPreparedStatement(js: String): Unit = {
    preparedStatements += js
  }

  /**
   * 添加主JavaScript语句，在预处理语句之后执行
   * @param js 要添加的JavaScript代码
   */
  def addStatement(js: String): Unit = {
    statements += js
  }

  /**
   * 导入指定JS文件中的指定函数
   * @param sourceFile JS文件路径
   * @param functions 要导入的函数名称列表
   */
  def addImports(sourceFile: String, functions: String*): Unit = {
    imports.add(formatImportJavaScript(req, sourceFile, functions: _*))
  }

  /**
   * 添加已格式化好的JavaScript导入语句
   * @param js 导入语句代码
   */
  def addImports(js: String): Unit = {
    imports.add(js)
  }

  /**
   * 生成最终可插入HTML的script标签，所有收集到的代码在DOM加载完成后执行
   * @return 包含所有JavaScript代码的HTML script节点
   */
  def toHtml: Seq[Node] = {
    val js =
      s"""
         |${imports.mkString("\n")}
         |
         |$$(document).ready(function() {
         |    ${preparedStatements.mkString("\n")}
         |    ${statements.mkString("\n")}
         |});""".stripMargin

    <script type="module" nonce={CspNonce.get}>{Unparsed(js)}</script>
  }
}
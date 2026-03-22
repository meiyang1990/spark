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

package org.apache.spark.deploy.master.ui

import scala.jdk.CollectionConverters._
import scala.xml.Node

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.UI.MASTER_UI_VISIBLE_ENV_VAR_PREFIXES
import org.apache.spark.ui._
import org.apache.spark.util.Utils

/**
 * Spark Master WebUI 环境信息页面，用于展示集群运行环境的各类配置信息
 * 包括JVM信息、Spark配置、Hadoop配置、系统属性、类路径和环境变量等内容
 */
private[ui] class EnvironmentPage(
    parent: MasterWebUI,
    conf: SparkConf) extends WebUIPage("Environment") {

  /**
   * 渲染环境信息页面，生成完整的HTML内容返回给前端
   * @param request HTTP请求对象
   * @return 生成的页面HTML节点序列
   */
  def render(request: HttpServletRequest): Seq[Node] = {
    // 获取完整的环境详细信息
    val details = SparkEnv.environmentDetails(conf, SparkHadoopUtil.get.newConfiguration(conf),
      "", Seq.empty, Seq.empty, Seq.empty, Map.empty)
    val jvmInformation = details("JVM Information").sorted
    // 对敏感配置信息做脱敏处理
    val sparkProperties = Utils.redact(conf, details("Spark Properties")).sorted
    val hadoopProperties = Utils.redact(conf, details("Hadoop Properties")).sorted
    val systemProperties = Utils.redact(conf, details("System Properties")).sorted
    val metricsProperties = Utils.redact(conf, details("Metrics Properties")).sorted
    val classpathEntries = details("Classpath Entries").sorted
    // 获取配置允许展示的环境变量前缀
    val prefixes = conf.get(MASTER_UI_VISIBLE_ENV_VAR_PREFIXES)
    // 过滤出符合前缀要求的环境变量并排序
    val environmentVariables = System.getenv().asScala
      .filter { case (k, _) => prefixes.exists(k.startsWith(_)) }.toSeq.sorted

    // 为各类信息生成可折叠表格HTML
    val runtimeInformationTable = UIUtils.listingTable(propertyHeader, propertyRow,
      jvmInformation, fixedWidth = true, headerClasses = headerClasses)
    val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      sparkProperties, fixedWidth = true, headerClasses = headerClasses)
    val hadoopPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      hadoopProperties, fixedWidth = true, headerClasses = headerClasses)
    val systemPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      systemProperties, fixedWidth = true, headerClasses = headerClasses)
    val metricsPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      metricsProperties, fixedWidth = true, headerClasses = headerClasses)
    val classpathEntriesTable = UIUtils.listingTable(classPathHeader, classPathRow,
      classpathEntries, fixedWidth = true, headerClasses = headerClasses)
    val environmentVariablesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      environmentVariables, fixedWidth = true, headerClasses = headerClasses)

    // 组装完整页面内容，使用可折叠分区展示不同类别信息
    val content =
      <div>
        <p><a href="/">Back to Master</a></p>
      </div>
      <span>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-runtimeInformation"
            aria-expanded="true" aria-controls="aggregated-runtimeInformation"
            data-collapse-name="collapse-aggregated-runtimeInformation">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Runtime Information</a>
          </h4>
        </span>
        <div class="collapsible-table collapse show" id="aggregated-runtimeInformation">
          {runtimeInformationTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-sparkProperties"
            aria-expanded="true" aria-controls="aggregated-sparkProperties"
            data-collapse-name="collapse-aggregated-sparkProperties">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Spark Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse show" id="aggregated-sparkProperties">
          {sparkPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-hadoopProperties"
            aria-expanded="false" aria-controls="aggregated-hadoopProperties"
            data-collapse-name="collapse-aggregated-hadoopProperties">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Hadoop Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-hadoopProperties">
          {hadoopPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-systemProperties"
            aria-expanded="false" aria-controls="aggregated-systemProperties"
            data-collapse-name="collapse-aggregated-systemProperties">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>System Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-systemProperties">
          {systemPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-metricsProperties"
            aria-expanded="false" aria-controls="aggregated-metricsProperties"
            data-collapse-name="collapse-aggregated-metricsProperties">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Metrics Properties</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-metricsProperties">
          {metricsPropertiesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-classpathEntries"
            aria-expanded="false" aria-controls="aggregated-classpathEntries"
            data-collapse-name="collapse-aggregated-classpathEntries">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Classpath Entries</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-classpathEntries">
          {classpathEntriesTable}
        </div>
        <span class="collapse-table" data-bs-toggle="collapse"
            data-bs-target="#aggregated-environmentVariables"
            aria-expanded="false" aria-controls="aggregated-environmentVariables"
            data-collapse-name="collapse-aggregated-environmentVariables">
          <h4>
            <span class="collapse-table-arrow arrow-closed"></span>
            <a>Environment Variables</a>
          </h4>
        </span>
        <div class="collapsible-table collapse" id="aggregated-environmentVariables">
          {environmentVariablesTable}
        </div>
        <script src={UIUtils.prependBaseUri(request, "/static/environmentpage.js")}></script>
      </span>
    // 包装成标准Spark UI页面结构返回
    UIUtils.basicSparkPage(request, content, "Environment")
  }

  // 键值对属性表格表头
  private def propertyHeader = Seq("Name", "Value")
  // 类路径表格表头
  private def classPathHeader = Seq("Resource", "Source")
  // 表头排序样式类
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def headerClassesNoSortValues = Seq("sorttable_numeric", "sorttable_nosort")

  // JVM信息行渲染，使用pre标签保持格式
  private def jvmRowDataPre(kv: (String, String)) =
    <tr><td>{kv._1}</td><td><pre>{kv._2}</pre></td></tr>
  // 通用键值对行渲染
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  // 类路径行渲染
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}
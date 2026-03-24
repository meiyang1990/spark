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

import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._

/**
 * Spark Web UI 存储监控标签页，负责展示Spark应用中所有RDD的存储状态信息
 *
 * 核心职责：在Spark Web UI中注册存储监控标签页，并挂载RDD存储列表页和单个RDD详情页
 *
 * @param parent 所属的父级SparkUI实例
 * @param store 应用状态存储，用于读取RDD存储状态数据
 */
private[ui] class StorageTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "storage") {

  // 挂载RDD存储列表页面
  attachPage(new StoragePage(this, store))
  // 挂载单个RDD存储详情页面
  attachPage(new RDDPage(this, store))
}
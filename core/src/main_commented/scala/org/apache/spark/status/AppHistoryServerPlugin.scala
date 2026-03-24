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

package org.apache.spark.status

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.ui.SparkUI

/**
 * 文件: AppHistoryServerPlugin.scala
 * 所属模块: Spark core 核心模块
 * 核心职责: 定义应用历史服务器插件扩展接口，允许其他模块（如Spark SQL）自定义历史事件解析和UI展示能力
 * 设计目的: 实现历史服务器功能的模块化扩展，核心模块不需要依赖上层模块即可加载自定义插件
 */

/**
 * 应用历史服务器扩展插件接口
 * 核心职责: 允许非core模块自定义事件日志回放逻辑和历史UI界面，用于在历史服务器中展示模块特有信息
 * 使用场景: 供SQL、Streaming等上层模块实现自己的历史信息展示功能，无需修改核心历史服务器代码
 */
private[spark] trait AppHistoryServerPlugin {
  /**
   * 创建用于回放事件日志的Spark监听器列表
   * @param conf Spark配置对象
   * @param store 历史数据跟踪存储，用于保存解析后的事件数据
   * @return 监听器列表，用于事件日志回放时处理对应事件
   */
  def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener]

  /**
   * 在历史服务器UI中初始化插件对应的界面
   * @param ui 历史服务器SparkUI实例，插件可向其添加自定义标签页和页面
   */
  def setupUI(ui: SparkUI): Unit

  /**
   * 插件标签页在历史UI中的展示排序，数值越小排序越靠前
   * @return 排序值，默认排在所有自定义插件最后
   */
  def displayOrder: Int = Integer.MAX_VALUE
}
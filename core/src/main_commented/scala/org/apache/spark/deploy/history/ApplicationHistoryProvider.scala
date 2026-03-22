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

package org.apache.spark.deploy.history

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.zip.ZipOutputStream

import scala.xml.Node

import org.apache.spark.SparkException
import org.apache.spark.status.api.v1.ApplicationInfo
import org.apache.spark.ui.SparkUI

/**
 * 文件概述：历史服务器应用查询提供者抽象基类，定义了历史服务端获取已完成应用信息的核心接口规范
 * 所属模块：Spark Core -> 部署模块 -> 历史服务器子模块
 * 核心职责：定义不同存储后端（文件系统、远程存储等）实现应用历史数据查询的统一接口，支持可扩展的应用历史存储方案
 */

/**
 * 已加载到内存的Spark应用UI包装类
 * 用于管理历史服务器中已缓存应用UI的生命周期和有效性状态，配合应用缓存实现动态更新
 *
 * 工作机制：当底层应用事件日志发生变更时，提供者会将当前UI标记为无效，不立即卸载；
 * 缓存检测到无效后会主动淘汰并触发清理，保证读写并发安全
 * 
 * @param ui 加载完成的Spark UI实例
 */
private[history] case class LoadedAppUI(ui: SparkUI) {

  // 读写锁，保证并发访问UI时的线程安全
  val lock = new ReentrantReadWriteLock()

  @volatile private var _valid = true

  def valid: Boolean = _valid

  /**
   * 将当前UI标记为无效，等待缓存淘汰
   */
  def invalidate(): Unit = {
    lock.writeLock().lock()
    try {
      _valid = false
    } finally {
      lock.writeLock().unlock()
    }
  }

}

/**
 * 应用历史数据提供者抽象基类
 * 定义了历史服务器访问、查询、加载已完成Spark应用历史数据的核心接口，不同存储后端需要实现该接口
 * 核心职责：提供应用列表查询、应用UI加载、事件日志导出等能力，对接不同存储系统（如本地日志目录、云存储等）
 */
private[history] abstract class ApplicationHistoryProvider {

  /**
   * 获取当前正在处理中的应用事件日志数量
   * 用于历史服务器首页提示用户，还有部分应用正在解析待展示
   * 
   * @return 正在处理的事件日志数量，不支持该统计时返回0
   */
  def getEventLogsUnderProcess(): Int = {
    0
  }

  /**
   * 获取应用历史信息最后一次更新时间戳
   * 
   * @return 最后更新时间（毫秒），不支持时返回0
   */
  def getLastUpdatedTime(): Long = {
    0
  }

  /**
   * 获取所有已知可展示的应用信息列表
   * 
   * @return 所有已完成应用信息迭代器
   */
  def getListing(): Iterator[ApplicationInfo]

  /**
   * 分页条件查询应用信息列表
   * 
   * @param max 返回结果最大数量
   * @param predicate 应用过滤条件函数
   * @return 符合条件的应用信息迭代器，最多返回max条
   */
  def getListing(max: Int)(predicate: ApplicationInfo => Boolean): Iterator[ApplicationInfo]

  /**
   * 根据应用ID和尝试ID加载对应应用的UI实例
   * 
   * @param appId 应用ID
   * @param attemptId 应用尝试ID，无尝试ID时为None
   * @return 包装后的已加载UI实例，未找到对应应用返回None
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI]

  /**
   * 历史服务器关闭时调用，用于清理资源
   */
  def stop(): Unit = { }

  /**
   * 历史服务器启动时调用，用于初始化提供者和启动后台线程
   * 支持提供者创建完成后延迟初始化
   */
  def start(): Unit = { }

  /**
   * 获取需要在历史服务器首页展示的配置信息
   * 
   * @return 配置键值对，页面按照返回顺序展示
   */
  def getConfig(): Map[String, String] = Map()

  /**
   * 将指定应用的事件日志压缩写入ZIP输出流，供用户下载
   * 
   * @param appId 应用ID
   * @param attemptId 应用尝试ID
   * @param zipStream 目标ZIP输出流
   * @throws SparkException 找不到对应应用日志时抛出异常
   */
  @throws(classOf[SparkException])
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit

  /**
   * 根据应用ID查询应用基础信息
   * 
   * @param appId 应用ID
   * @return 应用信息，不存在则返回None
   */
  def getApplicationInfo(appId: String): Option[ApplicationInfo]

  /**
   * 获取应用列表为空时页面展示的HTML内容
   * 
   * @return HTML节点序列，无自定义内容返回空序列
   */
  def getEmptyListingHtml(): Seq[Node] = Seq.empty

  /**
   * 应用UI从历史服务器缓存卸载时触发的回调，用于提供者清理资源
   * 
   * @param appId 应用ID
   * @param attemptId 应用尝试ID
   * @param ui 即将卸载的UI实例
   */
  def onUIDetached(appId: String, attemptId: Option[String], ui: SparkUI): Unit = { }

  /**
   * 检查当前用户是否有权限查看指定应用尝试的UI
   * 
   * @param appId 应用ID
   * @param attemptId 应用尝试ID
   * @param user 用户名
   * @return 有权限返回true，否则false
   * @throws NoSuchElementException 指定应用尝试不存在时抛出异常
   */
  def checkUIViewPermissions(appId: String, attemptId: Option[String], user: String): Boolean

}
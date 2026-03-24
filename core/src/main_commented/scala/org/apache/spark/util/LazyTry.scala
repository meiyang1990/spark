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

package org.apache.spark.util

import scala.util.Try

/**
 * 文件概述：延迟初始化工具类，修复Scala原生lazy val存在的重试初始化和锁粒度问题
 * 
 * 修复Scala原生lazy val的两个核心问题：
 * 1. 初始化失败不重试：Scala原生lazy val初始化失败后仍会重复尝试初始化，本类会缓存异常直接重抛
 * 2. 锁粒度隔离：本类仅对自身加锁，不会对外部对象加锁，避免父对象锁竞争导致的死锁问题
 * 
 * @param initialize 延迟初始化需要执行的代码块
 * @tparam T 延迟初始化值的类型
 */
private[spark] class LazyTry[T](initialize: => T) extends Serializable {
  // 缓存初始化结果，将异常包装在Try中，仅初始化一次
  private lazy val tryT: Try[T] = Utils.doTryWithCallerStacktrace { initialize }

  // 标记延迟值是否已经被访问过（已初始化完成）
  @volatile private var materialized: Boolean = false

  /**
   * 获取延迟初始化的值，若初始化失败则重抛原始异常
   * 会保留当前调用者栈轨迹，便于调试定位问题
   * 
   * 首次访问不添加抑制异常，后续访问将原始栈轨迹添加为抑制异常辅助调试
   * 
   * @return 初始化完成的值
   * @throws 初始化阶段抛出的异常，会保留原始栈信息
   */
  def get: T = {
    val isFirstAccess = !materialized
    materialized = true
    Utils.getTryWithCallerStacktrace(tryT, isFirstAccess)
  }
}

/**
 * LazyTry的伴随对象，提供便捷的实例创建方法
 */
private[spark] object LazyTry {
  /**
   * 创建一个新的LazyTry实例
   * 
   * @param initialize 延迟初始化代码块
   * @tparam T 延迟值类型
   * @return 创建完成的LazyTry实例
   */
  def apply[T](initialize: => T): LazyTry[T] = new LazyTry(initialize)
}
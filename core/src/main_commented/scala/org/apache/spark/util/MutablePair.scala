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

import org.apache.spark.annotation.DeveloperApi

/**
 * 文件: core/src/main/scala/org/apache/spark/util/MutablePair.scala
 * 模块: Spark核心公共工具模块
 * 职责: 提供可修改的二元组实现，用于减少对象分配优化性能
 *
 * :: DeveloperApi ::
 * 可修改的二元组，相比Scala原生不可变Tuple2，支持原地修改值，用于减少对象分配提升性能，适合频繁更新键值对的场景。
 *
 * @param  _1   二元组第一个元素
 * @param  _2   二元组第二个元素
 */
@DeveloperApi
case class MutablePair[@specialized(Int, Long, Double, Char, Boolean/* , AnyRef */) T1,
                       @specialized(Int, Long, Double, Char, Boolean/* , AnyRef */) T2]
  (var _1: T1, var _2: T2)
  extends Product2[T1, T2]
{
  /** 无参构造器，用于Java序列化机制 */
  def this() = this(null.asInstanceOf[T1], null.asInstanceOf[T2])

  /**
   * 更新二元组的值，返回当前对象本身支持链式调用
   * @param n1 新的第一个元素值
   * @param n2 新的第二个元素值
   * @return 更新后的当前MutablePair对象
   */
  def update(n1: T1, n2: T2): MutablePair[T1, T2] = {
    _1 = n1
    _2 = n2
    this
  }

  override def toString: String = "(" + _1 + "," + _2 + ")"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[MutablePair[_, _]]
}
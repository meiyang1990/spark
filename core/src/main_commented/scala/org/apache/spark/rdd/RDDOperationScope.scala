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

package org.apache.spark.rdd

import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
 * 文件概述: RDD操作作用域定义，用于跟踪记录RDD的创建操作链，支持可视化展示RDD谱系
 * 
 * 表示实例化RDD的命名代码块操作作用域。所有在该代码块内创建的RDD都会持有该对象的引用，
 * 用于记录RDD生成的操作来源，支持嵌套层级结构，可用于Spark UI展示RDD操作谱系。
 * 作用域与Spark的Stage/Job没有强制对应关系，一个作用域可以位于单个Stage内，也可以跨多个Job。
 *
 * @param name 操作作用域名称，通常对应创建RDD的方法名
 * @param parent 父级作用域，用于构建嵌套层级结构
 * @param id 全局唯一作用域ID
 */
@JsonInclude(Include.NON_ABSENT)
@JsonPropertyOrder(Array("id", "name", "parent"))
private[spark] class RDDOperationScope(
    val name: String,
    val parent: Option[RDDOperationScope] = None,
    val id: String = RDDOperationScope.nextScopeId().toString) {

  /**
   * 将当前作用域序列化为JSON格式，用于存储到Spark本地属性
   * @return 序列化后的JSON字符串
   */
  def toJson: String = {
    RDDOperationScope.jsonMapper.writeValueAsString(this)
  }

  /**
   * 获取从最外层祖先作用域到当前作用域的完整作用域链
   * @return 按从外到内顺序排列的作用域序列，包含当前作用域本身
   */
  @JsonIgnore
  def getAllScopes: Seq[RDDOperationScope] = {
    parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case s: RDDOperationScope =>
        id == s.id && name == s.name && parent == s.parent
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hash(id, name, parent)

  override def toString: String = toJson
}

/**
 * RDD操作作用域工具类，提供构建层级作用域、JSON序列化反序列化、执行带作用域的代码块等能力，
 * 用于跟踪记录每个RDD创建时对应的操作链，供Spark UI展示RDD谱系和操作来源。
 */
private[spark] object RDDOperationScope extends Logging {
  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val scopeCounter = new AtomicInteger(0)

  /**
   * 从JSON字符串反序列化恢复RDDOperationScope对象
   * @param s JSON序列化字符串
   * @return 反序列化得到的RDDOperationScope对象
   */
  def fromJson(s: String): RDDOperationScope = {
    jsonMapper.readValue(s, classOf[RDDOperationScope])
  }

  /**
   * 获取全局唯一的递增作用域ID
   * @return 新的唯一ID
   */
  def nextScopeId(): Int = scopeCounter.getAndIncrement

  /**
   * 自动推断调用者方法名，执行指定代码块并使块内创建的所有RDD都归属到同一作用域下
   * @param sc SparkContext上下文实例
   * @param allowNesting 是否允许嵌套子作用域
   * @param body 需要执行的代码块
   * @return 代码块执行结果
   * @note 代码块内不允许使用return语句
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      allowNesting: Boolean = false)(body: => T): T = {
    val ourMethodName = "withScope"
    // 从调用栈中找到当前withScope之外的调用者方法名
    val callerMethodName = Thread.currentThread.getStackTrace()
      .dropWhile(_.getMethodName != ourMethodName)
      .find(_.getMethodName != ourMethodName)
      .map(_.getMethodName)
      .getOrElse {
        // 理论上几乎不会触发，异常情况记录警告并返回默认名称
        logWarning("No valid method name for this RDD operation scope!")
        "N/A"
      }
    withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
  }

  /**
   * 执行指定代码块，使块内创建的所有RDD都归属到当前命名作用域下，支持配置嵌套规则和父作用域继承规则
   * 
   * @param sc SparkContext上下文实例
   * @param name 当前作用域名称
   * @param allowNesting 是否允许当前作用域内嵌套子作用域
   * @param ignoreParent 是否忽略父作用域，作为根作用域重新开始
   * @param body 需要执行的代码块
   * @return 代码块执行结果
   * @note 代码块内不允许使用return语句
   */
  private[spark] def withScope[T](
      sc: SparkContext,
      name: String,
      allowNesting: Boolean,
      ignoreParent: Boolean)(body: => T): T = {
    // 保存旧的作用域状态，方法退出后恢复
    val scopeKey = SparkContext.RDD_SCOPE_KEY
    val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
    val oldScopeJson = sc.getLocalProperty(scopeKey)
    val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
    val oldNoOverride = sc.getLocalProperty(noOverrideKey)
    try {
      if (ignoreParent) {
        // 忽略所有父作用域设置，当前作用域作为根作用域
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
      } else if (sc.getLocalProperty(noOverrideKey) == null) {
        // 仅在父作用域允许覆盖时设置新作用域，继承父作用域构建嵌套结构
        sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
      }
      // 如果不允许嵌套，设置禁止覆盖标记，阻止子代码块修改作用域
      if (!allowNesting) {
        sc.setLocalProperty(noOverrideKey, "true")
      }
      // 执行用户代码块
      body
    } finally {
      // 恢复方法调用前的原有作用域状态
      sc.setLocalProperty(scopeKey, oldScopeJson)
      sc.setLocalProperty(noOverrideKey, oldNoOverride)
    }
  }
}
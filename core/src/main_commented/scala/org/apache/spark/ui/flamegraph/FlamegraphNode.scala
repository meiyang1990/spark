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
package org.apache.spark.ui.flamegraph

import scala.collection.mutable.HashMap

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.status.api.v1.ThreadStackTrace

/**
 * 火焰图节点，存储单个栈帧信息及其子节点，支持序列化为JSON用于前端渲染
 * @param name 栈帧对应的方法名称，已做JSON转义处理
 */
case class FlamegraphNode(name: String) {
  // 子节点映射，key为栈帧名称，value为对应节点
  private val children = new HashMap[String, FlamegraphNode]()
  // 当前节点出现次数，对应火焰图宽度
  private var value: Int = 0

  /**
   * 将当前节点及其子树序列化为JSON格式字符串，供前端火焰图渲染
   * @return 符合火焰图要求的JSON字符串
   */
  def toJsonString: String = {
    // scalastyle:off line.size.limit
    s"""{"name":"$name","value":$value,"children":[${children.map(_._2.toJsonString).mkString(",")}]}"""
    // scalastyle:on line.size.limit
  }
}

/**
 * 火焰图节点构建工具，从线程堆栈列表构建整棵火焰图节点树
 */
object FlamegraphNode {
  /**
   * 从一批线程堆栈构建火焰图节点树，统计各调用栈出现次数
   * @param stacks 待统计的线程堆栈数组
   * @return 构建完成的火焰图根节点
   */
  def apply(stacks: Array[ThreadStackTrace]): FlamegraphNode = {
    val root = FlamegraphNode("root")
    // 遍历所有线程堆栈，统计每个栈帧出现次数
    stacks.foreach { stack =>
      root.value += 1
      var cur = root
      // 反转调用栈顺序，从根节点开始构建（调用栈底到栈顶对应火焰图从父到子）
      stack.stackTrace.elems.reverse.foreach { e =>
        // 提取栈帧第一行作为名称
        val head = e.split("\n").head
        // 对名称做JSON转义，避免特殊字符破坏JSON格式
        val name = StringEscapeUtils.escapeJson(head)
        // 获取或创建子节点，移动当前指针到子节点
        cur = cur.children.getOrElseUpdate(name, FlamegraphNode(name))
        // 当前节点计数加1
        cur.value += 1
      }
    }
    root
  }
}
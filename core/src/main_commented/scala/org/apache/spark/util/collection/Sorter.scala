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

package org.apache.spark.util.collection

import java.util.Comparator

/**
 * 排序工具类，对包私有Java实现TimSort提供公开包装
 * 
 * 核心职责：为Spark其他模块暴露TimSort排序能力，解决原始TimSort实现包私有无法外部调用的问题
 */
/**
 * A simple wrapper over the Java implementation [[TimSort]].
 *
 * The Java implementation is package private, and hence it cannot be called outside package
 * org.apache.spark.util.collection. This is a simple wrapper of it that is available to spark.
 */
private[spark]
class Sorter[K, Buffer](private val s: SortDataFormat[K, Buffer]) {

  // 初始化内部TimSort排序实例
  private val timSort = new TimSort(s)

  /**
   * 对指定缓冲区中的元素在[lo, hi)区间范围内进行排序
   * 
   * @param a 待排序的元素缓冲区
   * @param lo 排序起始下标（包含）
   * @param hi 排序结束下标（不包含）
   * @param c 元素比较器，用于定义排序顺序
   */
  def sort(a: Buffer, lo: Int, hi: Int, c: Comparator[_ >: K]): Unit = {
    timSort.sort(a, lo, hi, c)
  }
}
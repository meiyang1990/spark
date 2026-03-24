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

/**
 * 文件说明：带内存大小跟踪的仅追加哈希映射，用于估算自身占用的内存字节数，
 * 通常在Spark spills（溢出）到磁盘前的大小估算场景使用
 */

/**
 * 带大小跟踪能力的仅追加哈希映射，继承AppendOnlyMap的仅追加写入能力，
 * 混入SizeTracker实现内存占用的增量采样估算，支持Spark任务对内存数据大小的动态跟踪
 * 
 * @tparam K 键类型
 * @tparam V 值类型
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V]
  extends AppendOnlyMap[K, V] with SizeTracker
{
  /**
   * 更新指定键的值，更新后触发大小跟踪采样
   * @param key 待更新的键
   * @param value 新值
   */
  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    super.afterUpdate()
  }

  /**
   * 通过更新函数修改指定键的值，修改后触发大小跟踪采样
   * @param key 待更新的键
   * @param updateFunc 更新函数，参数为是否存在旧值和旧值，返回新值
   * @return 更新后的新值
   */
  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    super.afterUpdate()
    newValue
  }

  /**
   * 扩容哈希表，扩容后重置大小跟踪的采样状态
   */
  override protected def growTable(): Unit = {
    super.growTable()
    resetSamples()
  }
}
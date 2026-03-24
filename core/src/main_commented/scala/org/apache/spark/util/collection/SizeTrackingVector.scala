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

import scala.reflect.ClassTag

/**
 * 文件级注释：仅追加的向量缓冲容器，能够跟踪估算自身占用的内存大小
 * 属于Spark核心工具集合，用于需要监控内存占用的动态数据集合场景
 */

/**
 * 仅追加向量，支持跟踪估算自身占用的字节大小
 * 用于Spark内部需要监控动态集合内存占用的场景，结合SizeTracker实现增量采样估算
 * 
 * @tparam T 存储元素的类型
 */
private[spark] class SizeTrackingVector[T: ClassTag]
  extends PrimitiveVector[T]
  with SizeTracker {

  /**
   * 向向量追加新元素，并更新大小估算样本
   * @param value 要追加的元素
   */
  override def +=(value: T): Unit = {
    super.+=(value)
    // 更新后采样更新大小估算
    super.afterUpdate()
  }

  /**
   * 调整向量容量，重置大小估算样本
   * @param newLength 新的容量长度
   * @return 调整容量后的当前向量
   */
  override def resize(newLength: Int): PrimitiveVector[T] = {
    super.resize(newLength)
    // 容量变化后重置采样，重新开始估算大小
    resetSamples()
    this
  }
}
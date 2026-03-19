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

package org.apache.spark

/**
 * RDD中分区的标识符。每个RDD由多个Partition组成，每个Partition对应一个计算任务。
 * 该trait是所有分区实现的基类，可序列化以便在集群中传输。
 */
trait Partition extends Serializable {
  /**
   * 获取该分区在其所属RDD中的索引编号（从0开始）
   */
  def index: Int

  // 使用分区索引作为hashCode的默认实现，比Object.hashCode更有意义
  override def hashCode(): Int = index

  // equals保持使用引用相等性（Object.equals），避免不同RDD中相同索引的分区被误判为相等
  override def equals(other: Any): Boolean = super.equals(other)
}

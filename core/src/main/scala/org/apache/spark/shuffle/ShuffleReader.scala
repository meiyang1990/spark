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

// 这个文件已经全部加上中文注释

package org.apache.spark.shuffle

/**
 * 在 Reduce 任务中获取，用于从 Mapper 读取合并后的记录。
 */
private[spark] trait ShuffleReader[K, C] {
  /**
   * 读取此 Reduce 任务的合并键值对。
   * 返回一个迭代器，包含所有从 Map 任务获取的数据记录。
   */
  def read(): Iterator[Product2[K, C]]

  /**
   * 关闭此读取器。
   * TODO: 当 ShuffleReader 成为开发者 API 后添加此方法（届时可能需要）
   */
  // def stop(): Unit
}

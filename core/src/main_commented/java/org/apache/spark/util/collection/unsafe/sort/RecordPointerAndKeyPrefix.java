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

package org.apache.spark.util.collection.unsafe.sort;

/**
 * 不安全排序过程中存储记录指针和键前缀的不可变数据结构
 * 用于在基于内存的外部排序中，优化排序比较性能：通过前缀比较避免频繁访问完整键
 */
public final class RecordPointerAndKeyPrefix {
  /**
   * A pointer to a record; see {@link org.apache.spark.memory.TaskMemoryManager} for a
   * description of how these addresses are encoded.
   */
  public long recordPointer;

  /**
   * A key prefix, for use in comparisons.
   */
  public long keyPrefix;
}
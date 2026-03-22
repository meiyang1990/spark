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

import org.apache.spark.annotation.Private;

/**
 * 文件说明：前缀排序中的8字节键前缀比较器抽象基类
 * 
 * 核心职责：为Spark Unsafe排序中的前缀排序提供前缀比较能力，子类可以针对不同数据类型
 * 实现特定的比较逻辑，例如对字符串的字典序前缀比较，提升排序性能。
 * 在Spark排序中，前缀排序先比较前缀快速缩小比较范围，减少完整键比较的次数。
 */
@Private
public abstract class PrefixComparator {
  /**
   * 比较两个排序键的8字节前缀大小
   * @param prefix1 第一个键的前缀
   * @param prefix2 第二个键的前缀
   * @return 比较结果：负整数表示prefix1 < prefix2，0表示相等，正整数表示prefix1 > prefix2
   */
  public abstract int compare(long prefix1, long prefix2);
}
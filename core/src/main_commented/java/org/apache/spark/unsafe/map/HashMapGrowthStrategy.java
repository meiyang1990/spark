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

package org.apache.spark.unsafe.map;

import org.apache.spark.unsafe.array.ByteArrayMethods;

/**
 * 文件说明: 哈希表扩容策略接口，定义了哈希表容量超过阈值时的扩容规则，为底层unsafe哈希map提供可扩展的扩容策略支持
 */
/**
 * Interface that defines how we can grow the size of a hash map when it is over a threshold.
 * 接口说明: 哈希表扩容策略接口，定义哈希表容量扩容的计算方法
 */
public interface HashMapGrowthStrategy {

  /**
   * 计算下一次扩容后的哈希表容量
   * @param currentCapacity 当前哈希表容量
   * @return 扩容后的新容量
   */
  int nextCapacity(int currentCapacity);

  /**
   * Double the size of the hash map every time.
   * 加倍扩容策略实例，每次将容量翻倍，是Spark哈希表默认的扩容策略
   */
  HashMapGrowthStrategy DOUBLING = new Doubling();

  /**
   * 类说明: 加倍扩容策略实现类，每次扩容将哈希表容量翻倍，同时处理容量溢出边界
   */
  class Doubling implements HashMapGrowthStrategy {

    // 数组最大允许容量，取自ByteArrayMethods的常量定义
    private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

    @Override
    public int nextCapacity(int currentCapacity) {
      assert (currentCapacity > 0);
      // 计算翻倍后的容量
      int doubleCapacity = currentCapacity * 2;
      // 防溢出检查，若翻倍后容量合法则返回翻倍值，否则返回最大允许容量
      return (doubleCapacity > 0 && doubleCapacity <= ARRAY_MAX) ? doubleCapacity : ARRAY_MAX;
    }
  }

}
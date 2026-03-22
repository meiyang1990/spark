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
 * 不安全排序流程中的记录比较器抽象基类，用于比较存储在堆外内存的排序记录
 * 当排序键完整容纳在8字节前缀中时，可直接返回0，由前缀比较完成排序
 */
public abstract class RecordComparator {

  /**
   * 比较两条存储在堆外内存的记录顺序，用于排序
   *
   * @param leftBaseObject 左侧记录的基对象（堆内存储时为对象，堆外存储时为null）
   * @param leftBaseOffset 左侧记录的起始偏移地址
   * @param leftBaseLength 左侧记录的总长度
   * @param rightBaseObject 右侧记录的基对象（堆内存储时为对象，堆外存储时为null）
   * @param rightBaseOffset 右侧记录的起始偏移地址
   * @param rightBaseLength 右侧记录的总长度
   * @return 负数表示第一条记录小于第二条，0表示相等，正数表示第一条记录大于第二条
   */
  public abstract int compare(
    Object leftBaseObject,
    long leftBaseOffset,
    int leftBaseLength,
    Object rightBaseObject,
    long rightBaseOffset,
    int rightBaseLength);
}
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

package org.apache.spark.api.java;

import org.apache.spark.storage.StorageLevel;

/**
 * 文件级注释：Java API 层的存储级别常量工厂类，为Java开发者提供Spark常用的RDD缓存存储级别预定义常量，
 * 核心职责是封装底层Scala实现的StorageLevel，提供符合Java编程习惯的静态常量访问入口。
 * 
 * 暴露常用的存储级别常量，供Java开发者使用。
 */
public class StorageLevels {
  /** 不缓存RDD分区，计算后自动丢弃 */
  public static final StorageLevel NONE = create(false, false, false, false, 1);
  /** 仅存储在磁盘，副本数1 */
  public static final StorageLevel DISK_ONLY = create(true, false, false, false, 1);
  /** 仅存储在磁盘，副本数2 */
  public static final StorageLevel DISK_ONLY_2 = create(true, false, false, false, 2);
  /** 仅存储在磁盘，副本数3 */
  public static final StorageLevel DISK_ONLY_3 = create(true, false, false, false, 3);
  /** 仅存储在堆内存，以反序列化对象形式存储，副本数1 */
  public static final StorageLevel MEMORY_ONLY = create(false, true, false, true, 1);
  /** 仅存储在堆内存，以反序列化对象形式存储，副本数2 */
  public static final StorageLevel MEMORY_ONLY_2 = create(false, true, false, true, 2);
  /** 仅存储在堆内存，以序列化字节数组形式存储，节省内存空间，副本数1 */
  public static final StorageLevel MEMORY_ONLY_SER = create(false, true, false, false, 1);
  /** 仅存储在堆内存，以序列化字节数组形式存储，节省内存空间，副本数2 */
  public static final StorageLevel MEMORY_ONLY_SER_2 = create(false, true, false, false, 2);
  /** 优先存储在堆内存，内存放不下时溢写到磁盘，以反序列化对象形式存储，副本数1 */
  public static final StorageLevel MEMORY_AND_DISK = create(true, true, false, true, 1);
  /** 优先存储在堆内存，内存放不下时溢写到磁盘，以反序列化对象形式存储，副本数2 */
  public static final StorageLevel MEMORY_AND_DISK_2 = create(true, true, false, true, 2);
  /** 优先存储在堆内存，内存放不下时溢写到磁盘，以序列化字节数组形式存储，节省内存空间，副本数1 */
  public static final StorageLevel MEMORY_AND_DISK_SER = create(true, true, false, false, 1);
  /** 优先存储在堆内存，内存放不下时溢写到磁盘，以序列化字节数组形式存储，节省内存空间，副本数2 */
  public static final StorageLevel MEMORY_AND_DISK_SER_2 = create(true, true, false, false, 2);
  /** 存储在堆外内存，以序列化字节数组形式存储，副本数1，多用于动态内存管理和大缓存场景 */
  public static final StorageLevel OFF_HEAP = create(true, true, true, false, 1);

  /**
   * 根据传入参数创建自定义存储级别，封装底层StorageLevel构造，供Java API使用。
   * @param useDisk 是否允许使用磁盘存储
   * @param useMemory 是否允许使用堆内内存存储
   * @param useOffHeap 是否允许使用堆外内存存储
   * @param deserialized 是否以反序列化对象形式存储
   * @param replication 存储副本数
   * @return 构造完成的自定义存储级别实例
   */
  public static StorageLevel create(
    boolean useDisk,
    boolean useMemory,
    boolean useOffHeap,
    boolean deserialized,
    int replication) {
    return StorageLevel.apply(useDisk, useMemory, useOffHeap, deserialized, replication);
  }
}
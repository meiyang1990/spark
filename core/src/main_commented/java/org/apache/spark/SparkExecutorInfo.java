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

package org.apache.spark;

import java.io.Serializable;

/**
 * Spark Executor信息接口，提供Executor运行状态和资源使用信息。
 * 
 * 本接口不允许在Spark外部实现，Spark后续可能会新增方法，外部实现会存在二进制兼容性风险。
 * 用于对外暴露集群中执行器节点的基本信息和资源使用情况，供监控、调度等场景使用。
 */
public interface SparkExecutorInfo extends Serializable {
  /**
   * 获取Executor所在的主机地址
   * @return 主机地址字符串
   */
  String host();

  /**
   * 获取Executor的服务端口号
   * @return 端口号
   */
  int port();

  /**
   * 获取Executor缓存已使用的大小
   * @return 已使用缓存大小，单位字节
   */
  long cacheSize();

  /**
   * 获取Executor当前正在运行的任务数量
   * @return 运行中任务数
   */
  int numRunningTasks();

  /**
   * 获取Executor堆内存储内存已使用量
   * @return 已使用堆内存储内存，单位字节
   */
  long usedOnHeapStorageMemory();

  /**
   * 获取Executor堆外存储内存已使用量
   * @return 已使用堆外存储内存，单位字节
   */
  long usedOffHeapStorageMemory();

  /**
   * 获取Executor总堆内存储内存容量
   * @return 总堆内存储内存，单位字节
   */
  long totalOnHeapStorageMemory();

  /**
   * 获取Executor总堆外存储内存容量
   * @return 总堆外存储内存，单位字节
   */
  long totalOffHeapStorageMemory();
}
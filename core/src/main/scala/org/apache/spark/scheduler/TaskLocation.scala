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

package org.apache.spark.scheduler

/**
 * 任务应该运行的位置。可以是一个主机名，也可以是（主机名, executorID）对。
 * 在后一种情况下，会优先在指定的executorID上启动任务；
 * 如果不可行，则退而求其次选择同一主机上的其他Executor。
 */
private[spark] sealed trait TaskLocation {
  def host: String
}

/**
 * 包含主机名和该主机上Executor ID的任务位置。
 * 表示数据已缓存在特定Executor的内存中。
 */
private [spark]
case class ExecutorCacheTaskLocation(override val host: String, executorId: String)
  extends TaskLocation {
  override def toString: String = s"${TaskLocation.executorLocationTag}${host}_$executorId"
}

/**
 * 仅包含主机名的任务位置，表示数据在该主机上（如本地磁盘）。
 */
private [spark] case class HostTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = host
}

/**
 * 数据被HDFS缓存在内存中的主机位置。
 */
private [spark] case class HDFSCacheTaskLocation(override val host: String) extends TaskLocation {
  override def toString: String = TaskLocation.inMemoryLocationTag + host
}

private[spark] object TaskLocation {
  // 标识数据块被HDFS缓存的主机前缀。
  // 由于此前缀包含下划线（主机名中不合法的字符），因此不会与真实主机名混淆。
  // 参见 RFC 952 和 RFC 1123 关于主机名格式的规范。
  val inMemoryLocationTag = "hdfs_cache_"

  // 标识Executor位置的前缀
  val executorLocationTag = "executor_"

  def apply(host: String, executorId: String): TaskLocation = {
    new ExecutorCacheTaskLocation(host, executorId)
  }

  /**
   * 从 getPreferredLocations 返回的字符串创建 TaskLocation。
   * 字符串格式为：executor_[hostname]_[executorid]、[hostname] 或
   * hdfs_cache_[hostname]，取决于数据是否被缓存。
   */
  def apply(str: String): TaskLocation = {
    val hstr = str.stripPrefix(inMemoryLocationTag)
    if (hstr.equals(str)) {
      // 不是HDFS缓存位置，检查是否为Executor位置
      if (str.startsWith(executorLocationTag)) {
        val hostAndExecutorId = str.stripPrefix(executorLocationTag)
        val splits = hostAndExecutorId.split("_", 2)
        require(splits.length == 2, "Illegal executor location format: " + str)
        val Array(host, executorId) = splits
        new ExecutorCacheTaskLocation(host, executorId)
      } else {
        // 普通主机位置
        new HostTaskLocation(str)
      }
    } else {
      // HDFS内存缓存位置
      new HDFSCacheTaskLocation(hstr)
    }
  }
}

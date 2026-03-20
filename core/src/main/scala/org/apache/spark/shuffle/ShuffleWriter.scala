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

import java.io.IOException

import org.apache.spark.scheduler.MapStatus

/**
 * 在 Map 任务中获取，用于将记录写入 Shuffle 系统。
 */
private[spark] abstract class ShuffleWriter[K, V] {
  /**
   * 将一批记录写入此任务的输出。
   *
   * @throws IOException 写入过程中发生 I/O 错误
   */
  @throws[IOException]
  def write(records: Iterator[Product2[K, V]]): Unit

  /**
   * 关闭此写入器，并传递 Map 任务是否成功完成。
   *
   * @param success Map 任务是否成功完成
   * @return 成功时返回 Some(MapStatus)，失败时返回 None
   */
  def stop(success: Boolean): Option[MapStatus]

  /** 获取各分区的数据长度 */
  def getPartitionLengths(): Array[Long]
}

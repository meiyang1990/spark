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

package org.apache.spark.ui.storage

/**
 * 存储页面UI提示工具类，定义存储页面各列的悬浮提示文本
 * 用于在Spark WebUI存储页面显示各指标的帮助说明
 */
private[ui] object ToolTips {

  /** RDD名称列提示文本 */
  val RDD_NAME =
    "Name of the persisted RDD"

  /** 存储级别列提示文本 */
  val STORAGE_LEVEL =
    "StorageLevel displays where the persisted RDD is stored, " +
      "format of the persisted RDD (serialized or de-serialized) and " +
      "replication factor of the persisted RDD"

  /** 已缓存分区数量列提示文本 */
  val CACHED_PARTITIONS =
    "Number of partitions cached"

  /** 已缓存分区占比列提示文本 */
  val FRACTION_CACHED =
    "Fraction of total partitions cached"

  /** 内存中分区总大小列提示文本 */
  val SIZE_IN_MEMORY =
    "Total size of partitions in memory"

  /** 磁盘上分区总大小列提示文本 */
  val SIZE_ON_DISK =
    "Total size of partitions on the disk"
}
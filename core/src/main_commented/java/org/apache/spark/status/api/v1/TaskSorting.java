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

package org.apache.spark.status.api.v1;

import org.apache.spark.util.EnumUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * 任务排序方式枚举，定义Spark UI API v1中任务列表的可选排序规则
 */
public enum TaskSorting {
  /** 按任务ID排序 */
  ID,
  /** 按运行时间升序排序 */
  INCREASING_RUNTIME("runtime"),
  /** 按运行时间降序排序 */
  DECREASING_RUNTIME("-runtime");

  /** 该排序方式的别名集合，用于从字符串匹配排序方式 */
  private final Set<String> alternateNames;
  /**
   * 枚举构造方法，初始化排序方式的别名集合
   * @param names 排序方式的别名列表
   */
  TaskSorting(String... names) {
    alternateNames = new HashSet<>();
    Collections.addAll(alternateNames, names);
  }

  /**
   * 根据输入字符串解析得到对应的任务排序方式
   * 先匹配别名，匹配不到则尝试忽略大小写解析枚举
   * @param str 输入的排序方式字符串
   * @return 解析得到的TaskSorting枚举实例
   */
  public static TaskSorting fromString(String str) {
    String lower = str.toLowerCase(Locale.ROOT);
    for (TaskSorting t: values()) {
      if (t.alternateNames.contains(lower)) {
        return t;
      }
    }
    return EnumUtil.parseIgnoreCase(TaskSorting.class, str);
  }

}
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

/**
 * Spark任务状态枚举，定义REST API返回的任务执行状态
 */
public enum TaskStatus {
  /** 任务正在执行中 */
  RUNNING,
  /** 任务被用户杀死 */
  KILLED,
  /** 任务执行失败 */
  FAILED,
  /** 任务执行成功 */
  SUCCESS,
  /** 未知任务状态 */
  UNKNOWN;

  /**
   * 根据字符串不区分大小写解析得到任务状态枚举
   * @param str 输入的状态字符串
   * @return 对应的任务状态枚举
   */
  public static TaskStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(TaskStatus.class, str);
  }
}
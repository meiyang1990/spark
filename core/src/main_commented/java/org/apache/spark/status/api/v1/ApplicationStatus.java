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
 * Spark应用运行状态枚举，定义REST API返回的应用生命周期状态
 */
public enum ApplicationStatus {
  /** 应用已完成执行 */
  COMPLETED,
  /** 应用正在运行中 */
  RUNNING;

  /**
   * 忽略大小写从字符串解析得到应用状态枚举值
   * @param str 输入的状态字符串
   * @return 对应的应用状态枚举
   */
  public static ApplicationStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(ApplicationStatus.class, str);
  }

}
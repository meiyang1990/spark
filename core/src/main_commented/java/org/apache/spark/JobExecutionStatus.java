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

import org.apache.spark.util.EnumUtil;

/**
 * Spark作业执行状态枚举，描述作业当前所处的运行阶段
 */
public enum JobExecutionStatus {
  /** 作业正在运行中 */
  RUNNING,
  /** 作业执行成功完成 */
  SUCCEEDED,
  /** 作业执行失败 */
  FAILED,
  /** 作业状态未知 */
  UNKNOWN;

  /**
   * 从字符串解析作业执行状态，不区分大小写
   * @param str 待解析的状态字符串
   * @return 对应的枚举值
   */
  public static JobExecutionStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(JobExecutionStatus.class, str);
  }
}
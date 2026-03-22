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
 * Stage状态枚举，定义Spark作业中Stage的所有可能执行状态
 * 供REST状态API使用，对外暴露Stage执行状态信息
 */
public enum StageStatus {
  /** Stage当前正在执行 */
  ACTIVE,
  /** Stage执行完成成功 */
  COMPLETE,
  /** Stage执行失败 */
  FAILED,
  /** Stage等待调度执行 */
  PENDING,
  /** Stage被跳过未执行 */
  SKIPPED;

  /**
   * 从字符串大小写不敏感解析获取StageStatus枚举实例
   * @param str 待解析的状态字符串
   * @return 对应的StageStatus枚举实例
   */
  public static StageStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(StageStatus.class, str);
  }
}
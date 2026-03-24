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

package org.apache.spark.status.protobuf

import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.status.protobuf.StoreTypes.{StageStatus => GStageStatus}

/**
 * 阶段状态序列化/反序列化工具，用于API层StageStatus与Protobuf存储层GStageStatus类型互转
 * 负责将Spark状态存储中的阶段状态在内存对象和Protobuf二进制格式之间转换
 */
private[protobuf] object StageStatusSerializer {

  /**
   * 将API层阶段状态序列化为Protobuf存储层枚举值
   * @param input API层定义的StageStatus状态
   * @return Protobuf存储层对应的GStageStatus枚举值
   */
  def serialize(input: StageStatus): GStageStatus = {
    input match {
      case StageStatus.ACTIVE => GStageStatus.STAGE_STATUS_ACTIVE
      case StageStatus.COMPLETE => GStageStatus.STAGE_STATUS_COMPLETE
      case StageStatus.FAILED => GStageStatus.STAGE_STATUS_FAILED
      case StageStatus.PENDING => GStageStatus.STAGE_STATUS_PENDING
      case StageStatus.SKIPPED => GStageStatus.STAGE_STATUS_SKIPPED
    }
  }

  /**
   * 将Protobuf存储层枚举值反序列化为API层阶段状态
   * @param binary Protobuf存储层的GStageStatus枚举值
   * @return API层对应的StageStatus状态，未知值返回null
   */
  def deserialize(binary: GStageStatus): StageStatus = {
    binary match {
      case GStageStatus.STAGE_STATUS_ACTIVE => StageStatus.ACTIVE
      case GStageStatus.STAGE_STATUS_COMPLETE => StageStatus.COMPLETE
      case GStageStatus.STAGE_STATUS_FAILED => StageStatus.FAILED
      case GStageStatus.STAGE_STATUS_PENDING => StageStatus.PENDING
      case GStageStatus.STAGE_STATUS_SKIPPED => StageStatus.SKIPPED
      case _ => null
    }
  }
}
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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi


/**
 * :: DeveloperApi ::
 * 在任务或Stage执行期间被修改的 [[org.apache.spark.util.AccumulatorV2]] 的信息。
 *
 * @param id 累加器ID
 * @param name 累加器名称
 * @param update 来自单个任务的部分更新值，在Driver端描述Stage时可能为None
 * @param value 到目前为止的累积总值，在Executor端描述任务时可能为None
 * @param internal 是否为内部累加器
 * @param countFailedValues 如果任务失败，是否计入此累加器的部分值
 * @param metadata 与此累加器关联的内部元数据（如果有）
 *
 * @note 一旦进行JSON序列化，`update`和`value`的类型信息将丢失并被转换为字符串。
 * 这是因为用户可以定义任意类型的累加器，在事件日志的消费者中很难保留其类型信息。
 * 此限制不适用于表示任务级别度量指标的内部累加器。
 */
@DeveloperApi
case class AccumulableInfo private[spark] (
    id: Long,
    name: Option[String],
    update: Option[Any], // represents a partial update within a task
    value: Option[Any],
    private[spark] val internal: Boolean,
    private[spark] val countFailedValues: Boolean,
    // TODO: use this to identify internal task metrics instead of encoding it in the name
    private[spark] val metadata: Option[String] = None)

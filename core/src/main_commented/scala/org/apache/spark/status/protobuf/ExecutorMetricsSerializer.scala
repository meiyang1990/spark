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

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType

/**
 * Executor指标Protobuf序列化与反序列化工具类
 * 负责将Executor运行指标在Spark内部对象和Protobuf持久化格式之间转换，用于状态存储
 */
private[protobuf] object ExecutorMetricsSerializer {

  /**
   * 将内部ExecutorMetrics对象序列化为Protobuf格式
   * @param e 待序列化的内部Executor指标对象
   * @return Protobuf格式的ExecutorMetrics对象
   */
  def serialize(e: ExecutorMetrics): StoreTypes.ExecutorMetrics = {
    val builder = StoreTypes.ExecutorMetrics.newBuilder()
    // 遍历所有指标类型，将指标值存入Protobuf构建器
    ExecutorMetricType.metricToOffset.foreach { case (metric, _) =>
      builder.putMetrics(metric, e.getMetricValue(metric))
    }
    builder.build()
  }

  /**
   * 将Protobuf格式的Executor指标反序列化为内部对象
   * @param binary Protobuf格式的Executor指标对象
   * @return 反序列化后的内部ExecutorMetrics对象
   */
  def deserialize(binary: StoreTypes.ExecutorMetrics): ExecutorMetrics = {
    // 按照指标索引顺序组装指标值数组，匹配ExecutorMetrics构造要求
    val array = ExecutorMetricType.metricToOffset.map { case (name, idx) =>
      binary.getMetricsOrDefault(name, 0L)
    }.toArray
    new ExecutorMetrics(array)
  }
}
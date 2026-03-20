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

package org.apache.spark

import java.io.Serializable

import org.apache.spark.annotation.{DeveloperApi, Since}

/**
 * PartitionEvaluator 的工厂接口。Spark 将此工厂序列化发送到 Executor，
 * 然后在 Executor 端创建 PartitionEvaluator 实例。
 * 每个 RDD 分区创建一个 Evaluator 实例，即单线程使用。
 */
@DeveloperApi
@Since("3.5.0")
trait PartitionEvaluatorFactory[T, U] extends Serializable {

  /**
   * 创建分区计算器。每个 RDD 分区会创建一个计算器实例，
   * 即一个计算器实例只会由单个线程使用。
   */
  def createEvaluator(): PartitionEvaluator[T, U]
}

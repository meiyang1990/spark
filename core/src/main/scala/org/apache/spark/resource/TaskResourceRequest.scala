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

package org.apache.spark.resource

import org.apache.spark.annotation.{Since, Stable}

/**
 * TaskResourceRequest - 任务资源请求
 * 
 * 与 ResourceProfile 配合使用，以编程方式指定 RDD 在 Stage 级别所需的 Task 资源。
 * 
 * 关键特性：
 * - amount 使用 Double 类型支持分数资源请求
 * - 有效值为 <= 1.0 或整数
 * - 分数资源允许多个任务共享同一资源地址
 *   例如：amount = 0.5 表示 2 个任务共享 1 个资源地址
 * 
 * 建议使用 TaskResourceRequests 类作为便捷 API。
 *
 * @param resourceName 资源名称
 * @param amount 请求数量（Double 类型，支持分数）
 */
@Stable
@Since("3.1.0")
class TaskResourceRequest(val resourceName: String, val amount: Double)
  extends Serializable {

  assert(amount <= 1.0 || amount % 1 == 0,
    s"The resource amount ${amount} must be either <= 1.0, or a whole number.")

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: TaskResourceRequest =>
        that.getClass == this.getClass &&
          that.resourceName == resourceName && that.amount == amount
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(resourceName, amount).hashCode()

  override def toString(): String = {
    s"name: $resourceName, amount: $amount"
  }
}

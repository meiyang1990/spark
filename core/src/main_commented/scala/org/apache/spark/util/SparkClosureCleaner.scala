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

package org.apache.spark.util

import scala.collection.mutable

import org.apache.spark.{SparkEnv, SparkException}

/**
 * Spark闭包清理工具，负责清理闭包中不必要的引用，使闭包可以被正确序列化
 * 在Spark分布式执行中，用户自定义函数（闭包）需要发送到执行节点，必须可序列化
 * 该工具会移除闭包对外部不可序列化对象的不必要引用，解决任务无法序列化问题
 */
private[spark] object SparkClosureCleaner {
  /**
   * 清理指定闭包，使其满足序列化要求
   * 核心作用是移除闭包对外部对象的不必要引用，只要闭包没有显式引用不可序列化对象，清理后即可序列化
   * 
   * @param closure           待清理的闭包对象
   * @param checkSerializable 清理完成后是否检查闭包是否可序列化
   * @param cleanTransitively 是否递归清理外层嵌套的闭包
   * @return 清理后的闭包对象，如果实际未进行清理则返回原闭包
   */
  def clean[F <: AnyRef](
      closure: F,
      checkSerializable: Boolean = true,
      cleanTransitively: Boolean = true): F = {
    val cleanedClosureOpt = ClosureCleaner.clean(closure, cleanTransitively, mutable.Map.empty)
    if (cleanedClosureOpt.isDefined) {
      val cleanedClosure = cleanedClosureOpt.get
      try {
        // 开启检查且Spark环境已初始化时，序列化闭包验证是否可序列化
        if (checkSerializable && SparkEnv.get != null) {
          SparkEnv.get.closureSerializer.newInstance().serialize(cleanedClosure: AnyRef)
        }
      } catch {
        // 序列化失败时抛出任务不可序列化异常，提示用户问题
        case ex: Exception => throw new SparkException("Task not serializable", ex)
      }
      cleanedClosure
    } else {
      closure
    }
  }
}
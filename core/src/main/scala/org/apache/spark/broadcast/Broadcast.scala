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

package org.apache.spark.broadcast

import java.io.Serializable

import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.util.Utils

/**
 * 广播变量。允许程序员将只读变量缓存在每台机器上，而不是随任务一起发送副本。
 * 可用于将大型输入数据集高效地分发到所有节点。Spark 使用高效的广播算法降低通信成本。
 *
 * 广播变量通过调用 [[org.apache.spark.SparkContext#broadcast]] 从变量 `v` 创建。
 * 广播变量是 `v` 的包装器，其值可通过 `value` 方法访问。示例如下：
 *
 * {{{
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
 *
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 * }}}
 *
 * 创建广播变量后，应在集群上运行的函数中使用该变量而非原始值 `v`，
 * 以确保 `v` 不会多次发送到节点。另外，不应修改广播值以保证所有节点获得一致的值
 * （例如变量稍后被发送到新节点时）。
 *
 * @param id 广播变量的唯一标识符
 * @tparam T 广播变量包含的数据类型
 */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
   * 表示该广播变量是否有效（未被销毁）的标志
   */
  @volatile private var _isValid = true

  // 记录销毁该广播变量的调用位置（用于错误诊断）
  private var _destroySite = ""

  /**
   * 获取广播的值
   */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
   * 异步删除该广播在所有 executor 上的缓存副本。
   * 如果之后仍使用该广播，需要重新从 driver 发送给各 executor。
   */
  def unpersist(): Unit = {
    unpersist(blocking = false)
  }

  /**
   * 删除该广播在所有 executor 上的缓存副本。
   * @param blocking 是否阻塞直到删除完成
   */
  def unpersist(blocking: Boolean): Unit = {
    assertValid()
    doUnpersist(blocking)
  }


  /**
   * 销毁该广播变量的所有数据和元数据。谨慎使用；
   * 一旦广播变量被销毁，就不能再使用。
   */
  def destroy(): Unit = {
    destroy(blocking = false)
  }

  /**
   * 销毁该广播变量的所有数据和元数据。谨慎使用；
   * 一旦广播变量被销毁，就不能再使用。
   * @param blocking 是否阻塞直到销毁完成
   */
  private[spark] def destroy(blocking: Boolean): Unit = {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo(log"Destroying ${MDC(LogKeys.BROADCAST, toString)} " +
      log"(from ${MDC(LogKeys.CALL_SITE_SHORT_FORM, _destroySite)})")
    doDestroy(blocking)
  }

  /**
   * 该广播变量是否可用。一旦 driver 上的持久化状态被删除，应返回 false。
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * 实际获取广播的值。具体实现类必须定义自己的方式获取值。
   */
  protected def getValue(): T

  /**
   * 实际删除该广播在 executor 上的持久化值。
   * 具体实现类必须定义自己的删除逻辑。
   */
  protected def doUnpersist(blocking: Boolean): Unit

  /**
   * 实际销毁该广播变量的所有数据和元数据。
   * 具体实现类必须定义自己的销毁逻辑。
   */
  protected def doDestroy(blocking: Boolean): Unit

  /**
   * 检查该广播是否有效。如无效则抛出异常
   */
  protected def assertValid(): Unit = {
    if (!_isValid) {
      throw SparkException.internalError(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite),
        category = "BROADCAST")
    }
  }

  override def toString: String = "Broadcast(" + id + ")"
}

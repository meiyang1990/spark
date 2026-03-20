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

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * TaskContext 感知的迭代器。
 * 当任务完成或被中断后自动停止消费数据，防止 Python 评估线程在任务结束后
 * 继续读取父迭代器导致的堆外内存访问段错误。
 *
 * @since 3.1.0
 * @deprecated 自 4.0.0 起已废弃，因其唯一的 Python 评估使用场景已消除
 */
@DeveloperApi
@deprecated("Only usage for Python evaluation is now extinct", "4.0.0")
class ContextAwareIterator[+T](val context: TaskContext, val delegate: Iterator[T])
  extends Iterator[T] {

  override def hasNext: Boolean =
    !context.isCompleted() && !context.isInterrupted() && delegate.hasNext

  override def next(): T = delegate.next()
}

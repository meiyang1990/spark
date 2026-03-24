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

/**
 * 提供基础迭代器模板实现，简化自定义迭代器开发。
 * 抽象基类，封装了迭代器的状态管理逻辑，子类只需要实现获取下一个元素和关闭资源的方法即可。
 *
 * @tparam U 迭代器返回的元素类型
 */
private[spark] abstract class NextIterator[U] extends Iterator[U] {

  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false

  /**
   * 子类需要实现的方法，用于获取下一个元素。
   * 如果没有更多元素，子类应该将`finished`标记设为true，返回值将被忽略。
   * 该设计约定避免了null作为有效元素的问题，也避免了在紧循环中创建Option对象带来的额外开销。
   *
   * @return 下一个元素，如果遍历完成则任意值都可
   */
  protected def getNext(): U

  /**
   * 子类需要实现的方法，在遍历完成后释放相关资源。
   * 注意：NextIterator无法保证close一定会被调用，因为无法控制用户代码在调用hasNext/next时抛出异常的场景。
   * 理想情况下应该像HadoopRDD那样，在外部额外添加try/catch来保证迭代失败时资源也能被正确关闭。
   */
  protected def close(): Unit

  /**
   * 调用子类定义的close方法，保证只执行一次。
   * 通常多次调用close应该没问题，但历史上某些InputFormat存在重复调用抛出异常的问题，因此这里做了保护。
   */
  def closeIfNeeded(): Unit = {
    if (!closed) {
      // 注意：必须在调用close()之前就将closed设为true，否则如果close抛出异常，会导致多次调用close()
      closed = true
      close()
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        // 获取下一个元素
        nextValue = getNext()
        // 如果获取后标记遍历完成，则关闭资源
        if (finished) {
          closeIfNeeded()
        }
        // 标记已经预取出下一个元素
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    // 重置预取标记，下次需要重新获取
    gotNext = false
    nextValue
  }
}
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

package org.apache.spark.rdd

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.unsafe.types.UTF8String

/**
 * 存储当前Spark任务正在读取的输入文件块信息，供HadoopRDD、FileScanRDD、NewHadoopRDD以及Spark SQL的input_file_name函数获取上下文信息
 */
private[spark] object InputFileBlockHolder {
  /**
   * 输入文件块信息的封装类
   *
   * @param filePath 读取文件的路径，不可用则为空字符串
   * @param startOffset 块的起始偏移量（字节），不可用则为-1
   * @param length 块的大小（字节），不可用则为-1
   */
  private class FileBlock(val filePath: UTF8String, val startOffset: Long, val length: Long) {
    def this() = {
      this(UTF8String.fromString(""), -1, -1)
    }
  }

  /**
   * 线程本地变量，存储当前线程正在读取的文件块信息，供Spark SQL的input_file_name函数使用
   *
   * @note 这里使用继承性线程本地存储配合原子引用，保证子线程写入的块信息可以被父线程读取到，解决Python UDF执行等跨线程场景的问题，参见SPARK-28153
   */
  private[this] val inputBlock: InheritableThreadLocal[AtomicReference[FileBlock]] =
    new InheritableThreadLocal[AtomicReference[FileBlock]] {
      override protected def initialValue(): AtomicReference[FileBlock] =
        new AtomicReference(new FileBlock)
    }

  /**
   * 设置线程本地存储的原子引用，用于特殊跨线程场景下复用已有引用
   */
  private[spark] def setThreadLocalValue(ref: Object): Unit = {
    inputBlock.set(ref.asInstanceOf[AtomicReference[FileBlock]])
  }

  /**
   * 获取当前线程本地存储的原子引用，用于跨线程场景传递引用
   */
  private[spark] def getThreadLocalValue(): Object = {
    inputBlock.get()
  }

  /**
   * 获取当前输入文件的路径，未知则返回空字符串
   */
  def getInputFilePath: UTF8String = inputBlock.get().get().filePath

  /**
   * 获取当前读取块的起始偏移量，未知则返回-1
   */
  def getStartOffset: Long = inputBlock.get().get().startOffset

  /**
   * 获取当前读取块的长度，未知则返回-1
   */
  def getLength: Long = inputBlock.get().get().length

  /**
   * 设置当前线程的输入块信息
   *
   * 调用方必须确保注册了任务完成监听器，在任务结束后调用unset清空线程本地变量
   */
  def set(filePath: String, startOffset: Long, length: Long): Unit = {
    // 参数校验：文件路径不能为空
    require(filePath != null, "filePath cannot be null")
    // 参数校验：起始偏移量不能为负
    require(startOffset >= 0, s"startOffset ($startOffset) cannot be negative")
    // 参数校验：长度不能小于-1（-1表示未知）
    require(length >= -1, s"length ($length) cannot be smaller than -1")
    // 更新原子引用中的文件块信息
    inputBlock.get().set(new FileBlock(UTF8String.fromString(filePath), startOffset, length))
  }

  /**
   * 清空当前线程的输入块信息，重置为默认值
   */
  def unset(): Unit = inputBlock.remove()

  /**
   * 初始化父线程的线程本地变量，触发ThreadLocal的initialValue方法，确保子线程可以继承正确的引用
   */
  def initialize(): Unit = inputBlock.get()
}
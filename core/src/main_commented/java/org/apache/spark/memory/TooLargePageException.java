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

package org.apache.spark.memory;

/**
 * 内存分配异常类，表示请求分配的内存页大小超过当前可用的最大内存页大小
 * 当Spark内存管理器无法分配满足大小要求的内存页时抛出此异常
 */
public class TooLargePageException extends RuntimeException {
  /**
   * 构造方法，创建请求过大内存页的异常实例
   * @param size 请求分配的内存页大小（字节）
   */
  TooLargePageException(long size) {
    super("Cannot allocate a page of " + size + " bytes.");
  }
}
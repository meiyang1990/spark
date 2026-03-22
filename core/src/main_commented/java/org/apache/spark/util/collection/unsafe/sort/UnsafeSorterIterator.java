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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.IOException;

/**
 * 外部排序迭代器抽象基类，用于Unsafe排序模块遍历已排序记录
 * 提供基于堆外内存的记录访问接口，支持溢出排序后的有序记录遍历
 */
public abstract class UnsafeSorterIterator {

  /**
   * 检查是否还有下一条可遍历的记录
   * @return true 存在下一条记录，false 已遍历完成
   */
  public abstract boolean hasNext();

  /**
   * 加载下一条记录，将当前指针移动到下一条记录位置
   * @throws IOException 读取溢出磁盘文件时可能抛出IO异常
   */
  public abstract void loadNext() throws IOException;

  /**
   * 获取当前记录所在的基础对象（堆内内存为数组对象，堆外内存为null）
   * @return 当前记录的基础对象引用
   */
  public abstract Object getBaseObject();

  /**
   * 获取当前记录在基础对象中的偏移地址
   * @return 偏移地址（字节数）
   */
  public abstract long getBaseOffset();

  /**
   * 获取当前记录的长度
   * @return 记录长度（字节数）
   */
  public abstract int getRecordLength();

  /**
   * 获取当前记录键的前缀，用于排序时的快速比较
   * @return 键前缀值
   */
  public abstract long getKeyPrefix();

  /**
   * 获取当前迭代器包含的总记录数
   * @return 总记录数
   */
  public abstract int getNumRecords();

  /**
   * 获取当前记录所在的内存页编号
   * @return 内存页编号
   */
  public abstract long getCurrentPageNumber();
}
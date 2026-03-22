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

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.SortDataFormat;

/**
 * 基于Unsafe内存模型的排序数据格式实现，支持对(记录指针, 键前缀)二元组数组进行排序，
 * 用于UnsafeInMemorySorter实现基于直接内存的高性能排序。
 * <p>
 * 在LongArray缓冲区中，索引{@code 2 * i}存储第i个元素的记录指针，
 * 索引{@code 2 * i + 1}存储该元素的8字节键前缀，单个元素占用16字节内存。
 */
public final class UnsafeSortDataFormat
  extends SortDataFormat<RecordPointerAndKeyPrefix, LongArray> {

  private final LongArray buffer;

  /**
   * 构造函数，使用预分配的LongArray缓冲区初始化排序数据格式
   * @param buffer 预分配的Unsafe长整型数组缓冲区
   */
  public UnsafeSortDataFormat(LongArray buffer) {
    this.buffer = buffer;
  }

  @Override
  public RecordPointerAndKeyPrefix getKey(LongArray data, int pos) {
    // 本实现复用键对象，不应调用此方法分配新键
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordPointerAndKeyPrefix newKey() {
    return new RecordPointerAndKeyPrefix();
  }

  @Override
  public RecordPointerAndKeyPrefix getKey(LongArray data, int pos,
                                          RecordPointerAndKeyPrefix reuse) {
    // 从数组指定位置读取记录指针
    reuse.recordPointer = data.get(pos * 2);
    // 从数组指定位置读取键前缀
    reuse.keyPrefix = data.get(pos * 2 + 1);
    return reuse;
  }

  @Override
  public void swap(LongArray data, int pos0, int pos1) {
    // 暂存第一个位置的记录指针和键前缀
    long tempPointer = data.get(pos0 * 2);
    long tempKeyPrefix = data.get(pos0 * 2 + 1);
    // 将第二个位置的数据写入第一个位置
    data.set(pos0 * 2, data.get(pos1 * 2));
    data.set(pos0 * 2 + 1, data.get(pos1 * 2 + 1));
    // 将暂存的数据写入第二个位置，完成交换
    data.set(pos1 * 2, tempPointer);
    data.set(pos1 * 2 + 1, tempKeyPrefix);
  }

  @Override
  public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
    // 复制源位置的记录指针到目标位置
    dst.set(dstPos * 2, src.get(srcPos * 2));
    // 复制源位置的键前缀到目标位置
    dst.set(dstPos * 2 + 1, src.get(srcPos * 2 + 1));
  }

  @Override
  public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
    // 使用Unsafe批量内存拷贝，提高批量复制效率，每个元素占用16字节
    Platform.copyMemory(
      src.getBaseObject(),
      src.getBaseOffset() + srcPos * 16L,
      dst.getBaseObject(),
      dst.getBaseOffset() + dstPos * 16L,
      length * 16L);
  }

  @Override
  public LongArray allocate(int length) {
    // 检查预分配缓冲区容量是否满足需求
    assert (length * 2L <= buffer.size()) :
      "the buffer is smaller than required: " + buffer.size() + " < " + (length * 2);
    // 返回预分配好的缓冲区，不重新分配内存
    return buffer;
  }

}
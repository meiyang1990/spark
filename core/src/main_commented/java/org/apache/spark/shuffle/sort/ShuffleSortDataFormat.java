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

package org.apache.spark.shuffle.sort;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.SortDataFormat;

/**
 * Shuffle排序专用的排序数据格式适配器
 * 适配SortTimer排序框架，处理打包后的Shuffle记录指针数组
 */
final class ShuffleSortDataFormat extends SortDataFormat<PackedRecordPointer, LongArray> {

  private final LongArray buffer;

  /**
   * 构造Shuffle排序数据格式适配器
   * @param buffer 存储打包记录指针的底层数组
   */
  ShuffleSortDataFormat(LongArray buffer) {
    this.buffer = buffer;
  }

  @Override
  public PackedRecordPointer getKey(LongArray data, int pos) {
    // 由于我们重用键对象，该方法不应被调用
    throw new UnsupportedOperationException();
  }

  @Override
  public PackedRecordPointer newKey() {
    return new PackedRecordPointer();
  }

  @Override
  public PackedRecordPointer getKey(LongArray data, int pos, PackedRecordPointer reuse) {
    // 从数组指定位置读取打包指针，复用传入的对象
    reuse.set(data.get(pos));
    return reuse;
  }

  @Override
  public void swap(LongArray data, int pos0, int pos1) {
    // 交换数组中两个位置的打包指针
    final long temp = data.get(pos0);
    data.set(pos0, data.get(pos1));
    data.set(pos1, temp);
  }

  @Override
  public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
    // 复制单个打包指针从源数组到目标数组
    dst.set(dstPos, src.get(srcPos));
  }

  @Override
  public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
    // 批量拷贝一段打包指针，使用Unsafe直接内存拷贝提升性能
    Platform.copyMemory(
      src.getBaseObject(),
      src.getBaseOffset() + srcPos * 8L,
      dst.getBaseObject(),
      dst.getBaseOffset() + dstPos * 8L,
      length * 8L
    );
  }

  @Override
  public LongArray allocate(int length) {
    // 检查预分配缓冲区空间是否足够
    assert (length <= buffer.size()) :
      "the buffer is smaller than required: " + buffer.size() + " < " + length;
    // 返回预分配好的缓冲区，避免重复分配内存
    return buffer;
  }
}
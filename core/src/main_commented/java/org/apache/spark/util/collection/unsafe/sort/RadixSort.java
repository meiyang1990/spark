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
import org.apache.spark.network.util.JavaUtils;

/**
 * 基于最低有效字节优先的基数排序实现，用于Spark Shuffle阶段的无序内存排序
 * 提供普通长整型数组排序和键前缀数组排序两种特化实现，利用空间换时间提供O(n)排序性能
 */
public class RadixSort {

  /**
   * 对给定长整型数组执行最低有效字节优先基数排序
   * 要求数组末尾预留至少等于记录数量的空槽用于排序过程中转存储，排序会修改原数组数据位置
   *
   * @param array 长整型数组，数据记录后需保留至少同等数量的空槽
   * @param numRecords 数组中的数据记录数量
   * @param startByteIndex 起始排序字节索引，从最低有效字节开始计数，范围[0, 7]
   * @param endByteIndex 结束排序字节索引，从最低有效字节开始计数，范围[0, 7]，必须大于startByteIndex
   * @param desc 是否降序排序（二进制序）
   * @param signed 是否按有符号补码规则排序
   *
   * @return 排序后数据在原数组中的起始索引，为了性能不主动拷贝回数组起始位置，由调用方处理
   */
  public static int sort(
      LongArray array, long numRecords, int startByteIndex, int endByteIndex,
      boolean desc, boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert endByteIndex > startByteIndex;
    assert numRecords * 2 <= array.size();
    long inIndex = 0;
    long outIndex = numRecords;
    if (numRecords > 0) {
      // 统计每个字节位置各值的出现次数，提前跳过无差异字节
      long[][] counts = getCounts(array, numRecords, startByteIndex, endByteIndex);
      // 逐字节进行基数排序
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortAtByte(
            array, numRecords, counts[i], i, inIndex, outIndex,
            desc, signed && i == endByteIndex);
          // 交换输入输出位置，下一轮排序基于当前轮的结果继续
          long tmp = inIndex;
          inIndex = outIndex;
          outIndex = tmp;
        }
      }
    }
    return JavaUtils.checkedCast(inIndex);
  }

  /**
   * 在指定字节位置执行一轮基数排序，将数据按该字节值分配到目标偏移位置
   *
   * @param array 需要排序的数组
   * @param numRecords 数据记录数量
   * @param counts 每个字节值的计数数组，本方法会破坏性修改该数组
   * @param byteIdx 当前排序的字节索引，从最低有效字节开始计数
   * @param inIndex 输入数据在数组中的起始索引
   * @param outIndex 排序结果输出在数组中的起始索引
   * @param desc 是否降序排序
   * @param signed 是否按有符号规则排序，仅对最后一字节生效
   */
  private static void sortAtByte(
      LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    // 将计数转换为每个桶的输出偏移地址
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + outIndex * 8L, 8, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + inIndex * 8L;
    long maxOffset = baseOffset + numRecords * 8L;
    // 遍历所有记录，按当前字节值分配到对应桶位置
    for (long offset = baseOffset; offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      int bucket = (int)((value >>> (byteIdx * 8)) & 0xff);
      Platform.putLong(baseObject, offsets[bucket], value);
      offsets[bucket] += 8;
    }
  }

  /**
   * 统计数组中每个字节位置各值的出现次数，优化：提前跳过所有记录该字节值相同的位置
   *
   * @param array 需要统计的数组
   * @param numRecords 数据记录数量
   * @param startByteIndex 起始统计字节索引
   * @param endByteIndex 结束统计字节索引
   *
   * @return 长度为8的数组，每个元素对应一个字节位置的256长度计数数组，不需要排序的字节位置为null
   */
  private static long[][] getCounts(
      LongArray array, long numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    // 优化：提前判断哪些字节位置所有值都相同，可以跳过排序
    long bitwiseMax = 0;
    long bitwiseMin = -1L;
    long maxOffset = array.getBaseOffset() + numRecords * 8L;
    Object baseObject = array.getBaseObject();
    // 通过按位与或快速检测哪些字节位置存在值变化
    for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
      long value = Platform.getLong(baseObject, offset);
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    long bitsChanged = bitwiseMin ^ bitwiseMax;
    // 对存在值变化的字节位置统计每个值的出现次数
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        // TODO(ekl) consider computing all the counts in one pass.
        for (long offset = array.getBaseOffset(); offset < maxOffset; offset += 8) {
          counts[i][(int)((Platform.getLong(baseObject, offset) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  /**
   * 将每个字节值的计数转换为对应输出地址偏移，支持升序/降序、有符号/无符号排序
   *
   * @param counts 每个字节值的计数数组，本方法会破坏性修改该数组
   * @param numRecords 原始数据记录数量
   * @param outputOffset 输出区域基于数组基对象的字节偏移
   * @param bytesPerRecord 每个记录占用字节数，普通排序为8，键前缀排序为16
   * @param desc 是否降序排序
   * @param signed 是否按有符号补码排序
   *
   * @return 转换后的偏移数组，每个位置对应字节值的输出起始地址
   */
  private static long[] transformCountsToOffsets(
      long[] counts, long numRecords, long outputOffset, long bytesPerRecord,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    int start = signed ? 128 : 0;  // 有符号排序时负数（128-255）排在前面
    if (desc) {
      // 降序排序，从后往前计算偏移
      long pos = numRecords;
      for (int i = start; i < start + 256; i++) {
        pos -= counts[i & 0xff];
        counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
      }
    } else {
      // 升序排序，从前往后计算偏移
      long pos = 0;
      for (int i = start; i < start + 256; i++) {
        long tmp = counts[i & 0xff];
        counts[i & 0xff] = outputOffset + pos * bytesPerRecord;
        pos += tmp;
      }
    }
    return counts;
  }

  /**
   * 键前缀数组特化的基数排序，每个记录由两个长整型组成，仅对第二个长整型（前缀）排序
   * 用于Spark Shuffle中排序分区指针+排序前缀的组合记录结构
   *
   * @param array 键前缀数组
   * @param startIndex 数组中排序起始索引
   * @param numRecords 数据记录数量
   * @param startByteIndex 起始排序字节索引，从最低有效字节开始计数，范围[0, 7]
   * @param endByteIndex 结束排序字节索引，从最低有效字节开始计数，范围[0, 7]，必须大于startByteIndex
   * @param desc 是否降序排序（二进制序）
   * @param signed 是否按有符号补码规则排序
   * @return 排序后数据在原数组中的起始索引
   */
  public static int sortKeyPrefixArray(
      LongArray array,
      long startIndex,
      long numRecords,
      int startByteIndex,
      int endByteIndex,
      boolean desc,
      boolean signed) {
    assert startByteIndex >= 0 : "startByteIndex (" + startByteIndex + ") should >= 0";
    assert endByteIndex <= 7 : "endByteIndex (" + endByteIndex + ") should <= 7";
    assert endByteIndex > startByteIndex;
    assert numRecords * 4 <= array.size();
    long inIndex = startIndex;
    long outIndex = startIndex + numRecords * 2L;
    if (numRecords > 0) {
      // 统计键前缀数组各字节值的出现次数
      long[][] counts = getKeyPrefixArrayCounts(
        array, startIndex, numRecords, startByteIndex, endByteIndex);
      // 逐字节进行基数排序
      for (int i = startByteIndex; i <= endByteIndex; i++) {
        if (counts[i] != null) {
          sortKeyPrefixArrayAtByte(
            array, numRecords, counts[i], i, inIndex, outIndex,
            desc, signed && i == endByteIndex);
          // 交换输入输出位置
          long tmp = inIndex;
          inIndex = outIndex;
          outIndex = tmp;
        }
      }
    }
    return JavaUtils.checkedCast(inIndex);
  }

  /**
   * 键前缀数组特化的计数统计，和通用计数逻辑分离是为了性能考虑
   *
   * @param array 键前缀数组
   * @param startIndex 起始索引
   * @param numRecords 记录数量
   * @param startByteIndex 起始统计字节索引
   * @param endByteIndex 结束统计字节索引
   * @return 计数数组，不需要排序的字节位置为null
   */
  private static long[][] getKeyPrefixArrayCounts(
      LongArray array, long startIndex, long numRecords, int startByteIndex, int endByteIndex) {
    long[][] counts = new long[8][];
    long bitwiseMax = 0;
    long bitwiseMin = -1L;
    long baseOffset = array.getBaseOffset() + startIndex * 8L;
    long limit = baseOffset + numRecords * 16L;
    Object baseObject = array.getBaseObject();
    // 快速检测哪些字节位置存在值变化
    for (long offset = baseOffset; offset < limit; offset += 16) {
      long value = Platform.getLong(baseObject, offset + 8);
      bitwiseMax |= value;
      bitwiseMin &= value;
    }
    long bitsChanged = bitwiseMin ^ bitwiseMax;
    // 对存在变化的字节位置统计计数
    for (int i = startByteIndex; i <= endByteIndex; i++) {
      if (((bitsChanged >>> (i * 8)) & 0xff) != 0) {
        counts[i] = new long[256];
        for (long offset = baseOffset; offset < limit; offset += 16) {
          counts[i][(int)((Platform.getLong(baseObject, offset + 8) >>> (i * 8)) & 0xff)]++;
        }
      }
    }
    return counts;
  }

  /**
   * 键前缀数组特化的单字节基数排序
   *
   * @param array 键前缀数组
   * @param numRecords 记录数量
   * @param counts 每个字节值的计数数组
   * @param byteIdx 当前排序字节索引
   * @param inIndex 输入数据起始索引
   * @param outIndex 输出数据起始索引
   * @param desc 是否降序排序
   * @param signed 是否按有符号排序，仅对最后一字节生效
   */
  private static void sortKeyPrefixArrayAtByte(
      LongArray array, long numRecords, long[] counts, int byteIdx, long inIndex, long outIndex,
      boolean desc, boolean signed) {
    assert counts.length == 256;
    // 转换计数为输出偏移地址
    long[] offsets = transformCountsToOffsets(
      counts, numRecords, array.getBaseOffset() + outIndex * 8L, 16, desc, signed);
    Object baseObject = array.getBaseObject();
    long baseOffset = array.getBaseOffset() + inIndex * 8L;
    long maxOffset = baseOffset + numRecords * 16L;
    // 遍历所有记录，将键和前缀一起拷贝到对应桶位置
    for (long offset = baseOffset; offset < maxOffset; offset += 16) {
      long key = Platform.getLong(baseObject, offset);
      long prefix = Platform.getLong(baseObject, offset + 8);
      int bucket = (int)((prefix >>> (byteIdx * 8)) & 0xff);
      long dest = offsets[bucket];
      Platform.putLong(baseObject, dest, key);
      Platform.putLong(baseObject, dest + 8, prefix);
      offsets[bucket] += 16;
    }
  }
}
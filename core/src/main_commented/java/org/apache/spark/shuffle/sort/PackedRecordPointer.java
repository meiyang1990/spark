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

/**
 * 排序式Shuffle中对分区编号和记录地址的打包存储工具类，将两个信息压缩存储到一个8字节长整型中。
 * <p>
 * 长整型的位分布如下：
 * <pre>
 *   [24位分区编号][13位内存页号][27位页内偏移]
 * </pre>
 * 该位分布设计支持单个任务最大可寻址 1TB 内存：最大页大小为 2^27 = 128MB，最多支持 2^13 = 8192 个页。
 * <p>
 * 如果采用页对齐优化，可以支持最大1GB页大小，但需要记录填充对齐，该优化留待未来实现。
 */
final class PackedRecordPointer {

  /** 单个内存页的最大字节数，128MB */
  static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;  // 128 megabytes

  /** 可编码的最大分区ID（分区ID从0开始计数），共24位，最大值为16777215 */
  static final int MAXIMUM_PARTITION_ID = (1 << 24) - 1;  // 16777215

  /** 分区ID起始字节索引，从最低有效字节开始计数 */
  static final int PARTITION_ID_START_BYTE_INDEX = 5;

  /** 分区ID结束字节索引，从最低有效字节开始计数 */
  static final int PARTITION_ID_END_BYTE_INDEX = 7;

  /** 长整型低40位的位掩码 */
  private static final long MASK_LONG_LOWER_40_BITS = (1L << 40) - 1;

  /** 长整型高24位的位掩码 */
  private static final long MASK_LONG_UPPER_24_BITS = ~MASK_LONG_LOWER_40_BITS;

  /** 长整型低27位的位掩码 */
  private static final long MASK_LONG_LOWER_27_BITS = (1L << 27) - 1;

  /** 长整型低51位的位掩码 */
  private static final long MASK_LONG_LOWER_51_BITS = (1L << 51) - 1;

  /** 长整型高13位的位掩码 */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * 将记录地址和分区ID打包压缩到一个64位长整型中。
   *
   * @param recordPointer TaskMemoryManager编码后的记录地址
   * @param partitionId Shuffle分区ID，最大值不超过2^24
   * @return 打包后的压缩指针，可通过PackedRecordPointer类解码
   */
  public static long packPointer(long recordPointer, int partitionId) {
    assert (partitionId <= MAXIMUM_PARTITION_ID);
    // Note that without word alignment we can address 2^27 bytes = 128 megabytes per page.
    // Also note that this relies on some internals of how TaskMemoryManager encodes its addresses.
    // 从原始记录地址中提取13位页号，并右移对齐到压缩位置
    final long pageNumber = (recordPointer & MASK_LONG_UPPER_13_BITS) >>> 24;
    // 拼接压缩后的页号和页内偏移
    final long compressedAddress = pageNumber | (recordPointer & MASK_LONG_LOWER_27_BITS);
    // 将分区ID放到高24位，拼接压缩地址得到最终打包结果
    return (((long) partitionId) << 40) | compressedAddress;
  }

  /** 存储打包后的记录指针长整型 */
  private long packedRecordPointer;

  /**
   * 设置要解码的打包记录指针
   * @param packedRecordPointer 打包后的长整型指针
   */
  public void set(long packedRecordPointer) {
    this.packedRecordPointer = packedRecordPointer;
  }

  /**
   * 从打包指针中解码获取分区ID
   * @return 分区ID
   */
  public int getPartitionId() {
    return (int) ((packedRecordPointer & MASK_LONG_UPPER_24_BITS) >>> 40);
  }

  /**
   * 从打包指针中解码恢复原始记录地址
   * @return TaskMemoryManager可识别的原始记录地址
   */
  public long getRecordPointer() {
    // 恢复页号到原始位置
    final long pageNumber = (packedRecordPointer << 24) & MASK_LONG_UPPER_13_BITS;
    // 获取页内偏移
    final long offsetInPage = packedRecordPointer & MASK_LONG_LOWER_27_BITS;
    // 拼接得到原始地址
    return pageNumber | offsetInPage;
  }

}
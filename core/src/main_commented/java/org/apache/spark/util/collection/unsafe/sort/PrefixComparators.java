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

import com.google.common.primitives.UnsignedLongs;

import org.apache.spark.annotation.Private;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * 不安全排序前缀比较器工厂类，提供多种数据类型和排序规则的前缀比较器实现
 * 用于Spark基于基数排序的外部排序流程，通过前缀比较快速过滤排序键
 */
@Private
public class PrefixComparators {
  private PrefixComparators() {}

  /** 字符串升序排序，空值排在最前 */
  public static final PrefixComparator STRING = new UnsignedPrefixComparator();
  /** 字符串降序排序，空值排在最后 */
  public static final PrefixComparator STRING_DESC = new UnsignedPrefixComparatorDesc();
  /** 字符串升序排序，空值排在最后 */
  public static final PrefixComparator STRING_NULLS_LAST = new UnsignedPrefixComparatorNullsLast();
  /** 字符串降序排序，空值排在最前 */
  public static final PrefixComparator STRING_DESC_NULLS_FIRST =
    new UnsignedPrefixComparatorDescNullsFirst();

  /** 二进制数据升序排序，空值排在最前 */
  public static final PrefixComparator BINARY = new UnsignedPrefixComparator();
  /** 二进制数据降序排序，空值排在最后 */
  public static final PrefixComparator BINARY_DESC = new UnsignedPrefixComparatorDesc();
  /** 二进制数据升序排序，空值排在最后 */
  public static final PrefixComparator BINARY_NULLS_LAST = new UnsignedPrefixComparatorNullsLast();
  /** 二进制数据降序排序，空值排在最前 */
  public static final PrefixComparator BINARY_DESC_NULLS_FIRST =
    new UnsignedPrefixComparatorDescNullsFirst();

  /** 长整型升序排序，空值排在最前 */
  public static final PrefixComparator LONG = new SignedPrefixComparator();
  /** 长整型降序排序，空值排在最后 */
  public static final PrefixComparator LONG_DESC = new SignedPrefixComparatorDesc();
  /** 长整型升序排序，空值排在最后 */
  public static final PrefixComparator LONG_NULLS_LAST = new SignedPrefixComparatorNullsLast();
  /** 长整型降序排序，空值排在最前 */
  public static final PrefixComparator LONG_DESC_NULLS_FIRST =
    new SignedPrefixComparatorDescNullsFirst();

  /** 双精度浮点数升序排序，空值排在最前 */
  public static final PrefixComparator DOUBLE = new UnsignedPrefixComparator();
  /** 双精度浮点数降序排序，空值排在最后 */
  public static final PrefixComparator DOUBLE_DESC = new UnsignedPrefixComparatorDesc();
  /** 双精度浮点数升序排序，空值排在最后 */
  public static final PrefixComparator DOUBLE_NULLS_LAST = new UnsignedPrefixComparatorNullsLast();
  /** 双精度浮点数降序排序，空值排在最前 */
  public static final PrefixComparator DOUBLE_DESC_NULLS_FIRST =
    new UnsignedPrefixComparatorDescNullsFirst();

  /**
   * 字符串排序前缀工具类，负责从UTF8String提取排序前缀
   */
  public static final class StringPrefixComparator {
    /**
     * 从UTF8String提取排序前缀，空值返回默认前缀0
     * @param value 输入UTF8字符串
     * @return 提取出的64位排序前缀
     */
    public static long computePrefix(UTF8String value) {
      return value == null ? 0L : value.getPrefix();
    }
  }

  /**
   * 二进制数据排序前缀工具类，负责从字节数组提取排序前缀
   */
  public static final class BinaryPrefixComparator {
    /**
     * 从字节数组提取排序前缀
     * @param bytes 输入二进制字节数组
     * @return 提取出的64位排序前缀
     */
    public static long computePrefix(byte[] bytes) {
      return ByteArray.getPrefix(bytes);
    }
  }

  /**
   * 双精度浮点数排序前缀工具类，负责将double转换为可正确比较的64位前缀
   */
  public static final class DoublePrefixComparator {
    /**
     * Converts the double into a value that compares correctly as an unsigned long. For more
     * details see http://stereopsis.com/radix.html.
     * 转换double为可作为无符号长整型正确比较的前缀位
     * @param value 输入双精度浮点数
     * @return 转换后的64位排序前缀
     */
    public static long computePrefix(double value) {
      // 将-0.0标准化为0.0，保证比较相等性
      value = value == -0.0 ? 0.0 : value;
      // Java的doubleToLongBits已经将所有NaN值统一规范为最小正NaN，不需要额外处理
      long bits = Double.doubleToLongBits(value);
      // 浮点数符号幅度表示法中负数比较顺序反转，因此需要翻转所有位
      long mask = -(bits >>> 63) | 0x8000000000000000L;
      return bits ^ mask;
    }
  }

  /**
   * 基数排序支持抽象基类，定义基数排序所需参数，实现该接口表示比较器兼容基数排序
   */
  public abstract static class RadixSortSupport extends PrefixComparator {
    /** @return 是否为降序排序 */
    public abstract boolean sortDescending();

    /** @return 排序是否需要考虑符号位 */
    public abstract boolean sortSigned();

    /** @return 是否将空值排在最前 */
    public abstract boolean nullsFirst();
  }

  //
  // Standard prefix comparator implementations
  //

  /**
   * 无符号前缀升序比较器，空值排在最前
   */
  public static final class UnsignedPrefixComparator extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  /**
   * 无符号前缀升序比较器，空值排在最后
   */
  public static final class UnsignedPrefixComparatorNullsLast extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long aPrefix, long bPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  /**
   * 无符号前缀降序比较器，空值排在最前
   */
  public static final class UnsignedPrefixComparatorDescNullsFirst extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  /**
   * 无符号前缀降序比较器，空值排在最后
   */
  public static final class UnsignedPrefixComparatorDesc extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return false; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long bPrefix, long aPrefix) {
      return UnsignedLongs.compare(aPrefix, bPrefix);
    }
  }

  /**
   * 有符号前缀升序比较器，空值排在最前
   */
  public static final class SignedPrefixComparator extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long a, long b) {
      return Long.compare(a, b);
    }
  }

  /**
   * 有符号前缀升序比较器，空值排在最后
   */
  public static final class SignedPrefixComparatorNullsLast extends RadixSortSupport {
    @Override public boolean sortDescending() { return false; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long a, long b) {
      return Long.compare(a, b);
    }
  }

  /**
   * 有符号前缀降序比较器，空值排在最前
   */
  public static final class SignedPrefixComparatorDescNullsFirst extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return true; }
    @Override
    public int compare(long b, long a) {
      return Long.compare(a, b);
    }
  }

  /**
   * 有符号前缀降序比较器，空值排在最后
   */
  public static final class SignedPrefixComparatorDesc extends RadixSortSupport {
    @Override public boolean sortDescending() { return true; }
    @Override public boolean sortSigned() { return true; }
    @Override public boolean nullsFirst() { return false; }
    @Override
    public int compare(long b, long a) {
      return Long.compare(a, b);
    }
  }
}
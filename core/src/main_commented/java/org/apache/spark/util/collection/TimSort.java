// 这个文件已经全部加上中文注释
/*
 * Based on TimSort.java from the Android Open Source Project
 *
 *  Copyright (C) 2008 The Android Open Source Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.util.collection;

import java.util.Comparator;

/**
 * 从Android开源项目移植的TimSort排序实现，是一种稳定、自适应、迭代式归并排序算法。
 * 
 * 为了保持和Android源码高度一致，方便验证正确性，保留了Java实现和原始代码风格。
 * 该类为包私有，对外提供Scala包装类{@link org.apache.spark.util.collection.Sorter}，供Spark核心模块使用。
 * 
 * 本次移植的核心目的是泛化排序接口，能够支持除普通元素数组外的其他数据格式。
 * 例如AppendOnlyMap使用该实现对[key, value, key, value]交替存储的数组进行排序，
 * 泛化带来的额外开销极小，具体可参考SortDataFormat类。
 * 
 * 本实现允许键重用，避免创建大量重复键对象，降低GC压力。
 *
 * @see org.apache.spark.util.collection.SortDataFormat
 * @see org.apache.spark.util.collection.Sorter
 */
class TimSort<K, Buffer> {

  /**
   * 最小需要进行归并的序列长度。更短的序列会通过binarySort扩展长度。
   * 如果整个待排序数组长度小于该值，不会执行任何归并操作，直接使用二分插入排序完成。
   * 
   * 该常量应为2的幂。Tim Peter的C实现为64，本实现通过实验确定32性能更好。
   * 如果修改该常量，必须同步修改minRunLength计算逻辑，否则可能出现栈溢出。
   */
  private static final int MIN_MERGE = 32;

  private final SortDataFormat<K, Buffer> s;

  /**
   * 构造TimSort实例，指定数据格式处理对象
   * @param sortDataFormat 自定义数据格式处理实现
   */
  public TimSort(SortDataFormat<K, Buffer> sortDataFormat) {
    this.s = sortDataFormat;
  }

  /**
   * 执行TimSort排序，这是一种稳定、自适应、迭代式归并排序：
   * 在部分有序数组上，比较次数远少于n lg(n)；在随机数组上，性能和传统归并排序相当。
   * 最坏情况下时间复杂度O(n log n)，空间复杂度最坏需要n/2个对象引用的临时空间，最好情况只需要常量空间。
   * 
   * 本实现改编自Python的Tim Peters列表排序实现。
   * 
   * @param a 待排序缓冲区
   * @param lo 排序起始索引（包含）
   * @param hi 排序结束索引（不包含）
   * @param c 键比较器
   */
  public void sort(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert c != null;

    int nRemaining  = hi - lo;
    if (nRemaining < 2)
      return;  // 长度0或1的数组已经有序，无需排序

    // 数组长度小于最小归并长度，直接使用二分插入排序完成，不需要归并
    if (nRemaining < MIN_MERGE) {
      int initRunLen = countRunAndMakeAscending(a, lo, hi, c);
      binarySort(a, lo, hi, lo + initRunLen, c);
      return;
    }

    /**
     * 从左到右遍历数组一次，识别自然有序片段（run），将短自然片段扩展到minRun长度，
     * 然后合并片段保持栈不变性，最终完成全排序。
     */
    SortState sortState = new SortState(a, c, hi - lo);
    int minRun = minRunLength(nRemaining);
    do {
      // 识别下一个有序片段
      int runLen = countRunAndMakeAscending(a, lo, hi, c);

      // 如果片段长度小于minRun，扩展到min(minRun, nRemaining)长度
      if (runLen < minRun) {
        int force = nRemaining <= minRun ? nRemaining : minRun;
        binarySort(a, lo, lo + force, lo + runLen, c);
        runLen = force;
      }

      // 将片段压入待归并栈，可能触发合并维持栈不变性
      sortState.pushRun(lo, runLen);
      sortState.mergeCollapse();

      // 前进指针寻找下一个片段
      lo += runLen;
      nRemaining -= runLen;
    } while (nRemaining != 0);

    // 合并所有剩余片段完成排序
    assert lo == hi;
    sortState.mergeForceCollapse();
    assert sortState.stackSize == 1;
  }

  /**
   * 使用二分插入排序对数组指定区间排序。该方法适合对少量元素排序，
   * 比较次数为O(n log n)，但数据移动最坏情况为O(n^2)。
   * 
   * 该方法可以利用已有有序前缀：假定从lo（包含）到start（不包含）已经有序。
   *
   * @param a 待排序缓冲区
   * @param lo 排序区间第一个元素索引
   * @param hi 排序区间最后一个元素之后的索引
   * @param start 第一个未确认有序的元素索引（lo <= start <= hi）
   * @param c 排序比较器
   */
  @SuppressWarnings("fallthrough")
  private void binarySort(Buffer a, int lo, int hi, int start, Comparator<? super K> c) {
    assert lo <= start && start <= hi;
    if (start == lo)
      start++;

    K key0 = s.newKey();
    K key1 = s.newKey();

    Buffer pivotStore = s.allocate(1);
    for ( ; start < hi; start++) {
      s.copyElement(a, start, pivotStore, 0);
      K pivot = s.getKey(pivotStore, 0, key0);

      // 二分查找确定pivot应该插入的位置
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * 不变量:
       *   pivot >= [lo, left) 中所有元素
       *   pivot <  [right, start) 中所有元素
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (c.compare(pivot, s.getKey(a, mid, key1)) < 0)
          right = mid;
        else
          left = mid + 1;
      }
      assert left == right;

      /*
       * 插入位置就是left，由于相等元素会放在已有相等元素后面，因此排序是稳定的。
       * 将插入位置之后的元素整体后移一位，腾出空间放入pivot。
       */
      int n = start - left;  // 需要移动的元素数量
      // switch对小数量移动做优化，避免数组拷贝开销
      switch (n) {
        case 2:  s.copyElement(a, left + 1, a, left + 2);
        case 1:  s.copyElement(a, left, a, left + 1);
          break;
        default: s.copyRange(a, left, a, left + 1, n);
      }
      s.copyElement(pivotStore, 0, a, left);
    }
  }

  /**
   * 计算从指定位置开始的有序片段长度，如果片段是降序则反转确保返回的片段一定是升序。
   * 
   * 升序定义：a[lo] <= a[lo + 1] <= a[lo + 2] <= ...
   * 降序定义：a[lo] >  a[lo + 1] >  a[lo + 2] >  ...
   * 严格降序定义保证反转后仍然稳定（相等元素不会交换位置）。
   *
   * @param a 待处理缓冲区
   * @param lo 片段起始索引
   * @param hi 整个排序区间结束索引
   * @param c 排序比较器
   * @return 有序片段长度
   */
  private int countRunAndMakeAscending(Buffer a, int lo, int hi, Comparator<? super K> c) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi)
      return 1;

    K key0 = s.newKey();
    K key1 = s.newKey();

    // 判断当前序列走向，如果降序反转整个降序片段
    if (c.compare(s.getKey(a, runHi++, key0), s.getKey(a, lo, key1)) < 0) { // 降序
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) < 0)
        runHi++;
      reverseRange(a, lo, runHi);
    } else {                              // 升序
      while (runHi < hi && c.compare(s.getKey(a, runHi, key0), s.getKey(a, runHi - 1, key1)) >= 0)
        runHi++;
    }

    return runHi - lo;
  }

  /**
   * 反转数组指定区间的元素顺序
   *
   * @param a 目标缓冲区
   * @param lo 区间起始索引（包含）
   * @param hi 区间结束索引（不包含）
   */
  private void reverseRange(Buffer a, int lo, int hi) {
    hi--;
    while (lo < hi) {
      s.swap(a, lo, hi);
      lo++;
      hi--;
    }
  }

  /**
   * 计算给定长度数组的最小可接受run长度。天然长度小于该值的run会通过binarySort扩展。
   * 
   * 计算逻辑：
   * 1. 如果n < MIN_MERGE，直接返回n
   * 2. 如果n恰好是2的幂，返回MIN_MERGE/2
   * 3. 否则返回k，满足MIN_MERGE/2 <= k <= MIN_MERGE，且n/k严格小于并接近2的幂
   *
   * @param n 待排序数组长度
   * @return 最小run长度
   */
  private int minRunLength(int n) {
    assert n >= 0;
    int r = 0;      // 只要移出了1位，就会置为1
    while (n >= MIN_MERGE) {
      r |= (n & 1);
      n >>= 1;
    }
    return n + r;
  }

  /**
   * TimSort排序过程中的状态保存类，保存待归并run栈、临时存储等排序过程状态
   */
  private class SortState {

    /**
     * 正在排序的缓冲区
     */
    private final Buffer a;

    /**
     * 待排序缓冲区总长度
     */
    private final int aLength;

    /**
     * 当前排序使用的比较器
     */
    private final Comparator<? super K> c;

    /**
     * 进入galloping（奔驰）模式后，保持该模式要求两个run连续赢的次数都小于MIN_GALLOP
     */
    private static final int  MIN_GALLOP = 7;

    /**
     * 控制进入galloping模式的阈值，初始化为MIN_GALLOP。
     * mergeLo和mergeHi会根据数据有序性动态调整该值：随机数据会调高，有序数据会调低。
     */
    private int minGallop = MIN_GALLOP;

    /**
     * 归并使用的临时数组最大初始大小，后续可以按需扩容。
     * 和原始C实现不同，排序小数组时不会分配这么大空间，该优化提升了性能。
     */
    private static final int INITIAL_TMP_STORAGE_LENGTH = 256;

    /**
     * 归并使用的临时存储
     */
    private Buffer tmp; // Actual runtime type will be Object[], regardless of T

    /**
     * 临时存储容量
     */
    private int tmpLength = 0;

    /**
     * 待合并run栈，run i从base[i]开始，长度为len[i]。始终满足：
     * runBase[i] + runLen[i] == runBase[i + 1]
     * 虽然可以压缩存储空间，但显式保存简化了代码，存储开销可以忽略。
     */
    private int stackSize = 0;  // 栈中待合并run数量
    private final int[] runBase;
    private final int[] runLen;

    /**
     * 创建排序状态对象，分配初始存储空间
     *
     * @param a 待排序缓冲区
     * @param c 排序比较器
     * @param len 待排序总长度
     */
    private SortState(Buffer a, Comparator<? super K> c, int len) {
      this.aLength = len;
      this.a = a;
      this.c = c;

      // 分配临时存储，后续可按需扩容
      tmpLength = len < 2 * INITIAL_TMP_STORAGE_LENGTH ? len >>> 1 : INITIAL_TMP_STORAGE_LENGTH;
      tmp = s.allocate(tmpLength);

      /*
       * 分配待合并run栈，栈长度根据数组长度动态调整：
       * 原始C实现固定栈长85，Java中小数组分配这么大空间浪费，因此根据长度选择足够但更小的栈长。
       * 最大49栈长足够处理Integer.MAX_VALUE长度数组最坏情况。
       */
      int stackLen = (len <    120  ?  5 :
                      len <   1542  ? 10 :
                      len < 119151  ? 24 : 49);
      runBase = new int[stackLen];
      runLen = new int[stackLen];
    }

    /**
     * 将新的run压入待合并栈
     *
     * @param runBase run起始索引
     * @param runLen run长度
     */
    private void pushRun(int runBase, int runLen) {
      this.runBase[stackSize] = runBase;
      this.runLen[stackSize] = runLen;
      stackSize++;
    }

    /**
     * 检查待合并run栈，合并相邻run直到满足栈不变性：
     * 1. runLen[i - 3] > runLen[i - 2] + runLen[i - 1]
     * 2. runLen[i - 2] > runLen[i - 1]
     * 
     * 每次压入新run后调用该方法，进入方法时对于i < stackSize不变性已经成立。
     * 该实现修复了TimSort原有的最坏时间复杂度问题。
     */
    private void mergeCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if (n > 0 && runLen[n-1] <= runLen[n] + runLen[n+1] ||
            n > 1 && runLen[n-2] <= runLen[n] + runLen[n-1]) {
          if (runLen[n - 1] < runLen[n + 1])
            n--;
        } else if (n < 0 || runLen[n] > runLen[n + 1]) {
          break; // 不变性已满足
        }
        mergeAt(n);
      }
    }

    /**
     * 强制合并栈中所有run直到只剩一个，完成整个排序。在排序结束时调用一次。
     */
    private void mergeForceCollapse() {
      while (stackSize > 1) {
        int n = stackSize - 2;
        if (n > 0 && runLen[n - 1] < runLen[n + 1])
          n--;
        mergeAt(n);
      }
    }

    /**
     * 合并栈中索引i和i+1位置的两个相邻run。i必须是stackSize-2或stackSize-3。
     *
     * @param i 第一个待合并run在栈中的索引
     */
    private void mergeAt(int i) {
      assert stackSize >= 2;
      assert i >= 0;
      assert i == stackSize - 2 || i == stackSize - 3;

      int base1 = runBase[i];
      int len1 = runLen[i];
      int base2 = runBase[i + 1];
      int len2 = runLen[i + 1];
      assert len1 > 0 && len2 > 0;
      assert base1 + len1 == base2;

      /*
       * 记录合并后的总长度；如果i是倒数第三个run，需要将最后一个run前移一位。
       * 无论如何，i+1位置的run都会被合并消失。
       */
      runLen
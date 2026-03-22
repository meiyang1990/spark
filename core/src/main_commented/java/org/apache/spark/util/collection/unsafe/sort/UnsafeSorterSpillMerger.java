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
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * 外部排序溢出文件合并器，使用k-way归并算法合并多个已排序的溢出分区
 * 用于Spark unsafe排序过程中，当内存不足溢出到磁盘后，将多个有序溢出发件合并为全局有序结果
 */
final class UnsafeSorterSpillMerger {

  private int numRecords = 0;
  private final PriorityQueue<UnsafeSorterIterator> priorityQueue;

  /**
   * 构造溢出合并器，初始化优先队列用于k-way归并排序
   * @param recordComparator 记录键比较器，用于比较两条完整记录的大小
   * @param prefixComparator 前缀比较器，用于比较记录键的前缀，加快比较速度
   * @param numSpills 待合并的溢出发件数量，用于初始化优先队列容量
   */
  UnsafeSorterSpillMerger(
      RecordComparator recordComparator,
      PrefixComparator prefixComparator,
      int numSpills) {
    Comparator<UnsafeSorterIterator> comparator = (left, right) -> {
      // 先比较键前缀，如果前缀不同直接返回比较结果
      int prefixComparisonResult =
        prefixComparator.compare(left.getKeyPrefix(), right.getKeyPrefix());
      if (prefixComparisonResult == 0) {
        // 前缀相同，再比较完整记录
        return recordComparator.compare(
          left.getBaseObject(), left.getBaseOffset(), left.getRecordLength(),
          right.getBaseObject(), right.getBaseOffset(), right.getRecordLength());
      } else {
        return prefixComparisonResult;
      }
    };
    // 初始化基于比较器的优先队列，每次取出当前最小元素
    priorityQueue = new PriorityQueue<>(numSpills, comparator);
  }

  /**
   * 添加一个非空的溢出发件迭代器到合并队列
   * 如果溢出发件为空则不添加，避免生成空记录
   * @param spillReader 溢出发件的迭代器
   * @throws IOException 读取溢出发件时IO异常
   */
  public void addSpillIfNotEmpty(UnsafeSorterIterator spillReader) throws IOException {
    if (spillReader.hasNext()) {
      // 预加载下一条记录，保证优先队列中每个迭代器都已经有当前最小记录
      spillReader.loadNext();
      priorityQueue.add(spillReader);
      // 累加总记录数
      numRecords += spillReader.getNumRecords();
    }
  }

  /**
   * 获取合并后的全局有序迭代器
   * @return 合并后的有序记录迭代器
   * @throws IOException 读取溢出发件时IO异常
   */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    return new UnsafeSorterIterator() {

      // 当前正在输出的溢出发件迭代器
      private UnsafeSorterIterator spillReader;

      @Override
      public int getNumRecords() {
        return numRecords;
      }

      @Override
      public long getCurrentPageNumber() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasNext() {
        // 优先队列非空说明还有待合并的记录，或者当前迭代器还有未输出的记录
        return !priorityQueue.isEmpty() || (spillReader != null && spillReader.hasNext());
      }

      @Override
      public void loadNext() throws IOException {
        if (spillReader != null) {
          // 如果当前迭代器还有剩余记录，将其重新加入优先队列等待后续合并
          if (spillReader.hasNext()) {
            spillReader.loadNext();
            priorityQueue.add(spillReader);
          }
        }
        // 取出当前所有溢出发件中最小的记录，作为下一条输出
        spillReader = priorityQueue.remove();
      }

      @Override
      public Object getBaseObject() { return spillReader.getBaseObject(); }

      @Override
      public long getBaseOffset() { return spillReader.getBaseOffset(); }

      @Override
      public int getRecordLength() { return spillReader.getRecordLength(); }

      @Override
      public long getKeyPrefix() { return spillReader.getKeyPrefix(); }
    };
  }
}
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.errors.SparkCoreErrors;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * 文件说明：TaskMemoryManager的内存消费者抽象基类，支持内存不足时将数据溢写 spilling 到磁盘
 * 
 * 核心职责：为需要申请和释放钨计划(Tungsten)内存的消费者提供统一抽象接口，支持内存溢出时自动 spill 溢出，仅支持钨计划内存的分配与溢出
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  private final long pageSize;
  private final MemoryMode mode;
  protected final AtomicLong used = new AtomicLong(0L);

  /**
   * 构造函数，初始化内存消费者
   * @param taskMemoryManager 任务内存管理器
   * @param pageSize 内存页大小
   * @param mode 内存模式（堆内/堆外）
   */
  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
    this.mode = mode;
  }

  /**
   * 构造函数，使用默认页大小初始化内存消费者
   * @param taskMemoryManager 任务内存管理器
   * @param mode 内存模式（堆内/堆外）
   */
  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, MemoryMode mode) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), mode);
  }

  /**
   * 获取当前内存消费者的内存模式
   * @return 堆内ON_HEAP或堆外OFF_HEAP
   */
  public MemoryMode getMode() {
    return mode;
  }

  /**
   * 获取当前已使用的内存大小
   * @return 已使用内存字节数
   */
  public long getUsed() {
    return used.get();
  }

  /**
   * 强制触发内存溢写，释放所有可释放内存
   * @throws IOException 溢写磁盘IO异常
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * 抽象方法：将部分数据溢写到磁盘释放内存，由任务内存管理器在内存不足时调用，需要子类实现
   * 
   * 注意事项：为避免死锁，spill方法内部不允许调用acquireMemory，且当前仅释放钨计划管理的内存页
   * 
   * @param size 需要释放的内存字节数
   * @param trigger 触发本次溢写的内存消费者
   * @return 实际释放的内存字节数
   * @throws IOException 溢写磁盘IO异常
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * 分配指定长度的LongArray，分配失败会抛出内存不足异常，调用者需要处理异常
   * 
   * @param size 数组长度
   * @return 分配得到的LongArray对象
   * @throws SparkOutOfMemoryError Spark内存不足异常
   * @throws TooLargePageException 数组过大无法容纳在单个内存页异常
   */
  public LongArray allocateArray(long size) {
    // 计算实际需要的字节数，每个long占8字节
    long required = size * 8L;
    // 向任务内存管理器申请内存页
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    // 申请失败或分配内存不足，抛出OOM异常
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    // 更新已使用内存统计
    used.getAndAdd(required);
    return new LongArray(page);
  }

  /**
   * 释放LongArray占用的内存
   * @param array 需要释放的LongArray
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * 分配至少满足需求大小的内存页
   * @param required 需要的最小字节数
   * @return 分配得到的内存块
   * @throws SparkOutOfMemoryError Spark内存不足异常
   */
  protected MemoryBlock allocatePage(long required) {
    // 实际分配大小取页大小和需求的最大值，保证内存页对齐
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    // 申请失败或分配内存不足，抛出OOM异常
    if (page == null || page.size() < required) {
      throwOom(page, required);
    }
    // 更新已使用内存统计
    used.getAndAdd(page.size());
    return page;
  }

  /**
   * 释放指定内存块
   * @param page 需要释放的内存块
   */
  protected void freePage(MemoryBlock page) {
    // 更新已使用内存统计，减去释放大小
    used.getAndAdd(-page.size());
    // 归还内存给任务内存管理器
    taskMemoryManager.freePage(page, this);
  }

  /**
   * 申请执行内存
   * @param size 需要申请的内存字节数
   * @return 实际获得分配的内存字节数
   */
  public long acquireMemory(long size) {
    long granted = taskMemoryManager.acquireExecutionMemory(size, this);
    // 更新已使用内存统计
    used.getAndAdd(granted);
    return granted;
  }

  /**
   * 释放执行内存
   * @param size 需要释放的内存字节数
   */
  public void freeMemory(long size) {
    // 归还内存给任务内存管理器
    taskMemoryManager.releaseExecutionMemory(size, this);
    // 更新已使用内存统计
    used.getAndAdd(-size);
  }

  /**
   * 处理内存分配失败，抛出OOM异常
   * 先释放已分配的部分内存，打印当前内存使用情况，然后抛出异常
   */
  private void throwOom(final MemoryBlock page, final long required) {
    long got = 0;
    if (page != null) {
      got = page.size();
      taskMemoryManager.freePage(page, this);
    }
    // 打印当前任务内存使用情况到日志，帮助排查问题
    taskMemoryManager.showMemoryUsage();
    throw SparkCoreErrors.outOfMemoryError(required, got);
  }
}
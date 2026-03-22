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
package org.apache.spark.util;

import scala.Function0;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 文件说明：无锁实现的尽力初始化延迟加载值工具类，用于避免标准延迟初始化在并发场景下的死锁问题
 * 
 * A lock-free implementation of a lazily-initialized variable.
 * If there are concurrent initializations then the `compute()` function may be invoked
 * multiple times. However, only a single `compute()` result will be stored and all readers
 * will receive the same result object instance.
 *
 * 适用场景：在某些不严格要求值只计算一次的场景中，避免并发初始化导致的死锁问题
 *
 * @note
 * This helper class has additional requirements on the compute function:
 *   1) The compute function MUST not return null;
 *   2) The computation failure is not cached.
 *
 * @note
 *   Scala 3 uses a different implementation of lazy vals which doesn't have this problem.
 *   Please refer to <a
 *   href="https://docs.scala-lang.org/scala3/reference/changed-features/lazy-vals-init.html">Lazy
 *   Vals Initialization</a> for more details.
 * 
 * 类说明：无锁尽力初始化延迟加载值，允许多次并发计算但保证仅存储一个结果，避免死锁
 */
public class BestEffortLazyVal<T> implements Serializable {
  // 计算值的函数，初始化完成后会被置空以支持垃圾回收
  private volatile Function0<T> compute;
  // 缓存的已计算结果，volatile保证可见性
  protected volatile T cached;

  // 用于原子操作cached字段的VarHandle实例
  private static final VarHandle HANDLE;
  static {
    try {
      // 通过MethodHandles获取cached字段的VarHandle，用于实现原子CAS操作
      HANDLE = MethodHandles.lookup()
        .in(BestEffortLazyVal.class)
        .findVarHandle(BestEffortLazyVal.class, "cached", Object.class);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize VarHandle", e);
    }
  }

  /**
   * 构造函数，使用指定的计算函数初始化延迟加载值
   * @param compute 计算目标值的函数
   */
  public BestEffortLazyVal(Function0<T> compute) {
    this.compute = compute;
  }

  /**
   * 获取延迟加载的值，若未初始化则触发计算，返回最终存储的唯一结果
   * @return 计算完成后的目标值
   */
  public T apply() {
    // 先读取一次缓存，已初始化则直接返回
    T value = cached;
    if (value != null) {
      return value;
    }
    // 读取计算函数，若不为空则尝试计算
    Function0<T> f = compute;
    if (f != null) {
      // 执行计算函数得到新值
      T newValue = f.apply();
      // 检查约束：计算函数不允许返回null
      assert newValue != null: "compute function cannot return null.";
      // CAS尝试将新值原子写入缓存，仅一个线程会成功
      HANDLE.compareAndSet(this, null, newValue);
      // 置空计算函数，让闭包对象可以被垃圾回收
      compute = null; // allow closure to be GC'd
    }
    // 返回最终缓存的结果（无论哪个线程CAS成功，结果一致）
    return cached;
  }
}
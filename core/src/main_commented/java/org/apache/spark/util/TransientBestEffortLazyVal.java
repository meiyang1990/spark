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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * 文件说明：无锁实现的尽最大努力延迟初始化变量，缓存值标记为transient，序列化时仅保留计算函数
 *
 * A lock-free implementation of a lazily-initialized variable.
 * If there are concurrent initializations then the `compute()` function may be invoked
 * multiple times. However, only a single `compute()` result will be stored and all readers
 * will receive the same result object instance.
 *
 * This may be helpful for avoiding deadlocks in certain scenarios where exactly-once
 * value computation is not a hard requirement.
 *
 * The main difference between this and [[BestEffortLazyVal]] is that:
 * [[BestEffortLazyVal]] serializes the cached value after computation, while
 * [[TransientBestEffortLazyVal]] always serializes the compute function.
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
 */
public class TransientBestEffortLazyVal<T> implements Serializable {
  // 用于计算延迟值的函数，初始化后可置空（此处保留引用保持可序列化）
  private volatile Function0<T> compute;
  // 缓存计算结果，标记为transient，反序列化后会重新计算
  protected transient volatile T cached;

  // 用于CAS操作修改缓存值的VarHandle句柄
  private static final VarHandle HANDLE;
  static {
    try {
      // 通过方法句柄查找获取cached字段的VarHandle，用于无锁CAS操作
      HANDLE = MethodHandles.lookup()
        .in(TransientBestEffortLazyVal.class)
        .findVarHandle(TransientBestEffortLazyVal.class, "cached", Object.class);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize VarHandle", e);
    }
  }

  /**
   * 构造函数，传入延迟值的计算函数
   * @param compute 用于计算最终值的无参函数
   */
  public TransientBestEffortLazyVal(Function0<T> compute) {
    this.compute = compute;
  }

  /**
   * 获取延迟初始化的值，第一次调用会触发计算，后续返回缓存值
   * @return 计算完成后的最终值
   */
  public T apply() {
    T value = cached;
    if (value != null) {
      return value;
    }
    // 调用计算函数生成新值，并发场景下可能多次执行
    T newValue = compute.apply();
    assert newValue != null: "compute function cannot return null.";
    // CAS原子更新缓存值，保证只有一个值会被成功存储
    HANDLE.compareAndSet(this, null, newValue);
    return cached;
  }

  /**
   * Java自定义反序列化逻辑，反序列化后重置缓存值为空，触发重新计算
   * @param ois 对象输入流
   * @throws IOException 反序列化IO异常
   * @throws ClassNotFoundException 找不到对应类异常
   */
  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    ois.defaultReadObject();
    cached = null;
  }
}
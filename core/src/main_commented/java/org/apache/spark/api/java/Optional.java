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

package org.apache.spark.api.java;

import java.io.Serializable;
import java.util.Objects;

/**
 * 文件级注释：Spark Java API 的可选值容器类，兼容Java 8+和Guava的Optional接口，用于避免返回null
 * <p>Like {@code java.util.Optional} in Java 8, {@code scala.Option} in Scala, and
 * {@code com.google.common.base.Optional} in Google Guava, this class represents a
 * value of a given type that may or may not exist. It is used in methods that wish
 * to optionally return a value, in preference to returning {@code null}.</p>
 *
 * <p>In fact, the class here is a reimplementation of the essential API of both
 * {@code java.util.Optional} and {@code com.google.common.base.Optional}. From
 * {@code java.util.Optional}, it implements:</p>
 *
 * <ul>
 *   <li>{@link #empty()}</li>
 *   <li>{@link #of(Object)}</li>
 *   <li>{@link #ofNullable(Object)}</li>
 *   <li>{@link #get()}</li>
 *   <li>{@link #orElse(Object)}</li>
 *   <li>{@link #isPresent()}</li>
 * </ul>
 *
 * <p>From {@code com.google.common.base.Optional} it implements:</p>
 *
 * <ul>
 *   <li>{@link #absent()}</li>
 *   <li>{@link #of(Object)}</li>
 *   <li>{@link #fromNullable(Object)}</li>
 *   <li>{@link #get()}</li>
 *   <li>{@link #or(Object)}</li>
 *   <li>{@link #orNull()}</li>
 *   <li>{@link #isPresent()}</li>
 * </ul>
 *
 * <p>{@code java.util.Optional} itself was not used because at the time, the
 * project did not require Java 8. Using {@code com.google.common.base.Optional}
 * has in the past caused serious library version conflicts with Guava that can't
 * be resolved by shading. Hence this work-alike clone.</p>
 *
 * @param <T> type of value held inside
 */
/**
 * 可选值容器类，用于表示可能存在也可能不存在的值，避免返回null
 * 同时兼容Java 8 java.util.Optional和Guava Optional的常用API，解决依赖冲突问题
 * 实现了Serializable接口，支持Spark序列化
 * @param <T> 容器内部存储的值的类型
 */
public final class Optional<T> implements Serializable {

  // 全局空Optional单例，复用空实例避免重复创建
  private static final Optional<?> EMPTY = new Optional<>();

  // 存储的实际值，为空时为null
  private final T value;

  // 私有构造方法，创建空Optional实例
  private Optional() {
    this.value = null;
  }

  // 私有构造方法，创建包含非空值的Optional实例
  private Optional(T value) {
    this.value = Objects.requireNonNull(value);
  }

  // java.util.Optional API (subset)

  /**
   * 获取一个空的Optional实例
   * @param <T> 期望的值类型
   * @return 空的Optional实例
   */
  public static <T> Optional<T> empty() {
    @SuppressWarnings("unchecked")
    Optional<T> t = (Optional<T>) EMPTY;
    return t;
  }

  /**
   * 创建一个包含非空值的Optional实例
   * @param value 要包装的非空值
   * @param <T> 值的类型
   * @return 包装了给定值的Optional实例
   * @throws NullPointerException 如果输入值为null则抛出异常
   */
  public static <T> Optional<T> of(T value) {
    return new Optional<>(value);
  }

  /**
   * 根据可为null的输入值创建Optional，输入为null则返回空实例
   * @param value 可以为null的输入值
   * @param <T> 值的类型
   * @return 如果输入非空返回包装后的实例，否则返回空实例
   */
  public static <T> Optional<T> ofNullable(T value) {
    if (value == null) {
      return empty();
    } else {
      return of(value);
    }
  }

  /**
   * 获取Optional中包装的值，如果为空则抛出异常
   * @return 包装的值
   * @throws NullPointerException 如果当前Optional为空则抛出异常
   */
  public T get() {
    return Objects.requireNonNull(value);
  }

  /**
   * 如果当前有值则返回值，否则返回给定的默认值
   * @param other 当前为空时返回的默认值
   * @return 当前值或默认值
   */
  public T orElse(T other) {
    return value != null ? value : other;
  }

  /**
   * 判断当前Optional是否包含非空值
   * @return 如果包含非空值返回true，否则返回false
   */
  public boolean isPresent() {
    return value != null;
  }

  // Guava API (subset)
  // of(), get() and isPresent() are identically present in the Guava API

  /**
   * Guava兼容API：获取空Optional实例，等价于empty()
   * @param <T> 期望的值类型
   * @return 空的Optional实例
   */
  public static <T> Optional<T> absent() {
    return empty();
  }

  /**
   * Guava兼容API：根据可为null的输入创建Optional，等价于ofNullable()
   * @param value 可以为null的输入值
   * @param <T> 值的类型
   * @return 包装后的Optional实例，输入为null则返回空实例
   */
  public static <T> Optional<T> fromNullable(T value) {
    return ofNullable(value);
  }

  /**
   * Guava兼容API：如果有值返回值，否则返回默认值，等价于orElse()
   * @param other 当前为空时返回的默认值
   * @return 当前值或默认值
   */
  public T or(T other) {
    return value != null ? value : other;
  }

  /**
   * Guava兼容API：如果有值返回值，否则返回null
   * @return 当前值或null
   */
  public T orNull() {
    return value;
  }

  // Common methods

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Optional<?> other)) {
      return false;
    }
    return Objects.equals(value, other.value);
  }

  @Override
  public int hashCode() {
    return value == null ? 0 : value.hashCode();
  }

  @Override
  public String toString() {
    return value == null ? "Optional.empty" : String.format("Optional[%s]", value);
  }

}
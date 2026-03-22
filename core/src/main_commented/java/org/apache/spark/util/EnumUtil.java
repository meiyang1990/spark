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

import java.util.StringJoiner;

import org.apache.spark.annotation.Private;

/**
 * 枚举工具类，提供忽略大小写的枚举解析功能，属于Spark内部私有工具类
 */
@Private
public class EnumUtil {
  /**
   * 忽略大小写解析字符串为对应枚举实例
   * @param clz 枚举类类型
   * @param str 需要解析的字符串
   * @return 匹配到的枚举实例，输入字符串为null时返回null
   * @throws IllegalArgumentException 未找到匹配枚举时抛出异常
   */
  public static <E extends Enum<E>> E parseIgnoreCase(Class<E> clz, String str) {
    E[] constants = clz.getEnumConstants();
    if (str == null) {
      return null;
    }
    for (E e : constants) {
      if (e.name().equalsIgnoreCase(str)) {
        return e;
      }
    }
    throw new IllegalArgumentException(
      String.format("Illegal type='%s'. Supported type values: %s",
        str, joinToString(constants)));
  }

  /**
   * 将所有枚举名称拼接为逗号分隔的字符串，用于异常信息展示
   * @param enums 枚举数组
   * @return 拼接后的字符串
   */
  private static <E extends Enum<E>> String joinToString(E[] enums) {
    StringJoiner stringJoiner = new StringJoiner(", ");
    for (E e : enums) {
      stringJoiner.add(e.name());
    }
    return stringJoiner.toString();
  }
}
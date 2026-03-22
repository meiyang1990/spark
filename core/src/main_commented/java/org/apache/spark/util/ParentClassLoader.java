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

/**
 * 暴露ClassLoader中protected方法的自定义类加载器，供Spark内部动态类加载场景使用
 * <p>
 * 通过继承父类加载器，开放原本受保护的类查找和加载方法，支持Spark的隔离类加载机制
 * </p>
 */
public class ParentClassLoader extends ClassLoader {

  /**
   * 静态初始化，注册当前类加载器支持并行类加载
   */
  static {
    ClassLoader.registerAsParallelCapable();
  }

  /**
   * 以指定父类加载器构造父类加载器包装实例
   * @param parent 父类加载器
   */
  public ParentClassLoader(ClassLoader parent) {
    super(parent);
  }

  /**
   * 开放父类的findClass方法，根据名称查找类
   * @param name 类的全限定名
   * @return 找到的Class对象
   * @throws ClassNotFoundException 如果类未找到
   */
  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    return super.findClass(name);
  }

  /**
   * 开放父类的loadClass方法，加载指定名称的类并可选解析
   * @param name 类的全限定名
   * @param resolve 是否执行类解析
   * @return 加载得到的Class对象
   * @throws ClassNotFoundException 如果类未找到
   */
  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    return super.loadClass(name, resolve);
  }
}
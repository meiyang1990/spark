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

import java.net.URL;
import java.net.URLClassLoader;

/**
 * 可变URL类加载器，开放URLClassLoader中原本是protected的addURL方法，支持运行时动态添加新的类路径URL。
 * 在Spark中用于动态加载用户JAR包、扩展依赖，满足Spark应用运行时动态加载额外类资源的需求。
 */
public class MutableURLClassLoader extends URLClassLoader {

  /**
   * 注册该类加载器支持并行加载，提升多线程环境下类加载性能，避免类加载死锁。
   */
  static {
    ClassLoader.registerAsParallelCapable();
  }

  /**
   * 构造方法，初始化可变URL类加载器。
   * @param urls 初始需要加载的URL数组
   * @param parent 父类加载器，遵循双亲委派模型
   */
  public MutableURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  /**
   * 重写addURL方法，将其开放为public方法，支持动态添加新的类路径URL。
   * @param url 需要添加的类路径URL
   */
  @Override
  public void addURL(URL url) {
    super.addURL(url);
  }
}
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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

/**
 * 文件: ChildFirstURLClassLoader.java
 * 所属模块: Spark core 核心工具模块
 * 核心功能: 实现子优先的类加载器，加载类和资源时优先使用当前类加载器的路径，而非委托给父类加载器
 * 应用场景: 用于解决 Spark 应用中用户依赖版本与 Spark 自身依赖版本冲突问题，保证用户作业的优先加载权
 */
public class ChildFirstURLClassLoader extends MutableURLClassLoader {

  // 注册当前类加载器支持并行类加载，提升多线程下类加载性能
  static {
    ClassLoader.registerAsParallelCapable();
  }

  // 封装父类加载器，用于当前类加载找不到时回退查找
  private ParentClassLoader parent;

  /**
   * 构造函数，创建子优先类加载器，加载顺序: child(当前urls) -> parent
   * @param urls 子加载器需要加载的类路径URL数组
   * @param parent 父类加载器，当前找不到时委托给它
   */
  public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, null);
    this.parent = new ParentClassLoader(parent);
  }

  /**
   * 构造函数，创建三级加载顺序的子优先类加载器，加载顺序: grandparent -> child(当前urls) -> parent
   * 用于需要跳过中间父加载器、优先从祖先加载器加载核心类的场景
   * @param urls 子加载器需要加载的类路径URL数组
   * @param parent 中间父类加载器，最后才委托它加载
   * @param grandparent 祖父类加载器，最先委托它加载核心类
   */
  public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent, ClassLoader grandparent) {
    super(urls, grandparent);
    this.parent = new ParentClassLoader(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // 先尝试从当前类加载器(及祖父，如果配置了)加载
    try {
      return super.loadClass(name, resolve);
    } catch (ClassNotFoundException cnf) {
      // 当前找不到，回退到父类加载器加载
      return parent.loadClass(name, resolve);
    }
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    // 先收集当前类加载器中的资源，再收集父类加载器中的资源，保证当前资源优先
    ArrayList<URL> urls = Collections.list(super.getResources(name));
    urls.addAll(Collections.list(parent.getResources(name)));
    return Collections.enumeration(urls);
  }

  @Override
  public URL getResource(String name) {
    // 先从当前类加载器查找资源，找到直接返回，找不到再去父类加载器查找
    URL url = super.getResource(name);
    if (url != null) {
      return url;
    } else {
      return parent.getResource(name);
    }
  }
}
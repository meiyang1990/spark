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

package org.apache.spark.deploy

import java.lang.reflect.Modifier

import org.apache.spark.SparkConf

/**
 * 文件概述: Spark应用启动入口抽象，定义了Spark应用启动的统一接口，供不同类型的应用实现
 */

/**
 * Spark应用入口 trait，定义Spark应用启动的统一接口
 * 所有实现类必须提供无参构造方法，供Spark部署模块反射实例化
 */
private[spark] trait SparkApplication {

  /**
   * 启动Spark应用的入口方法
   * @param args 应用启动参数数组
   * @param conf Spark配置对象
   */
  def start(args: Array[String], conf: SparkConf): Unit

}

/**
 * 包装标准Java主类的SparkApplication实现类
 * 通过反射调用Java类的静态main方法启动应用，配置通过系统属性传递
 * 注意：同一个JVM中运行多个此类实例可能会因为配置泄漏导致未定义行为
 */
private[deploy] class JavaMainApplication(klass: Class[_]) extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    // 反射获取目标类的main方法
    val mainMethod = klass.getMethod("main", new Array[String](0).getClass)
    // 检查main方法必须是静态的
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    // 将Spark配置转换为系统属性，传递给Java应用
    val sysProps = conf.getAll.toMap
    sysProps.foreach { case (k, v) =>
      sys.props(k) = v
    }

    // 反射调用静态main方法启动应用
    mainMethod.invoke(null, args)
  }

}
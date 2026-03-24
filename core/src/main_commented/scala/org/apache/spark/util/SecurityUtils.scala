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

package org.apache.spark.util

/**
 * 文件说明：Spark安全模块通用工具类，提供Kerberos认证相关的适配工具，
 * 用于兼容不同厂商JVM（IBM/Oracle OpenJDK）的Kerberos配置差异
 */
private[spark] object SecurityUtils {
  private val JAVA_VENDOR = "java.vendor"
  private val IBM_KRB_DEBUG_CONFIG = "com.ibm.security.krb5.Krb5Debug"
  private val SUN_KRB_DEBUG_CONFIG = "sun.security.krb5.debug"

  /**
   * 设置全局Kerberos调试日志开关，根据JVM厂商适配不同系统属性
   * @param enabled 是否开启Kerberos调试
   */
  def setGlobalKrbDebug(enabled: Boolean): Unit = {
    if (enabled) {
      if (isIBMVendor()) {
        System.setProperty(IBM_KRB_DEBUG_CONFIG, "all")
      } else {
        System.setProperty(SUN_KRB_DEBUG_CONFIG, "true")
      }
    } else {
      if (isIBMVendor()) {
        System.clearProperty(IBM_KRB_DEBUG_CONFIG)
      } else {
        System.clearProperty(SUN_KRB_DEBUG_CONFIG)
      }
    }
  }

  /**
   * 检查全局Kerberos调试是否已开启，从环境变量读取配置并适配不同JVM厂商
   * @return 调试是否开启
   */
  def isGlobalKrbDebugEnabled(): Boolean = {
    if (isIBMVendor()) {
      val debug = System.getenv(IBM_KRB_DEBUG_CONFIG)
      debug != null && debug.equalsIgnoreCase("all")
    } else {
      val debug = System.getenv(SUN_KRB_DEBUG_CONFIG)
      debug != null && debug.equalsIgnoreCase("true")
    }
  }

  /**
   * 获取适配当前JVM厂商的Kerberos登录模块类全限定名
   * 不同JVM厂商的Krb5LoginModule实现类包路径不同，需要动态适配
   * 参考Hadoop UserGroupInformation实现的兼容逻辑
   * @return Krb5LoginModule全限定类名
   */
  def getKrb5LoginModuleName(): String = {
    if (isIBMVendor()) {
      "com.ibm.security.auth.module.Krb5LoginModule"
    } else {
      "com.sun.security.auth.module.Krb5LoginModule"
    }
  }

  /**
   * 判断当前运行的JVM是否为IBM厂商版本
   * @return 是否IBM JVM
   */
  private def isIBMVendor(): Boolean = {
    System.getProperty(JAVA_VENDOR).contains("IBM")
  }
}
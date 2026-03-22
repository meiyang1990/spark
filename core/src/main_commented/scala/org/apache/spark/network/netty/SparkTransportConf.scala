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

package org.apache.spark.network.netty

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SSLOptions}
import org.apache.spark.network.util.{ConfigProvider, NettyUtils, TransportConf}

/**
 * 文件级注释：Spark Netty传输层配置转换工具，将Spark应用级的SparkConf转换为底层网络传输模块使用的TransportConf
 * 
 * 提供从Spark JVM（Driver、Executor、独立Shuffle服务等）内的SparkConf转换为TransportConf的工具能力，
 * 会根据当前JVM分配的核心数自动设置合理的IO线程数默认值
 */
object SparkTransportConf {

  /**
   * 从SparkConf构造底层网络传输模块使用的TransportConf
   * @param _conf 源Spark配置对象
   * @param module 目标网络模块名称
   * @param numUsableCores 当前JVM可用核心数，如果非零，会基于该值计算默认IO线程数，仅当用户未显式配置时生效；为0则使用机器全部核心数
   * @param role 当前节点角色，可选值为driver、executor、worker、master，默认为None表示不使用角色特定配置
   * @param sslOptions SSL配置选项，可选
   * @return 构造完成的TransportConf，供底层网络传输模块使用
   */
  def fromSparkConf(
      _conf: SparkConf,
      module: String,
      numUsableCores: Int = 0,
      role: Option[String] = None,
      sslOptions: Option[SSLOptions] = None): TransportConf = {
    val conf = _conf.clone
    // 根据当前JVM分配的核心数设置默认线程配置，而非默认使用机器全部核心
    val numThreads = NettyUtils.defaultNumThreads(numUsableCores)
    // 按优先级覆盖线程配置：角色特定配置 > 模块配置 > 默认配置
    Seq("serverThreads", "clientThreads").foreach { suffix =>
      val value = role.flatMap { r => conf.getOption(s"spark.$r.$module.io.$suffix") }
        .getOrElse(
          conf.get(s"spark.$module.io.$suffix", numThreads.toString))
      conf.set(s"spark.$module.io.$suffix", value)
    }

    // 如果有SSL配置则使用SSL生成的配置提供器，否则创建默认基于SparkConf的配置提供器
    val configProvider = sslOptions.map(_.createConfigProvider(conf)).getOrElse(
      new ConfigProvider {
        override def get(name: String): String = conf.get(name)
        override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
        override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
          conf.getAll.toMap.asJava.entrySet()
        }
      })
    new TransportConf(module, configProvider)
  }
}
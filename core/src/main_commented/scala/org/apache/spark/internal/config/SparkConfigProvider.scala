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
package org.apache.spark.internal.config

import java.util.{Map => JMap}

import org.apache.spark.SparkConf

/**
 * 文件描述: Spark配置提供者实现，仅负责读取以spark.开头的配置项
 * 核心职责: 提供Spark原生配置项的读取能力，同时兼容已废弃配置名的查找逻辑
 */
/**
 * 仅读取Spark配置项的配置提供者
 * 仅返回以"spark."开头的配置，过滤其他非Spark原生配置
 * @param conf 存储配置键值对的Java Map对象
 */
private[spark] class SparkConfigProvider(conf: JMap[String, String]) extends ConfigProvider {

  /**
   * 根据配置键获取配置值
   * @param key 要查询的配置键
   * @return 配置值的Option对象，非Spark配置或不存在则返回None
   */
  override def get(key: String): Option[String] = {
    if (key.startsWith("spark.")) {
      Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key, conf))
    } else {
      None
    }
  }
}
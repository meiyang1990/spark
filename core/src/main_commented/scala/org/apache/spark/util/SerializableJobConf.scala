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

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.mapred.JobConf

/**
 * 可序列化的Hadoop JobConf包装类
 * 解决原生JobConf的Java序列化问题，通过自定义序列化逻辑实现分布式环境下的JobConf传递
 * 仅对Spark核心模块内部开放，用于Hadoop相关任务调度中传递作业配置
 * @param value 被包装的原始Hadoop JobConf对象，标记为transient避免默认序列化
 */
private[spark]
class SerializableJobConf(@transient var value: JobConf) extends Serializable {

  /**
   * 自定义Java序列化逻辑，将JobConf内容写入输出流
   * @param out 对象输出流
   */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 执行默认序列化写入非transient字段
    out.defaultWriteObject()
    // 调用JobConf自身的序列化方法写入配置内容
    value.write(out)
  }

  /**
   * 自定义Java反序列化逻辑，从输入流读取重建JobConf
   * @param in 对象输入流
   */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    // 创建空的JobConf实例，不加载默认配置
    value = new JobConf(false)
    // 调用JobConf自身的反序列化方法读取配置内容
    value.readFields(in)
  }
}
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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Unstable}
import org.apache.spark.broadcast.Broadcast

/**
 * 可序列化包装类，解决Hadoop Configuration本身不可序列化的问题，用于在Spark任务中传递Hadoop配置
 * 核心文件级职责：为Hadoop Configuration提供Java序列化支持，方便在分布式任务间传递配置
 */
/**
 * 可序列化的Hadoop Configuration包装类，用于解决原生Configuration不可序列化问题，支持在分布式任务中传递配置
 * 使用 `value` 访问原始Hadoop配置实例
 *
 * @param value 原始Hadoop配置实例，标记为transient避免默认序列化
 */
@DeveloperApi @Unstable
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  /**
   * 自定义Java序列化逻辑，将Hadoop配置写入输出流
   */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // 执行默认序列化逻辑写入非transient字段
    out.defaultWriteObject()
    // 使用Hadoop Configuration自带方法写入配置内容
    value.write(out)
  }

  /**
   * 自定义Java反序列化逻辑，从输入流重建Hadoop配置实例
   */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    // 创建空的Configuration实例，不加载默认配置
    value = new Configuration(false)
    // 从输入流读取配置内容重建对象
    value.readFields(in)
  }
}

/**
 * 可序列化Hadoop配置工具对象，提供便捷的广播创建方法
 */
private[spark] object SerializableConfiguration {
  /**
   * 将传入的Hadoop配置包装为可序列化实例并创建广播变量，供集群所有节点共享配置
   * 
   * @param sc Spark上下文实例
   * @param conf 需要广播的Hadoop配置
   * @return 包装后可序列化配置的广播变量
   */
  def broadcast(sc: SparkContext, conf: Configuration): Broadcast[SerializableConfiguration] = {
    sc.broadcast(new SerializableConfiguration(conf))
  }

  /**
   * 将Spark上下文自带的Hadoop配置包装为可序列化实例并创建广播变量
   * 
   * @param sc Spark上下文实例
   * @return 包装后可序列化配置的广播变量
   */
  def broadcast(sc: SparkContext): Broadcast[SerializableConfiguration] = {
    broadcast(sc, sc.hadoopConfiguration)
  }
}
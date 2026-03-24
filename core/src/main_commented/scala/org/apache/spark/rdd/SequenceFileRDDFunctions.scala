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
package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileOutputFormat

import org.apache.spark.internal.{Logging, LogKeys}

/**
 * 文件级注释：为键值对RDD提供保存为Hadoop SequenceFile的扩展功能，通过隐式转换注入能力
 *
 * 为键值对类型RDD扩展生成Hadoop SequenceFile的功能，通过隐式转换提供能力。
 * 本类单独拆分的原因是需要额外的隐式参数完成键值对到Writable类型的转换，无法放入PairRDDFunctions。
 *
 * @param self 输入的键值对RDD
 * @param _keyWritableClass 输出SequenceFile中键对应的Hadoop Writable类型
 * @param _valueWritableClass 输出SequenceFile中值对应的Hadoop Writable类型
 * @tparam K 键的原始类型
 * @tparam V 值的原始类型
 */
class SequenceFileRDDFunctions[K: IsWritable: ClassTag, V: IsWritable: ClassTag](
    self: RDD[(K, V)],
    _keyWritableClass: Class[_ <: Writable],
    _valueWritableClass: Class[_ <: Writable])
  extends Logging
  with Serializable {

  /**
   * 将当前RDD保存为Hadoop SequenceFile格式，自动根据RDD键值类型推断对应Writable类型。
   * 如果键或值本身已经是Writable类型，直接使用；否则会将原始基本类型自动转换为对应Writable实现
   * （如Int转IntWritable、Double转DoubleWritable、字节数组转BytesWritable、字符串转Text等）。
   * 输出路径可位于任何Hadoop支持的文件系统。
   *
   * @param path 输出SequenceFile的存储路径
   * @param codec 可选压缩编码格式，默认不压缩
   */
  def saveAsSequenceFile(
      path: String,
      codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
    def anyToWritable[U: IsWritable](u: U): Writable = u

    // TODO 编译期无法强制约束anyToWritable返回类型与keyWritableClass/valueWritableClass一致
    // 要实现该约束需要为SequenceFileRDDFunctions添加额外类型参数，而SequenceFileRDDFunctions是公共类，修改会导致不兼容变更

    // 判断键类型是否需要转换为目标Writable类型
    val convertKey = self.keyClass != _keyWritableClass
    // 判断值类型是否需要转换为目标Writable类型
    val convertValue = self.valueClass != _valueWritableClass

    // 打印日志记录输出的SequenceFile键值类型
    logInfo(log"Saving as sequence file of type " +
      log"(${MDC(LogKeys.KEY, _keyWritableClass.getSimpleName)}," +
      log"${MDC(LogKeys.VALUE, _valueWritableClass.getSimpleName)})")
    // 定义SequenceFile输出格式类
    val format = classOf[SequenceFileOutputFormat[Writable, Writable]]
    // 基于Spark当前配置创建JobConf对象
    val jobConf = new JobConf(self.context.hadoopConfiguration)
    // 根据是否需要转换键值，分支处理保存逻辑
    if (!convertKey && !convertValue) {
      // 键值都无需转换，直接调用原生saveAsHadoopFile保存
      self.saveAsHadoopFile(path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    } else if (!convertKey && convertValue) {
      // 仅值需要转换，映射转换后保存
      self.map(x => (x._1, anyToWritable(x._2))).saveAsHadoopFile(
        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    } else if (convertKey && !convertValue) {
      // 仅键需要转换，映射转换后保存
      self.map(x => (anyToWritable(x._1), x._2)).saveAsHadoopFile(
        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    } else if (convertKey && convertValue) {
      // 键值都需要转换，映射转换后保存
      self.map(x => (anyToWritable(x._1), anyToWritable(x._2))).saveAsHadoopFile(
        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    }
  }
}
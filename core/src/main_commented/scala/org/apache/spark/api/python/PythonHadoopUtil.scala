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

package org.apache.spark.api.python

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.CLASS_NAME
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * 类型转换器特质，用于PySpark中自定义类型转换，子类通过覆盖convert方法实现自定义转换逻辑
 * @tparam T 输入类型
 * @tparam U 输出类型
 */
trait Converter[-T, +U] extends Serializable {
  def convert(obj: T): U
}

/**
 * 转换器工厂，用于通过反射加载用户自定义的转换器实例，供PythonHadoop模块使用
 */
private[python] object Converter extends Logging {

  /**
   * 获取转换器实例，优先加载用户自定义转换器，加载失败则抛出异常，无自定义转换器则返回默认转换器
   * @param converterClass 可选的自定义转换器全类名
   * @param defaultConverter 默认转换器实例
   * @return 可用的转换器实例
   */
  def getInstance[T, U](converterClass: Option[String],
                        defaultConverter: Converter[_ >: T, _ <: U]): Converter[T, U] = {
    converterClass.map { cc =>
      Try {
        val c = Utils.classForName[Converter[T, U]](cc).getConstructor().newInstance()
        logInfo(log"Loaded converter: ${MDC(CLASS_NAME, cc)}")
        c
      } match {
        case Success(c) => c
        case Failure(err) =>
          logError(log"Failed to load converter: ${MDC(CLASS_NAME, cc)}")
          throw err
      }
    }.getOrElse { defaultConverter }
  }
}

/**
 * 将Hadoop Writable类型转换为Java原生类型的转换器，供PySpark读取Hadoop数据时使用
 * 非Writable类型直接返回不做转换
 * @param conf 广播后的Hadoop配置对象，用于克隆自定义Writable
 */
private[python] class WritableToJavaConverter(
    conf: Broadcast[SerializableConfiguration]) extends Converter[Any, Any] {

  /**
   * 递归将Writable对象转换为对应Java原生类型
   * @param writable 输入Writable对象
   * @return 转换后的Java原生对象
   */
  private def convertWritable(writable: Writable): Any = {
    writable match {
      case iw: IntWritable => iw.get()
      case dw: DoubleWritable => dw.get()
      case lw: LongWritable => lw.get()
      case sw: ShortWritable => sw.get()
      case fw: FloatWritable => fw.get()
      case t: Text => t.toString
      case bw: BooleanWritable => bw.get()
      case byw: ByteWritable => byw.get()
      case byw: BytesWritable =>
        // 拷贝字节数组内容返回
        val bytes = new Array[Byte](byw.getLength)
        System.arraycopy(byw.getBytes(), 0, bytes, 0, byw.getLength)
        bytes
      case n: NullWritable => null
      case aw: ArrayWritable =>
        // 由于类型擦除，所有数组都作为Object[]处理，会被序列化为Python元组
        // 空数组无法确定元素类型，因此不转换为原生数组，用户可自定义转换器处理已知类型数组
        aw.get().map(convertWritable(_))
      case mw: MapWritable =>
        val map = new java.util.HashMap[Any, Any]()
        // 递归转换键和值
        mw.asScala.foreach { case (k, v) => map.put(convertWritable(k), convertWritable(v)) }
        map
      // 自定义Writable类型通过克隆返回新实例
      case w: Writable => WritableUtils.clone(w, conf.value.value)
      // 其他未知类型直接返回
      case other => other
    }
  }

  override def convert(obj: Any): Any = {
    obj match {
      case writable: Writable =>
        convertWritable(writable)
      case _ =>
        obj
    }
  }
}

/**
 * 将Java原生类型转换为Hadoop Writable类型的转换器，供PySpark写入Hadoop数据时使用
 * 原生数组类型不直接支持，用户需要继承ArrayWritable自定义类型来指定元素类型
 */
private[python] class JavaToWritableConverter extends Converter[Any, Writable] {

  /**
   * 将Java原生数据类型转换为对应Writable类型
   * @param obj 输入Java原生对象
   * @return 转换后的Writable对象
   */
  private def convertToWritable(obj: Any): Writable = {
    obj match {
      case i: java.lang.Integer => new IntWritable(i)
      case d: java.lang.Double => new DoubleWritable(d)
      case l: java.lang.Long => new LongWritable(l)
      case s: java.lang.Short => new ShortWritable(s)
      case f: java.lang.Float => new FloatWritable(f)
      case s: java.lang.String => new Text(s)
      case b: java.lang.Boolean => new BooleanWritable(b)
      case b: java.lang.Byte => new ByteWritable(b)
      case aob: Array[Byte] => new BytesWritable(aob)
      case null => NullWritable.get()
      case map: java.util.Map[_, _] =>
        val mapWritable = new MapWritable()
        // 递归转换键和值
        map.asScala.foreach { case (k, v) =>
          mapWritable.put(convertToWritable(k), convertToWritable(v))
        }
        mapWritable
      case array: Array[Any] =>
        // 转换为元素类型为Writable的ArrayWritable
        val arrayWriteable = new ArrayWritable(classOf[Writable])
        arrayWriteable.set(array.map(convertToWritable(_)))
        arrayWriteable
      // 不支持的类型抛出异常
      case other => throw new SparkException(
        s"Data of type ${other.getClass.getName} cannot be used")
    }
  }

  override def convert(obj: Any): Writable = obj match {
    case writable: Writable => writable
    case other => convertToWritable(other)
  }
}

/**
 * PySpark与Hadoop交互的工具类，提供配置转换和RDD类型转换能力
 */
private[python] object PythonHadoopUtil {

  /**
   * 将Java Map格式的配置转换为Hadoop Configuration对象
   * @param map 输入Java Map配置
   * @return 转换后的Hadoop Configuration
   */
  def mapToConf(map: java.util.Map[String, String]): Configuration = {
    val conf = new Configuration(false)
    map.asScala.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * 合并两个Hadoop配置，右侧配置会覆盖左侧配置中相同的键
   * @param left 左侧基础配置
   * @param right 右侧待合并配置
   * @return 合并后的新配置对象
   */
  def mergeConfs(left: Configuration, right: Configuration): Configuration = {
    val copy = new Configuration(left)
    right.asScala.foreach(entry => copy.set(entry.getKey, entry.getValue))
    copy
  }

  /**
   * 对RDD的键和值分别应用转换器，完成Writable与原生类型的相互转换
   * @param rdd 输入键值对RDD
   * @param keyConverter 键转换器
   * @param valueConverter 值转换器
   * @return 转换后的键值对RDD
   */
  def convertRDD[K, V](rdd: RDD[(K, V)],
                       keyConverter: Converter[K, Any],
                       valueConverter: Converter[V, Any]): RDD[(Any, Any)] = {
    rdd.map { case (k, v) => (keyConverter.convert(k), valueConverter.convert(v)) }
  }

}
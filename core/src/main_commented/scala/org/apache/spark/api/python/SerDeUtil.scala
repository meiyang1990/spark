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

import java.util.{ArrayList => JArrayList}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark.SparkException
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ArrayImplicits._

/**
 * PySpark Python-JVM 数据序列化/反序列化工具类，基于 Python Pickle 协议实现数据转换
 * 核心职责是在 JVM 端和 Python 端之间完成对象的序列化与反序列化，支撑 PySpark 核心数据交互
 */
private[spark] object SerDeUtil extends Logging {
  /**
   * 自定义字节数组构造器，适配 Python 3 空字节数组的 Pickle 序列化格式
   * 解决 Python 3 序列化空字节数组后在 JVM 端反序列化失败的问题
   */
  class ByteArrayConstructor extends net.razorvine.pickle.objects.ByteArrayConstructor {
    override def construct(args: Array[Object]): Object = {
      // 处理 Python 3 序列化的空字节数组场景
      if (args.length == 0) {
        Array.emptyByteArray
      } else {
        super.construct(args)
      }
    }
  }

  private var initialized = false
  /**
   * 初始化 Python 数组反序列化构造器，必须在反序列化来自 Python 的 array.array 之前调用
   * 集群模式下需要在闭包中调用确保每个执行器都完成初始化
   */
  // This should be called before trying to unpickle array.array from Python
  // In cluster mode, this should be put in closure
  def initialize(): Unit = {
    synchronized {
      if (!initialized) {
        // 为不同Python版本的字节类型注册自定义构造器
        Unpickler.registerConstructor("__builtin__", "bytearray", new ByteArrayConstructor())
        Unpickler.registerConstructor("builtins", "bytearray", new ByteArrayConstructor())
        Unpickler.registerConstructor("__builtin__", "bytes", new ByteArrayConstructor())
        Unpickler.registerConstructor("_codecs", "encode", new ByteArrayConstructor())
        initialized = true
      }
    }
  }
  initialize()


  /**
   * 将 Java 对象 RDD 转换为数组类型 RDD，不做递归转换
   * 仅被 pyspark.sql 模块使用，用于适配 Python 端数据格式
   */
  def toJavaArray(jrdd: JavaRDD[Any]): JavaRDD[Array[_]] = {
    jrdd.rdd.map {
      case objs: JArrayList[_] =>
        objs.toArray
      case obj if obj.getClass.isArray =>
        obj.asInstanceOf[Array[_]].toArray
    }.toJavaRDD()
  }

  /**
   * 自动批量Pickle序列化器，根据序列化后的字节大小自动调整批次大小
   * 动态平衡网络传输 overhead 和单批数据大小，提升Python-JVM数据传输效率
   * @param iter 待序列化的JVM对象迭代器
   */
  private[spark] class AutoBatchedPickler(iter: Iterator[Any]) extends Iterator[Array[Byte]] {
    private val pickle = new Pickler(/* useMemo = */ true,
      /* valueCompare = */ false)
    private var batch = 1
    private val buffer = new mutable.ArrayBuffer[Any]

    override def hasNext: Boolean = iter.hasNext

    override def next(): Array[Byte] = {
      // 收集当前批次对象，直到达到当前批次大小上限
      while (iter.hasNext && buffer.length < batch) {
        buffer += iter.next()
      }
      // 对当前批次执行pickle序列化
      val bytes = pickle.dumps(buffer.toArray)
      val size = bytes.length
      // 根据序列化后大小动态调整下一批次大小：保持在1MB~10MB区间
      if (size < 1024 * 1024) {
        batch *= 2
      } else if (size > 1024 * 1024 * 10 && batch > 1) {
        batch /= 2
      }
      buffer.clear()
      bytes
    }
  }

  /**
   * 将 JVM Java 对象 RDD 转换为可被 PySpark 消费的 Python Pickle 序列化 RDD
   * 使用自动批量序列化优化传输效率
   * @param jRDD JVM端Java对象RDD
   * @return 序列化后的字节数组RDD，可被Python端反序列化
   */
  def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    jRDD.rdd.mapPartitions { iter => new AutoBatchedPickler(iter) }
  }

  /**
   * 将 Python Pickle 序列化的字节数组 RDD 反转换为 JVM 对象 RDD，供 PySpark 使用
   * @param pyRDD Python端序列化后的字节数组RDD
   * @param batched 是否为批量序列化格式
   * @return 反序列化后的JVM对象RDD
   */
  def pythonToJava(pyRDD: JavaRDD[Array[Byte]], batched: Boolean): JavaRDD[Any] = {
    pyRDD.rdd.mapPartitions { iter =>
      initialize()
      val unpickle = new Unpickler
      iter.flatMap { row =>
        val obj = unpickle.loads(row)
        if (batched) {
          obj match {
            case array: Array[Any] => array.toImmutableArraySeq
            case _ => obj.asInstanceOf[JArrayList[_]].asScala
          }
        } else {
          Seq(obj)
        }
      }
    }.toJavaRDD()
  }

  /**
   * 检查键值对是否可以被Pickle序列化，用于提前处理不可序列化场景
   * @param t 待检查的键值对
   * @return (键序列化是否失败, 值序列化是否失败)
   */
  private def checkPickle(t: (Any, Any)): (Boolean, Boolean) = {
    val pickle = new Pickler(/* useMemo = */ true,
      /* valueCompare = */ false)
    val kt = Try {
      pickle.dumps(t._1)
    }
    val vt = Try {
      pickle.dumps(t._2)
    }
    (kt, vt) match {
      case (Failure(kf), Failure(vf)) =>
        logWarning(log"""
               |Failed to pickle Java object as key:
               |${MDC(CLASS_NAME, t._1.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, kf.getMessage)}""".stripMargin)
        logWarning(log"""
               |Failed to pickle Java object as value:
               |${MDC(CLASS_NAME, t._2.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, vf.getMessage)}""".stripMargin)
        (true, true)
      case (Failure(kf), _) =>
        logWarning(log"""
               |Failed to pickle Java object as key:
               |${MDC(CLASS_NAME, t._1.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, kf.getMessage)}""".stripMargin)
        (true, false)
      case (_, Failure(vf)) =>
        logWarning(log"""
               |Failed to pickle Java object as value:
               |${MDC(CLASS_NAME, t._2.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, vf.getMessage)}""".stripMargin)
        (false, true)
      case _ =>
        (false, false)
    }
  }

  /**
   * 将键值对RDD转换为可被 PySpark 消费的 Python Pickle 序列化 RDD
   * 如果序列化失败，自动回退为调用toString获取字符串后再序列化
   * @param rdd JVM端键值对RDD
   * @param batchSize 批量大小，0表示使用自动批量调整
   * @return 序列化后的字节数组RDD
   */
  def pairRDDToPython(rdd: RDD[(Any, Any)], batchSize: Int): RDD[Array[Byte]] = {
    // 采样第一个元素判断键和值是否可以被序列化
    val (keyFailed, valueFailed) = rdd.take(1) match {
      case Array() => (false, false)
      case Array(first) => checkPickle(first)
    }

    rdd.mapPartitions { iter =>
      // 预处理：对不可序列化的键值转换为字符串
      val cleaned = iter.map { case (k, v) =>
        val key = if (keyFailed) k.toString else k
        val value = if (valueFailed) v.toString else v
        Array[Any](key, value)
      }
      // 根据批量配置选择不同序列化方式
      if (batchSize == 0) {
        new AutoBatchedPickler(cleaned)
      } else {
        val pickle = new Pickler(/* useMemo = */ true,
          /* valueCompare = */ false)
        cleaned.grouped(batchSize).map(batched => pickle.dumps(batched.asJava))
      }
    }
  }

  /**
   * 将 Python 序列化的键值对元组 RDD 转换为 JVM 端 (K, V) RDD
   * @param pyRDD Python端序列化后的字节数组RDD
   * @param batched 是否为批量序列化格式
   * @tparam K 键类型
   * @tparam V 值类型
   * @return JVM端键值对RDD
   */
  def pythonToPairRDD[K, V](pyRDD: RDD[Array[Byte]], batched: Boolean): RDD[(K, V)] = {
    /**
     * 判断对象是否为长度为2的非基本类型数组，符合键值对结构要求
     */
    def isPair(obj: Any): Boolean = {
      Option(obj.getClass.getComponentType).exists(!_.isPrimitive) &&
        obj.asInstanceOf[Array[_]].length == 2
    }

    // 先反序列化得到Java对象RDD
    val rdd = pythonToJava(pyRDD, batched).rdd
    // 采样检查数据格式是否符合键值对要求
    rdd.take(1) match {
      case Array(obj) if isPair(obj) =>
        // 格式正确，直接通过
      case Array() =>
        // 空集合也接受
      case Array(other) => throw new SparkException(
        s"RDD element of type ${other.getClass.getName} cannot be used")
    }
    // 转换为JVM键值对格式
    rdd.map { obj =>
      val arr = obj.asInstanceOf[Array[_]]
      (arr.head.asInstanceOf[K], arr.last.asInstanceOf[V])
    }
  }
}
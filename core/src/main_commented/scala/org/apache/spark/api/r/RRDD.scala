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

package org.apache.spark.api.r

import java.io.{File, OutputStream}
import java.nio.channels.{Channels, SocketChannel}
import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.security.SocketAuthServer
import org.apache.spark.util.ArrayImplicits._

/**
 * 所有SparkR RDD的抽象基类，负责执行R语言定义的函数并返回计算结果
 * 为SparkR提供底层RDD执行框架，封装R函数调用和序列化反序列化逻辑
 * @tparam T 输入RDD元素类型
 * @tparam U 输出RDD元素类型
 */
private abstract class BaseRRDD[T: ClassTag, U: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]])
  extends RDD[U](parent) with Logging {
  override def getPartitions: Array[Partition] = parent.partitions

  override def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    // 创建R语言执行器，加载序列化后的R函数和配置
    val runner = new RRunner[T, U](
      func, deserializer, serializer, packageNames, broadcastVars, numPartitions)

    // 递归触发父RDD计算，保证前置依赖执行完成
    val parentIterator = firstParent[T].iterator(partition, context)

    // 调用R执行器计算当前分区，返回结果迭代器
    runner.compute(parentIterator, partition.index)
  }
}

/**
 * SparkR shuffle操作专用RDD，将R返回的键值对组织为(Int, Array[Byte])格式
 * 用于SparkR shuffle阶段的分区数据传输，通过int键实现分区排序
 */
private class PairwiseRRDD[T: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    hashFunc: Array[Byte],
    deserializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Object])
  extends BaseRRDD[T, (Int, Array[Byte])](
    parent, numPartitions, hashFunc, deserializer,
    SerializationFormats.BYTE, packageNames,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {
  // 转换为JavaPairRDD供R端调用
  lazy val asJavaPairRDD : JavaPairRDD[Int, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

/**
 * 存储R序列化对象的RDD，每个元素为序列化后的R对象字节数组
 * 用于SparkR中R函数返回结果的分布式存储
 */
private class RRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Object])
  extends BaseRRDD[T, Array[Byte]](
    parent, -1, func, deserializer, serializer, packageNames,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {
  // 转换为JavaRDD供R端调用
  lazy val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}

/**
 * 存储R字符串对象的RDD，每个元素为R对象对应的字符串
 * 用于文本格式的R结果输出
 */
private class StringRRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Object])
  extends BaseRRDD[T, String](
    parent, -1, func, deserializer, SerializationFormats.STRING, packageNames,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {
  // 转换为JavaRDD供R端调用
  lazy val asJavaRDD : JavaRDD[String] = JavaRDD.fromRDD(this)
}

/**
 * SparkR RDD工具对象，提供从R语言创建SparkContext和RDD的工厂方法
 * 是R语言客户端连接Spark核心引擎的入口工具类
 */
private[spark] object RRDD {
  /**
   * 从R语言参数创建JavaSparkContext，供SparkR初始化使用
   * @param master 集群主节点地址
   * @param appName 应用名称
   * @param sparkHome Spark安装目录
   * @param jars 依赖jar包路径数组
   * @param sparkEnvirMap Spark配置参数映射表
   * @param sparkExecutorEnvMap 执行器环境变量映射表
   * @return 初始化完成的JavaSparkContext实例
   */
  def createSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Array[String],
      sparkEnvirMap: JMap[Object, Object],
      sparkExecutorEnvMap: JMap[Object, Object]): JavaSparkContext = {
    // 创建Spark配置对象，设置基础参数
    val sparkConf = new SparkConf().setAppName(appName)
                                   .setSparkHome(sparkHome)

    // 用户指定了master则覆盖默认配置
    if (master != "") {
      sparkConf.setMaster(master)
    } else {
      // 未配置master时默认使用local模式，保持向后兼容
      sparkConf.setIfMissing("spark.master", "local")
    }

    // 将R传入的Spark配置参数设置到SparkConf
    for ((name, value) <- sparkEnvirMap.asScala) {
      sparkConf.set(name.toString, value.toString)
    }
    // 将R传入的执行器环境变量设置到SparkConf
    for ((name, value) <- sparkExecutorEnvMap.asScala) {
      sparkConf.setExecutorEnv(name.toString, value.toString)
    }

    // 配置Derby日志路径，用于SparkR SQL场景
    if (sparkEnvirMap.containsKey("spark.r.sql.derby.temp.dir") &&
        System.getProperty("derby.stream.error.file") == null) {
      // 必须在SparkContext实例化前设置该系统属性
      System.setProperty("derby.stream.error.file",
                         Seq(sparkEnvirMap.get("spark.r.sql.derby.temp.dir").toString, "derby.log")
                         .mkString(File.separator))
    }

    // 获取或创建SparkContext，包装为JavaSparkContext
    val jsc = new JavaSparkContext(SparkContext.getOrCreate(sparkConf))
    // 添加R依赖的jar包到上下文中
    jars.foreach { jar =>
      jsc.addJar(jar)
    }
    jsc
  }

  /**
   * 从内存字节数组创建RDD，供SparkR parallelize小批量R对象时使用
   * @param jsc JavaSparkContext实例
   * @param arr R序列化后的字节数组集合
   * @return 包含所有R对象的JavaRDD
   */
  def createRDDFromArray(jsc: JavaSparkContext, arr: Array[Array[Byte]]): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(jsc.sc.parallelize(arr.toImmutableArraySeq, arr.length))
  }

  /**
   * 从临时文件创建RDD，供SparkR parallelize大型R对象时使用
   * @param jsc JavaSparkContext实例
   * @param fileName 驱动节点上的临时文件路径
   * @param parallelism 并行度/分区数，默认4
   * @return 包含所有R对象的JavaRDD
   */
  def createRDDFromFile(jsc: JavaSparkContext, fileName: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromFile(jsc, fileName, parallelism)
  }

  /**
   * 通过授权套接字服务将R数据写出到流，供SparkR并行化大对象时使用
   * @param threadName 服务线程名称
   * @param writeFunc 写出数据到输出流的函数
   * @return 服务信息数组，包含地址和端口
   */
  private[spark] def serveToStream(
      threadName: String)(writeFunc: OutputStream => Unit): Array[Any] = {
    SocketAuthServer.serveToStream(threadName, new RAuthHelper(SparkEnv.get.conf))(writeFunc)
  }
}

/**
 * R并行化服务器，通过套接字从R读取序列化数据创建RDD[Array[Byte]]
 * 加密启用时优先使用此方式，替代临时文件方式传输R数据
 * @param sc JavaSparkContext实例
 * @param parallelism 目标RDD并行度
 */
private[spark] class RParallelizeServer(sc: JavaSparkContext, parallelism: Int)
    extends SocketAuthServer[JavaRDD[Array[Byte]]](
      new RAuthHelper(SparkEnv.get.conf), "sparkr-parallelize-server") {

  override def handleConnection(sock: SocketChannel): JavaRDD[Array[Byte]] = {
    // 获取套接字输入流，从R端读取序列化数据
    val in = Channels.newInputStream(sock)
    // 从输入流读取数据创建RDD并返回
    JavaRDD.readRDDFromInputStream(sc.sc, in, parallelism)
  }
}
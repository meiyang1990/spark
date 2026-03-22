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

import java.sql.{Connection, ResultSet}

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.util.NextIterator

/**
 * JDBC RDD分区实现，每个分区对应一个查询范围
 * @param idx 分区索引
 * @param lower 分区查询下界
 * @param upper 分区查询上界
 */
private[spark] class JdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index: Int = idx
}

/**
 * 用于通过JDBC从关系型数据库读取数据的RDD实现，将数据按主键范围切分为多个分区并行读取。
 * 该类从Spark 4.1.0版本开始被废弃，推荐使用Spark SQL JDBC数据源替代。
 *
 * @param sc Spark上下文
 * @param getConnection 获取JDBC连接的工厂方法，RDD会负责关闭连接
 * @param sql 查询语句，必须包含两个?占位符用于分区范围参数，例如：
 *            select title, author from books where ? <= id and id <= ?
 * @param lowerBound 整个查询的最小下界
 * @param upperBound 整个查询的最大上界，上下界都是闭区间
 * @param numPartitions 分区数量，范围会被平均切分为多个分区
 * @param mapRow 将JDBC结果集行转换为目标类型对象的函数，默认转换为Object数组
 * @deprecated Jdbc RDD已废弃，请改用Spark SQL JDBC数据源
 */
@deprecated("Jdbc RDD is deprecated, consider using JDBC data source instead.", "4.1.0")
class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    // 上下界都是闭区间，因此长度需要加1
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map { i =>
      // 计算当前分区的范围边界
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      new JdbcPartition(i, start.toLong, end.toLong)
    }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T]
  {
    // 任务完成时关闭资源
    context.addTaskCompletionListener[Unit]{ context => closeIfNeeded() }
    val part = thePart.asInstanceOf[JdbcPartition]
    // 获取JDBC连接
    val conn = getConnection()
    // 创建只读向前的Statement
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val url = conn.getMetaData.getURL
    if (url.startsWith("jdbc:mysql:")) {
      // MySQL驱动特定设置：设置fetchSize为Integer.MIN_VALUE开启流式读取，避免一次性将全量结果加载到内存
      stmt.setFetchSize(Integer.MIN_VALUE)
    } else {
      // 其他数据库默认设置fetchSize为100
      stmt.setFetchSize(100)
    }

    logInfo(log"statement fetch size set to: ${MDC(FETCH_SIZE, stmt.getFetchSize)}")

    // 设置分区范围参数到查询占位符
    stmt.setLong(1, part.lower)
    stmt.setLong(2, part.upper)
    // 执行查询获取结果集
    val rs = stmt.executeQuery()

    override def getNext(): T = {
      if (rs.next()) {
        // 还有结果，调用mapRow转换当前行
        mapRow(rs)
      } else {
        // 结果遍历完成
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close(): Unit = {
      // 关闭结果集
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      // 关闭Statement
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      // 关闭连接
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
}

/**
 * JdbcRDD的伴生对象，提供结果集转换方法和Java API创建入口
 */
object JdbcRDD {
  /**
   * 默认的结果集转换函数，将当前行转换为Object数组
   * @param rs JDBC结果集
   * @return 包含所有列值的Object数组
   */
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  /**
   * JDBC连接工厂接口，支持序列化
   */
  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }

  /**
   * 为Java API创建JdbcRDD，支持自定义行转换函数
   *
   * @param sc JavaSpark上下文
   * @param connectionFactory JDBC连接工厂，返回打开的连接，RDD会负责关闭
   * @param sql 查询语句，必须包含两个?占位符用于分区范围参数
   * @param lowerBound 整个查询的最小下界
   * @param upperBound 整个查询的最大上界，上下界都是闭区间
   * @param numPartitions 分区数量
   * @param mapRow 将ResultSet行转换为目标对象的Java函数
   * @return 封装后的JavaRDD
   */
  def create[T](
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val jdbcRdd = new JdbcRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      lowerBound,
      upperBound,
      numPartitions,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)

    new JavaRDD[T](jdbcRdd)(fakeClassTag)
  }

  /**
   * 为Java API创建JdbcRDD，使用默认行转换（每行转换为Object数组）
   *
   * @param sc JavaSpark上下文
   * @param connectionFactory JDBC连接工厂，返回打开的连接，RDD会负责关闭
   * @param sql 查询语句，必须包含两个?占位符用于分区范围参数
   * @param lowerBound 整个查询的最小下界
   * @param upperBound 整个查询的最大上界，上下界都是闭区间
   * @param numPartitions 分区数量
   * @return 封装后的JavaRDD，元素为Object数组
   */
  def create(
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }

    create(sc, connectionFactory, sql, lowerBound, upperBound, numPartitions, mapRow)
  }
}
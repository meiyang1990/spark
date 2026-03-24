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

package org.apache.spark.ui

import java.util.concurrent.Semaphore

import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config.SCHEDULER_MODE
import org.apache.spark.scheduler.SchedulingMode

// scalastyle:off
/**
 * 持续生成测试作业，用于测试Spark WebUI的各项功能展示（内部开发测试工具）
 *
 * 使用方式: ./bin/spark-class org.apache.spark.ui.UIWorkloadGenerator [master] [FIFO|FAIR] [#job set (4 jobs per set)]
 */
// scalastyle:on
/**
 * Spark UI压力测试与功能测试工具，持续生成不同特征的作业，验证WebUI对作业、任务状态展示的正确性
 */
private[spark] object UIWorkloadGenerator {

  // 生成数据的分区数量
  val NUM_PARTITIONS = 100
  // 作业间启动等待间隔（毫秒）
  val INTER_JOB_WAIT_MS = 5000

  /**
   * 工具主入口，启动UI测试负载生成器
   * @param args 命令行参数：[master地址] [调度模式] [作业组数，每组包含7个不同特征作业]
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      // scalastyle:off println
      println(
        "Usage: ./bin/spark-class org.apache.spark.ui.UIWorkloadGenerator " +
          "[master] [FIFO|FAIR] [#job set (4 jobs per set)]")
      // scalastyle:on println
      System.exit(1)
    }

    val conf = new SparkConf().setMaster(args(0)).setAppName("Spark UI tester")

    val schedulingMode = SchedulingMode.withName(args(1))
    conf.set(SCHEDULER_MODE, schedulingMode)
    val nJobSet = args(2).toInt
    val sc = new SparkContext(conf)

    /**
     * 设置作业本地属性，公平调度模式下配置调度池，同时设置作业描述
     * @param s 作业描述和调度池名称
     */
    def setProperties(s: String): Unit = {
      if (schedulingMode == SchedulingMode.FAIR) {
        sc.setLocalProperty(SparkContext.SPARK_SCHEDULER_POOL, s)
      }
      sc.setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, s)
    }

    // 生成测试基础数据RDD
    val baseData = sc.makeRDD(1 to NUM_PARTITIONS * 10, NUM_PARTITIONS)
    // 获取随机浮点数工具方法
    def nextFloat(): Float = new Random().nextFloat()

    // 定义不同特征的测试作业集合，每个作业包含描述和计算逻辑
    val jobs = Seq[(String, () => Long)](
      ("Count", () => baseData.count()),
      ("Cache and Count", () => baseData.map(x => x).cache().count()),
      ("Single Shuffle", () => baseData.map(x => (x % 10, x)).reduceByKey(_ + _).count()),
      ("Entirely failed phase", () => baseData.map { x => throw new Exception(); 1 }.count()),
      ("Partially failed phase", () => {
        baseData.map { x =>
          // 按概率计算任务失败比例
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            throw new Exception("This is a task failure")
          }
          1
        }.count()
      }),
      ("Partially failed phase (longer tasks)", () => {
        baseData.map { x =>
          // 失败任务执行100ms后再失败，模拟慢失败场景
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            Thread.sleep(100)
            throw new Exception("This is a task failure")
          }
          1
        }.count()
      }),
      ("Job with delays", () => baseData.map(x => Thread.sleep(100)).count())
    )

    // 信号量，用于等待所有作业线程执行完成
    val barrier = new Semaphore(-nJobSet * jobs.size + 1)

    // 按指定数量生成作业组
    (1 to nJobSet).foreach { _ =>
      // 遍历所有作业类型，每个作业启动独立线程异步提交
      for ((desc, job) <- jobs) {
        new Thread {
          override def run(): Unit = {
            // scalastyle:off println
            try {
              setProperties(desc)
              // 执行作业
              job()
              println("Job finished: " + desc)
            } catch {
              case e: Exception =>
                // 捕获作业异常，记录失败
                println("Job Failed: " + desc)
            } finally {
              // 释放信号量，表示当前作业完成
              barrier.release()
            }
            // scalastyle:on println
          }
        }.start
        // 间隔一段时间后再启动下一个作业，方便UI观察状态变化
        Thread.sleep(INTER_JOB_WAIT_MS)
      }
    }

    // 等待所有作业执行完成
    barrier.acquire()
    // 停止Spark上下文退出应用
    sc.stop()
  }
}
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

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{NEW_RDD_ID, RDD_CHECKPOINT_DIR, RDD_ID}
import org.apache.spark.internal.config.CLEANER_REFERENCE_TRACKING_CLEAN_CHECKPOINTS

/**
 * 可靠存储RDD检查点数据的实现类，将RDD数据写入可靠存储系统（如HDFS）。
 * 支持驱动程序故障重启后，基于已计算的检查点数据恢复状态，避免重复计算。
 * 核心职责：管理RDD检查点数据的持久化存储、路径生成和清理。
 */
private[spark] class ReliableRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  // 存储此RDD检查点数据的目录路径，指向可靠存储系统上的非本地路径
  private val cpDir: String =
    ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
      .map(_.toString)
      .getOrElse { throw SparkCoreErrors.mustSpecifyCheckpointDirError() }

  /**
   * 获取当前RDD检查点目录路径
   * @return 若已完成检查点则返回Some(目录路径)，否则返回None
   */
  def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
    if (isCheckpointed) {
      Some(cpDir)
    } else {
      None
    }
  }

  /**
   * 执行检查点操作：将RDD数据物化并写入可靠DFS存储
   * 在RDD第一次动作执行完成后立即调用此方法
   * @return 生成的检查点RDD，作为原RDD的新父RDD
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    // 将RDD写入检查点目录，生成新的ReliableCheckpointRDD
    val newRDD = ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)

    // 如果启用了检查点自动清理，注册新RDD的检查点数据到清理器，原RDD不可达时自动删除
    if (rdd.conf.get(CLEANER_REFERENCE_TRACKING_CLEAN_CHECKPOINTS)) {
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }

    logInfo(log"Done checkpointing RDD ${MDC(RDD_ID, rdd.id)}" +
      log" to ${MDC(RDD_CHECKPOINT_DIR, cpDir)}, new parent is RDD ${MDC(NEW_RDD_ID, newRDD.id)}")
    newRDD
  }

}

/**
 * ReliableRDDCheckpointData的伴生对象，提供检查点路径生成和清理的工具方法
 */
private[spark] object ReliableRDDCheckpointData extends Logging {

  /**
   * 生成指定RDD检查点数据的目录路径
   * @param sc Spark上下文
   * @param rddId RDD ID
   * @return 检查点目录路径Option，未设置检查点目录时返回None
   */
  def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }

  /**
   * 清理指定RDD对应的检查点数据文件
   * @param sc Spark上下文
   * @param rddId RDD ID
   */
  def cleanCheckpoint(sc: SparkContext, rddId: Int): Unit = {
    checkpointPath(sc, rddId).foreach { path =>
      path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
    }
  }
}
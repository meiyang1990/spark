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

import java.io.FileNotFoundException
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{BUFFER_SIZE, CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME, CHECKPOINT_COMPRESS}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * 从可靠存储上已写入的检查点文件读取数据的RDD，用于RDD检查点恢复
 * 此类仅对Spark内部开放，用于在检查点后替换原RDD，切断依赖链减少内存占用
 */
private[spark] class ReliableCheckpointRDD[T: ClassTag](
    sc: SparkContext,
    val checkpointPath: String,
    _partitioner: Option[Partitioner] = None
  ) extends CheckpointRDD[T](sc) {

  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val cpath = new Path(checkpointPath)
  @transient private val fs = cpath.getFileSystem(hadoopConf)
  // 广播Hadoop配置，避免每个任务重复序列化传输
  private val broadcastedConf = SerializableConfiguration.broadcast(sc, hadoopConf)

  // 快速失败：如果检查点目录不存在直接抛出异常
  require(fs.exists(cpath), s"Checkpoint directory does not exist: $checkpointPath")

  /**
   * 返回当前RDD读取数据的检查点文件目录
   */
  override val getCheckpointFile: Option[String] = Some(checkpointPath)

  override val partitioner: Option[Partitioner] = {
    // 如果传入了分区器直接使用，否则从检查点文件中读取分区器
    _partitioner.orElse {
      ReliableCheckpointRDD.readCheckpointedPartitionerFile(context, checkpointPath)
    }
  }

  /**
   * 根据检查点目录中的文件生成分区，每个检查点文件对应一个分区
   * 原RDD属于之前的应用，无法预先知道分区数量，因此根据目录中的part-*文件数量生成分区
   */
  protected override def getPartitions: Array[Partition] = {
    // 列出目录中所有文件，筛选出以part-开头的检查点分区文件
    val inputFiles = fs.listStatus(cpath)
      .map(_.getPath)
      .filter(_.getName.startsWith("part-"))
      .sortBy(_.getName.stripPrefix("part-").toInt)
    // 验证文件名顺序是否正确，快速失败校验
    inputFiles.zipWithIndex.foreach { case (path, i) =>
      if (path.getName != ReliableCheckpointRDD.checkpointFileName(i)) {
        throw SparkCoreErrors.invalidCheckpointFileError(path)
      }
    }
    // 根据文件数量生成对应的CheckpointRDD分区
    Array.tabulate(inputFiles.length)(i => new CheckpointRDDPartition(i))
  }

  // 检查点分区偏好位置缓存，用于数据本地性优化
  @transient private[spark] lazy val cachedPreferredLocations = CacheBuilder.newBuilder()
    // 根据配置设置缓存过期时间，单位分钟
    .expireAfterWrite(
      SparkEnv.get.conf.get(CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME).get,
      TimeUnit.MINUTES)
    .build(
      new CacheLoader[Partition, Seq[String]]() {
        override def load(split: Partition): Seq[String] = {
          getPartitionBlockLocations(split)
        }
      })

  // 获取指定分区对应检查点文件在HDFS上的块位置
  private def getPartitionBlockLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(
      new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index)))
    val locations = fs.getFileBlockLocations(status, 0, status.getLen)
    // 过滤掉localhost，只返回数据所在节点主机名
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")
  }

  // 缓存偏好位置过期时间配置
  private lazy val cachedExpireTime =
    SparkEnv.get.conf.get(CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME)

  /**
   * 返回指定分区的偏好位置（数据所在节点），用于数据本地性调度优化
   * 启用缓存时从缓存获取，否则直接从文件系统查询
   */
  protected override def getPreferredLocations(split: Partition): Seq[String] = {
    if (cachedExpireTime.isDefined && cachedExpireTime.get > 0) {
      cachedPreferredLocations.get(split)
    } else {
      getPartitionBlockLocations(split)
    }
  }

  /**
   * 读取指定分区对应的检查点文件，返回数据迭代器
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index))
    ReliableCheckpointRDD.readCheckpointFile(file, broadcastedConf, context)
  }

}

/**
 * ReliableCheckpointRDD的伴生对象，提供检查点写入和读取的工具方法
 */
private[spark] object ReliableCheckpointRDD extends Logging {

  /**
   * 生成指定分区索引对应的检查点文件名
   */
  private def checkpointFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }

  /**
   * 存储分区器的检查点文件名
   */
  private def checkpointPartitionerFileName(): String = {
    "_partitioner"
  }

  /**
   * 将原RDD写入可靠存储的检查点目录，返回代表该检查点的RDD
   * @param originalRDD 要写入检查点的原RDD
   * @param checkpointDir 检查点输出目录
   * @param blockSize HDFS块大小，主要用于测试
   * @return 代表检查点数据的ReliableCheckpointRDD
   */
  def writeRDDToCheckpointDirectory[T: ClassTag](
      originalRDD: RDD[T],
      checkpointDir: String,
      blockSize: Int = -1): ReliableCheckpointRDD[T] = {
    val checkpointStartTimeNs = System.nanoTime()

    val sc = originalRDD.sparkContext

    // 创建检查点输出目录
    val checkpointDirPath = new Path(checkpointDir)
    val fs = checkpointDirPath.getFileSystem(sc.hadoopConfiguration)
    if (!fs.mkdirs(checkpointDirPath)) {
      throw SparkCoreErrors.failToCreateCheckpointPathError(checkpointDirPath)
    }

    // 广播Hadoop配置，执行检查点写入任务
    val broadcastedConf = SerializableConfiguration.broadcast(sc)
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582)
    sc.runJob(originalRDD,
      writePartitionToCheckpointFile[T](checkpointDirPath.toString, broadcastedConf) _)

    // 如果原RDD有分区器，将分区器也写入检查点目录
    if (originalRDD.partitioner.nonEmpty) {
      writePartitionerToCheckpointDir(sc, originalRDD.partitioner.get, checkpointDirPath)
    }

    // 记录并打印检查点写入耗时
    val checkpointDurationMs =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - checkpointStartTimeNs)
    logInfo(log"Checkpointing took ${MDC(TOTAL_TIME, checkpointDurationMs)} ms.")

    // 读取检查点生成新的ReliableCheckpointRDD返回
    val newRDD = new ReliableCheckpointRDD[T](
      sc, checkpointDirPath.toString, originalRDD.partitioner)
    // 校验分区数量和原RDD一致
    if (newRDD.partitions.length != originalRDD.partitions.length) {
      throw SparkCoreErrors.checkpointRDDHasDifferentNumberOfPartitionsFromOriginalRDDError(
        originalRDD.id, originalRDD.partitions.length, newRDD.id, newRDD.partitions.length)
    }
    newRDD
  }

  /**
   * 将一个RDD分区的数据写入指定检查点文件，由执行任务在各个Executor上调用
   */
  def writePartitionToCheckpointFile[T: ClassTag](
      path: String,
      broadcastedConf: Broadcast[SerializableConfiguration],
      blockSize: Int = -1)(ctx: TaskContext, iterator: Iterator[T]): Unit = {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value)

    // 生成最终输出路径和临时输出路径（任务失败避免生成损坏文件）
    val finalOutputName = ReliableCheckpointRDD.checkpointFileName(ctx.partitionId())
    val finalOutputPath = new Path(outputDir, finalOutputName)
    val tempOutputPath = new Path(outputDir, s".$finalOutputName-attempt-${ctx.taskAttemptId()}")

    val bufferSize = env.conf.get(BUFFER_SIZE)

    // 创建输出流，根据配置决定是否启用压缩
    val fileOutputStream = if (blockSize < 0) {
      val fileStream = fs.create(tempOutputPath, false, bufferSize)
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedOutputStream(fileStream)
      } else {
        fileStream
      }
    } else {
      // 自定义块大小，主要用于测试场景
      fs.create(tempOutputPath, false, bufferSize,
        fs.getDefaultReplication(fs.getWorkingDirectory), blockSize)
    }
    // 使用Spark序列化器写入分区数据
    val serializer = env.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    // 写入数据，失败时清理临时文件
    Utils.tryWithSafeFinallyAndFailureCallbacks {
      serializeStream.writeAll(iterator)
    } (catchBlock = {
      val deleted = fs.delete(tempOutputPath, false)
      if (!deleted) {
        logInfo(log"Failed to delete tempOutputPath ${MDC(TEMP_OUTPUT_PATH, tempOutputPath)}.")
      }
    }, finallyBlock = {
      serializeStream.close()
    })

    // 写入成功后将临时文件重命名为最终文件，避免任务重试冲突
    if (!fs.rename(tempOutputPath, finalOutputPath)) {
      if (!fs.exists(finalOutputPath)) {
        logInfo(log"Deleting tempOutputPath ${MDC(TEMP_OUTPUT_PATH, tempOutputPath)}")
        fs.delete(tempOutputPath, false)
        throw SparkCoreErrors.checkpointFailedToSaveError(ctx.attemptNumber(), finalOutputPath)
      } else {
        // 已经有其他任务副本完成写入，不需要覆盖，删除临时文件即可
        logInfo(log"Final output path" +
          log" ${MDC(FINAL_OUTPUT_PATH, finalOutputPath)} already exists; not overwriting it")
        if (!fs.delete(tempOutputPath, false)) {
          logWarning(log"Error deleting ${MDC(PATH, tempOutputPath)}")
        }
      }
    }
  }

  /**
   * 将分区器写入检查点目录，尽力而为，写入失败仅打日志不中断流程
   */
  private def writePartitionerToCheckpointDir(
    sc: SparkContext, partitioner: Partitioner, checkpointDirPath: Path): Unit = {
    try {
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName())
      val bufferSize = sc.conf.get(BUFFER_SIZE)
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileOutputStream = fs.create(partitionerFilePath, false, bufferSize)
      val serializer = SparkEnv.get.serializer.newInstance()
      val serializeStream = serializer.serializeStream(fileOutputStream)
      Utils.tryWithSafeFinally {
        serializeStream.writeObject(partitioner)
      } {
        serializeStream.close()
      }
      logDebug(s"Written partitioner to $partitionerFilePath")
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error writing partitioner ${MDC(PARTITIONER, partitioner)} to " +
          log"${MDC(PATH, checkpointDirPath)}")
    }
  }


  /**
   * 从检查点目录读取分区器，如果存在的话，尽力而为，读取失败仅打日志返回None
   */
  private def readCheckpointedPartitionerFile(
      sc: SparkContext,
      checkpointDirPath: String): Option[Partitioner] = {
    try {
      val bufferSize = sc.conf.get(BUFFER_SIZE)
      val partitionerFilePath = new Path(checkpointDirPath, checkpointPartitionerFileName())
      val fs = partitionerFilePath.getFileSystem(sc.hadoopConfiguration)
      val fileInputStream = fs.open(partitionerFilePath, bufferSize)
      val serializer = SparkEnv.get.serializer.newInstance()
      val partitioner = Utils.tryWithSafeFinally {
        val deserializeStream = serializer.deserializeStream(fileInputStream)
        Utils.tryWithSafeFinally {
          deserializeStream.readObject[Partitioner]()
        } {
          deserializeStream.close()
        }
      } {
        fileInputStream.close()
      }

      logDebug(s"Read partitioner from $partitionerFilePath")
      Some(partitioner)
    } catch {
      case e: FileNotFoundException =>
        logDebug("No partitioner file", e)
        None
      case NonFatal(e) =>
        logWarning(log"Error reading partitioner from ${MDC(PATH, checkpointDirPath)}, " +
          log"partitioner will not be recovered which may lead to performance loss", e)
        None
    }
  }

  /**
   * 读取指定检查点文件，返回数据迭代器
   */
  def readCheckpointFile[T](
      path: Path,
      broadcastedConf: Broadcast[SerializableConfiguration],
      context: TaskContext): Iterator[T] = {
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.get(BUFFER_SIZE)
    // 打开输入流，根据配置决定是否解压
    val fileInputStream = {
      val fileStream = fs.open(path, bufferSize)
      if (env.conf.get(CHECKPOINT_COMPRESS)) {
        CompressionCodec.createCodec(env.conf).compressedInputStream(fileStream)
      } else {
        fileStream
      }
    }
    // 使用Spark反序列化器读取数据
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // 任务完成后自动关闭输入流，避免资源泄漏
    context.addTaskCompletionListener[Unit](context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

}
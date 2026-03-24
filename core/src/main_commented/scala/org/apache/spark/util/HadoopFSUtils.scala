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

import java.io.FileNotFoundException

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.viewfs.ViewFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.util.ArrayImplicits._

/**
 * Hadoop文件系统工具类，提供简化、高效的文件列表功能，Spark核心模块用于文件数据源的文件扫描
 */
private[spark] object HadoopFSUtils extends Logging {
  /**
   * 自适应并行递归列出路径下所有叶子文件，根据输入路径数量自动选择串行或并行扫描
   * 仅可在Driver端调用
   *
   * @param sc Spark上下文，用于执行并行列表扫描
   * @param paths 待扫描的输入路径集合
   * @param hadoopConf Hadoop配置
   * @param filter 路径过滤器，用于从结果中排除不符合条件的文件
   * @param ignoreMissingFiles 是否忽略递归扫描过程中遇到的文件不存在错误（例如因竞态条件产生）
   * @param ignoreLocality 是否获取文件数据位置信息，为false时返回的FileStatus不包含BlockLocation信息
   * @param parallelismThreshold 开启并行扫描的阈值，输入路径数量小于该值则使用串行扫描
   * @param parallelismMax 并行扫描最大并行度，输入路径数量超过该值会进行限流避免产生过多任务
   * @return 每个输入路径对应的发现文件集合
   */
  def parallelListLeafFiles(
    sc: SparkContext,
    paths: Seq[Path],
    hadoopConf: Configuration,
    filter: PathFilter,
    ignoreMissingFiles: Boolean,
    ignoreLocality: Boolean,
    parallelismThreshold: Int,
    parallelismMax: Int): Seq[(Path, Seq[FileStatus])] = {
    parallelListLeafFilesInternal(sc, paths, hadoopConf, filter, isRootLevel = true,
      ignoreMissingFiles, ignoreLocality, parallelismThreshold, parallelismMax)
  }

  /**
   * 使用Hadoop原生API单次递归列出路径下所有叶子文件，可在Driver或Executor端调用
   * 会自动处理根路径不存在的FileNotFoundException
   *
   * @param path 待扫描的路径
   * @param hadoopConf Hadoop配置
   * @param filter 路径过滤器，用于从结果中排除不符合条件的文件
   * @return 输入路径对应的发现文件集合
   */
  def listFiles(
      path: Path,
      hadoopConf: Configuration,
      filter: PathFilter): Seq[(Path, Seq[FileStatus])] = {
    logInfo(log"Listing ${MDC(PATH, path)} with listFiles API")
    try {
      val prefixLength = path.toString.length
      // 使用Hadoop原生listFiles递归遍历所有文件
      val remoteIter = path.getFileSystem(hadoopConf).listFiles(path, true)
      // 包装为迭代器过滤不符合条件的路径
      val statues = new Iterator[LocatedFileStatus]() {
        def next(): LocatedFileStatus = remoteIter.next
        def hasNext: Boolean = remoteIter.hasNext
      }.filterNot(status => shouldFilterOutPath(status.getPath.toString.substring(prefixLength)))
        .filter(f => filter.accept(f.getPath))
        .toArray
      Seq((path, statues.toImmutableArraySeq))
    } catch {
      case _: FileNotFoundException =>
        logWarning(log"The root directory ${MDC(PATH, path)} " +
          log"was not found. Was it deleted very recently?")
        // 根路径不存在返回空结果
        Seq((path, Seq.empty[FileStatus]))
    }
  }

  /**
   * parallelListLeafFiles的内部实现，根据路径数量决定使用串行还是并行扫描
   */
  private def parallelListLeafFilesInternal(
      sc: SparkContext,
      paths: Seq[Path],
      hadoopConf: Configuration,
      filter: PathFilter,
      isRootLevel: Boolean,
      ignoreMissingFiles: Boolean,
      ignoreLocality: Boolean,
      parallelismThreshold: Int,
      parallelismMax: Int): Seq[(Path, Seq[FileStatus])] = {

    // 路径数量低于阈值，直接使用串行扫描更快
    if (paths.size <= parallelismThreshold) {
      return paths.map { path =>
        val leafFiles = listLeafFiles(
          path,
          hadoopConf,
          filter,
          Some(sc),
          ignoreMissingFiles = ignoreMissingFiles,
          ignoreLocality = ignoreLocality,
          isRootPath = isRootLevel,
          parallelismThreshold = parallelismThreshold,
          parallelismMax = parallelismMax)
        (path, leafFiles)
      }
    }

    logInfo(log"Listing leaf files and directories in parallel under" +
      log"${MDC(NUM_PATHS, paths.length)} paths." +
      log" The first several paths are: ${MDC(PATHS, paths.take(10).mkString(", "))}.")
    // 指标计数：增加并行文件列表扫描任务数
    HiveCatalogMetrics.incrementParallelListingJobCount(1)

    // 包装Hadoop配置为可序列化对象，用于分发到Executor
    val serializableConfiguration = new SerializableConfiguration(hadoopConf)

    // 限制并行度，避免生成过多任务
    val numParallelism = Math.min(paths.size, parallelismMax)

    // 保存原有作业描述，扫描完成后恢复
    val previousJobDescription = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    try {
      // 设置当前扫描作业的描述
      val description = paths.size match {
        case 0 =>
          "Listing leaf files and directories 0 paths"
        case 1 =>
          s"Listing leaf files and directories for 1 path:<br/>${paths(0)}"
        case s =>
          s"Listing leaf files and directories for $s paths:<br/>${paths(0)}, ..."
      }
      sc.setJobDescription(description)
      // 并行分发路径到不同分区扫描，收集结果
      sc.parallelize(paths, numParallelism)
        .mapPartitions { pathsEachPartition =>
          val hadoopConf = serializableConfiguration.value
          pathsEachPartition.map { path =>
            val leafFiles = listLeafFiles(
              path = path,
              hadoopConf = hadoopConf,
              filter = filter,
              contextOpt = None, // 工作节点无法再发起并行扫描
              ignoreMissingFiles = ignoreMissingFiles,
              ignoreLocality = ignoreLocality,
              isRootPath = isRootLevel,
              parallelismThreshold = Int.MaxValue,
              parallelismMax = 0)
            (path, leafFiles)
          }
        }.collect().toImmutableArraySeq
    } finally {
      // 恢复原有作业描述
      sc.setJobDescription(previousJobDescription)
    }
  }

  // scalastyle:off argcount
  /**
   * 递归列出单个路径下所有符合条件的叶子文件，如果提供了SparkContext且子目录数量超过阈值会递归发起并行扫描
   * contextOpt为None时可在Executor端调用
   *
   * @param path 待扫描路径
   * @param hadoopConf Hadoop配置
   * @param filter 路径过滤器
   * @param contextOpt Spark上下文可选引用，存在时可发起子并行扫描
   * @param ignoreMissingFiles 是否忽略文件不存在错误
   * @param ignoreLocality 是否不获取位置信息
   * @param isRootPath 是否为根路径
   * @param parallelismThreshold 并行扫描阈值
   * @param parallelismMax 最大并行度
   * @return 路径下所有符合过滤条件的叶子文件状态集合
   */
  private def listLeafFiles(
      path: Path,
      hadoopConf: Configuration,
      filter: PathFilter,
      contextOpt: Option[SparkContext],
      ignoreMissingFiles: Boolean,
      ignoreLocality: Boolean,
      isRootPath: Boolean,
      parallelismThreshold: Int,
      parallelismMax: Int): Seq[FileStatus] = {

    logTrace(s"Listing $path")
    val fs = path.getFileSystem(hadoopConf)

    // 获取当前路径直接下级的文件和目录状态，不包含递归结果
    val statuses: Array[FileStatus] = try {
      fs match {
        // 分布式文件系统和视图文件系统，且需要获取位置信息时，使用listLocatedStatus一次性获取块位置
        // 该方法一次RPC调用即可同时获取文件状态和块位置，性能更好
        case (_: DistributedFileSystem | _: ViewFileSystem) if !ignoreLocality =>
          val remoteIter = fs.listLocatedStatus(path)
          new Iterator[LocatedFileStatus]() {
            def next(): LocatedFileStatus = remoteIter.next
            def hasNext: Boolean = remoteIter.hasNext
          }.toArray
        // 其他情况或者不需要位置信息时，使用普通listStatus
        case _ => fs.listStatus(path)
      }
    } catch {
      // 根路径或开启忽略缺失文件时，捕获FileNotFoundException返回空结果
      // 根路径不存在常见场景：表删除后刷新元数据、竞态条件删除，此时忽略错误不抛出异常
      case _: FileNotFoundException if isRootPath || ignoreMissingFiles =>
        logWarning(log"The directory ${MDC(PATH, path)} " +
          log"was not found. Was it deleted very recently?")
        Array.empty[FileStatus]
      // 不支持的文件系统操作抛出Spark自定义异常
      case u: UnsupportedOperationException =>
        throw new SparkUnsupportedOperationException(
          errorClass = "FAILED_READ_FILE.UNSUPPORTED_FILE_SYSTEM",
          messageParameters = Map(
            "path" -> path.toString,
            "fileSystemClass" -> fs.getClass.getName,
            "method" -> u.getStackTrace.head.getMethodName))
    }

    // 根据文件名过滤掉需要排除的路径
    val filteredStatuses =
      statuses.filterNot(status => shouldFilterOutPathName(status.getPath.getName))

    val allLeafStatuses = {
      // 将直接下级状态拆分为目录和文件两部分
      val (dirs, topLevelFiles) = filteredStatuses.partition(_.isDirectory)
      // 递归处理子目录
      val filteredNestedFiles: Seq[FileStatus] = contextOpt match {
        // 存在SparkContext且子目录数量超过阈值，递归发起并行扫描
        case Some(context) if dirs.length > parallelismThreshold =>
          parallelListLeafFilesInternal(
            context,
            dirs.map(_.getPath).toImmutableArraySeq,
            hadoopConf = hadoopConf,
            filter = filter,
            isRootLevel = false,
            ignoreMissingFiles = ignoreMissingFiles,
            ignoreLocality = ignoreLocality,
            parallelismThreshold = parallelismThreshold,
            parallelismMax = parallelismMax
          ).flatMap(_._2)
        // 否则串行递归处理每个子目录
        case _ =>
          dirs.flatMap { dir =>
            listLeafFiles(
              path = dir.getPath,
              hadoopConf = hadoopConf,
              filter = filter,
              contextOpt = contextOpt,
              ignoreMissingFiles = ignoreMissingFiles,
              ignoreLocality = ignoreLocality,
              isRootPath = false,
              parallelismThreshold = parallelismThreshold,
              parallelismMax = parallelismMax)
          }.toImmutableArraySeq
      }
      // 对顶层文件应用用户过滤器
      val filteredTopLevelFiles = if (filter != null) {
        topLevelFiles.filter(f => filter.accept(f.getPath))
      } else {
        topLevelFiles
      }
      // 合并顶层文件和递归子目录结果
      filteredTopLevelFiles ++ filteredNestedFiles
    }

    val missingFiles = mutable.ArrayBuffer.empty[String]
    // 如果需要位置信息且文件不是LocatedFileStatus，主动获取块位置信息包装为LocatedFileStatus
    val resolvedLeafStatuses = allLeafStatuses.flatMap {
      // 已经是LocatedFileStatus直接保留
      case f: LocatedFileStatus =>
        Some(f)

      case f if !ignoreLocality =>
        // 重新构造LocatedFileStatus，避免调用较慢的权限获取逻辑
        try {
          // 获取文件块位置信息，转换为原生BlockLocation节省内存
          val locations = fs.getFileBlockLocations(f, 0, f.getLen).map { loc =>
            if (loc.getClass == classOf[BlockLocation]) {
              loc
            } else {
              new BlockLocation(loc.getNames, loc.getHosts, loc.getOffset, loc.getLength)
            }
          }
          // 构造新的LocatedFileStatus，保留原文件所有属性
          val lfs = new LocatedFileStatus(f.getLen, f.isDirectory, f.getReplication, f.getBlockSize,
            f.getModificationTime, 0, null, null, null, null, f.getPath,
            f.hasAcl, f.isEncrypted, f.isErasureCoded, locations)
          if (f.isSymlink) {
            lfs.setSymlink(f.getSymlink)
          }
          Some(lfs)
        } catch {
          // 忽略缺失文件时，记录缺失文件并返回None
          case _: FileNotFoundException if ignoreMissingFiles =>
            missingFiles += f.getPath.toString
            None
        }

      // 不需要位置信息直接保留
      case f => Some(f)
    }

    // 扫描完成后输出所有缺失文件警告
    if (missingFiles.nonEmpty) {
      logWarning(log"the following files were missing during file scan:\n  " +
        log"${MDC(PATHS, missingFiles.mkString("\n  "))}")
    }

    resolvedLeafStatuses.toImmutableArraySeq
  }
  // scalastyle:on argcount

  /**
   * 检查文件名是否需要过滤掉
   * @param pathName 文件名
   * @return true表示需要过滤，false保留
   */
  def shouldFilterOutPathName(pathName: String): Boolean = {
    // 过滤规则：
    // 1. 以_和.开头的文件，例外：_common_metadata 和 _metadata（Parquet需要这些元数据文件）
    // 2. 以._COPYING_结尾的文件，这是写入中的临时文件，需要跳过避免重复读取
    val exclude = (pathName.startsWith("_") && !pathName.contains("=")) ||
      pathName.startsWith(".") || pathName.endsWith("._COPYING_")
    val include = pathName.startsWith("_common_metadata") || pathName.startsWith("_metadata")
    exclude && !include
  }

  // 匹配路径中间的下划线开头目录的正则
  private val underscore: Regex = "/_[^=/]*/".r
  // 匹配路径末尾的下划线开头目录的正则
  private val underscoreEnd: Regex = "/_[^=/]*$".r

  /**
   * 递归检查完整路径是否需要过滤掉，处理嵌套路径中的隐藏目录和元数据目录
   * @param path 完整路径字符串
   * @return true表示需要过滤，false保留
   */
  @scala.annotation.tailrec
  def shouldFilterOutPath(path: String): Boolean = {
    // 包含/.或以._COPYING_结尾直接过滤
    if (path.contains("/.") || path.endsWith("._COPYING_")) return true
    // 检查路径末尾是否有下划线开头目录
    underscoreEnd.findFirstIn(path) match {
      // 末尾是允许的元数据目录，不过滤
      case Some(dir) if dir.equals("/_metadata") || dir.equals("/_common_metadata") => false
      // 其他下划线开头目录，过滤
      case Some(_) => true
      case None =>
        // 检查路径中间是否有下划线开头目录
        underscore.findFirstIn(path) match {
          // 中间是允许的_metadata目录，移除后递归检查剩余路径
          case Some(dir) if dir.equals("/_metadata/") =>
            shouldFilterOutPath(path.replaceFirst("/_metadata", ""))
          // 中间是允许的_common_metadata目录，移除后递归检查剩余路径
          case Some(dir) if dir.equals("/_common_metadata/") =>
            shouldFilterOutPath(path.replaceFirst("/_common_metadata", ""))
          // 其他下划线开头目录，过滤
          case Some(_) => true
          // 没有下划线开头目录，不过滤
          case None => false
        }
    }
  }
}
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

package org.apache.spark.storage

import java.io.{File, IOException}
import java.nio.file.Files
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.util.UUID

import scala.collection.mutable.HashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.LogKeys.{MERGE_DIR_NAME, PATH}
import org.apache.spark.network.shuffle.ExecutorDiskUtils
import org.apache.spark.storage.DiskBlockManager.ATTEMPT_ID_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIR_KEY
import org.apache.spark.storage.DiskBlockManager.MERGE_DIRECTORY
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * 文件级注释：磁盘块管理器，维护逻辑块与物理磁盘文件位置的映射关系
 * 核心职责：为Spark存储模块提供磁盘块的创建、查找、删除等管理能力，将块数据哈希分散到多个本地目录避免顶级目录inode过多
 */
/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 *
 * ShuffleDataIO also can change the behavior of deleteFilesOnStop.
 */
private[spark] class DiskBlockManager(
    conf: SparkConf,
    var deleteFilesOnStop: Boolean,
    isDriver: Boolean)
  extends Logging {

  private[spark] val subDirsPerLocalDir = conf.get(config.DISKSTORE_SUB_DIRECTORIES)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  // 初始化根本地目录数组
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // 本地目录路径字符串数组，用于快速访问
  private[spark] val localDirsString: Array[String] = localDirs.map(_.toString)

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  // 二级子目录数组：[本地根目录][子目录序号]，子目录延迟创建，访问时加锁同步
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  // Get merge directory name, append attemptId if there is any
  // 合并Shuffle目录名，带尝试ID后缀（如果存在）
  private val mergeDirName =
    s"$MERGE_DIRECTORY${conf.get(config.APP_ATTEMPT_ID).map(id => s"_$id").getOrElse("")}"

  // Create merge directories
  // 创建用于存储合并Shuffle块的本地目录
  createLocalDirsForMergedShuffleBlocks()

  // 注册JVM关闭钩子，用于退出时清理本地目录
  private val shutdownHook = addShutdownHook()

  // If either of these features are enabled, we must change permissions on block manager
  // directories and files to accommodate the shuffle service deleting files in a secure
  // environment. Parent directories are assumed to be restrictive to prevent unauthorized users
  // from accessing or modifying world readable files.
  // 是否需要修改文件权限：开启外部Shuffle服务且启用清理或RDD获取时需要
  private val permissionChangingRequired = conf.get(config.SHUFFLE_SERVICE_ENABLED) && (
    conf.get(config.SHUFFLE_SERVICE_REMOVE_SHUFFLE_ENABLED) ||
    conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
  )

  /** 通过哈希算法定位块文件所在的子目录，返回块对应的File对象 */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExecutorDiskUtils#getFilePath().
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    // 计算文件名的非负哈希值
    val hash = Utils.nonNegativeHash(filename)
    // 选择根本地目录ID
    val dirId = hash % localDirs.length
    // 选择二级子目录ID
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    // 同步创建并获取二级子目录
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists()) {
          val path = newDir.toPath
          Files.createDirectory(path)
          if (permissionChangingRequired) {
            // SPARK-37618: Create dir as group writable so files within can be deleted by the
            // shuffle service in a secure setup. This will remove the setgid bit so files created
            // within won't be created with the parent folder group.
            // 添加组写权限，供外部Shuffle服务操作文件
            val currentPerms = Files.getPosixFilePermissions(path)
            currentPerms.add(PosixFilePermission.GROUP_WRITE)
            Files.setPosixFilePermissions(path, currentPerms)
          }
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  /** 根据BlockId获取对应磁盘文件 */
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /**
   * 获取合并后Shuffle块的磁盘文件，需要与远程块推送解析器保持路径生成逻辑一致
   * @param blockId 合并Shuffle块ID
   * @param dirs 合并Shuffle目录数组
   * @return 合并块对应的File对象
   */
  /**
   * This should be in sync with
   * @see [[org.apache.spark.network.shuffle.RemoteBlockPushResolver#getFile(
   *     java.lang.String, java.lang.String)]]
   */
  def getMergedShuffleFile(blockId: BlockId, dirs: Option[Array[String]]): File = {
    blockId match {
      case mergedBlockId: ShuffleMergedDataBlockId =>
        getMergedShuffleFile(mergedBlockId.name, dirs)
      case mergedIndexBlockId: ShuffleMergedIndexBlockId =>
        getMergedShuffleFile(mergedIndexBlockId.name, dirs)
      case mergedMetaBlockId: ShuffleMergedMetaBlockId =>
        getMergedShuffleFile(mergedMetaBlockId.name, dirs)
      case _ =>
        throw SparkException.internalError(
          s"Only merged block ID is supported, but got $blockId", category = "STORAGE")
    }
  }

  /** 私有实现：根据文件名和目录数组获取合并Shuffle文件 */
  private def getMergedShuffleFile(filename: String, dirs: Option[Array[String]]): File = {
    if (!dirs.exists(_.nonEmpty)) {
      throw SparkException.internalError(
        s"Cannot read $filename because merged shuffle dirs is empty", category = "STORAGE")
    }
    new File(ExecutorDiskUtils.getFilePath(dirs.get, subDirsPerLocalDir, filename))
  }

  /** 检查磁盘上是否存在对应块 */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** 获取磁盘管理器当前存储的所有块文件列表 */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      // 同步复制目录数组，避免并发修改问题
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files.toImmutableArraySeq else Seq.empty
    }.toImmutableArraySeq
  }

  /** 获取磁盘管理器当前存储的所有块ID列表 */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          // 跳过无法识别为Block的文件，比如临时文件
          None
      }
    }
  }

  /**
   * 创建全局可读的文件，用于在安全Yarn环境中让外部Shuffle服务能够读取Shuffle文件
   * 解决父目录失去setgid位后Shuffle服务无法读取文件的问题
   * @param file 要创建的文件对象
   */
  /**
   * SPARK-37618: Makes sure that the file is created as world readable. This is to get
   * around the fact that making the block manager sub dirs group writable removes
   * the setgid bit in secure Yarn environments, which prevents the shuffle service
   * from being able to read shuffle files. The outer directories will still not be
   * world executable, so this doesn't allow access to these files except for the
   * running user and shuffle service.
   */
  def createWorldReadableFile(file: File): Unit = {
    val path = file.toPath
    Files.createFile(path)
    val currentPerms = Files.getPosixFilePermissions(path)
    currentPerms.add(PosixFilePermission.OTHERS_READ)
    Files.setPosixFilePermissions(path, currentPerms)
  }

  /**
   * 根据目标文件创建带权限的临时文件，后续会重命名为最终文件
   * 如果需要权限调整则创建为全局可读
   * @param file 目标最终文件
   * @return 创建好的临时文件对象
   */
  /**
   * Creates a temporary version of the given file with world readable permissions (if required).
   * Used to create block files that will be renamed to the final version of the file.
   */
  def createTempFileWith(file: File): File = {
    val tmpFile = Utils.tempFileWith(file)
    if (permissionChangingRequired) {
      // SPARK-37618: we need to make the file world readable because the parent will
      // lose the setgid bit when making it group writable. Without this the shuffle
      // service can't read the shuffle files in a secure setup.
      createWorldReadableFile(tmpFile)
    }
    tmpFile
  }

  /** 创建用于存储本地中间结果的唯一临时块和对应文件 */
  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** 创建用于存储Shuffle中间结果的唯一临时块和对应文件 */
  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = TempShuffleBlockId(UUID.randomUUID())
    }
    val tmpFile = getFile(blockId)
    if (permissionChangingRequired) {
      // SPARK-37618: we need to make the file world readable because the parent will
      // lose the setgid bit when making it group writable. Without this the shuffle
      // service can't read the shuffle files in a secure setup.
      createWorldReadableFile(tmpFile)
    }
    (blockId, tmpFile)
  }

  /**
   * 创建存储块数据的根本地目录，基于spark.local.dir配置的路径创建
   * @param conf Spark配置
   * @return 创建成功的本地根目录数组
   */
  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(log"Created local directory at ${MDC(PATH, localDir)}")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(
            log"Failed to create local dir in ${MDC(PATH, rootDir)}. Ignoring this directory.", e)
          None
      }
    }
  }

  /**
   * 为基于推送的Shuffle创建合并块存储目录，外部Shuffle服务没有权限在应用本地目录下创建目录，因此由Executor提前创建
   */
  /**
   * Get the list of configured local dirs storing merged shuffle blocks created by executors
   * if push based shuffle is enabled. Note that the files in this directory will be created
   * by the external shuffle services. We only create the merge_manager directories and
   * subdirectories here because currently the external shuffle service doesn't have
   * permission to create directories under application local directories.
   */
  private def createLocalDirsForMergedShuffleBlocks(): Unit = {
    if (Utils.isPushBasedShuffleEnabled(conf, isDriver = isDriver, checkSerializer = false)) {
      // Will create the merge_manager directory only if it doesn't exist under the local dir.
      Utils.getConfiguredLocalDirs(conf).foreach { rootDir =>
        try {
          val mergeDir = new File(rootDir, mergeDirName)
          if (!mergeDir.exists() || mergeDir.listFiles().length < subDirsPerLocalDir) {
            // This executor does not find merge_manager directory, it will try to create
            // the merge_manager directory and the sub directories.
            logDebug(s"Try to create $mergeDir and its sub dirs since the " +
              s"$mergeDirName dir does not exist")
            for (dirNum <- 0 until subDirsPerLocalDir) {
              val subDir = new File(mergeDir, "%02x".format(dirNum))
              if (!subDir.exists()) {
                // Only one container will create this directory. The filesystem will handle
                // any race conditions.
                createDirWithPermission770(subDir)
              }
            }
          }
          logInfo(log"Merge directory and its sub dirs get created at ${MDC(PATH, mergeDir)}")
        } catch {
          case e: IOException =>
            logError(
              log"Failed to create ${MDC(MERGE_DIR_NAME, mergeDirName)} dir in " +
                log"${MDC(PATH, rootDir)}. Ignoring this directory.", e)
        }
      }
    }
  }

  /**
   * 创建770权限（rwxrwx---）的目录，让组内Shuffle服务可以读写，供合并Shuffle使用
   * @param dirToCreate 要创建的目录
   */
  /**
   * Create a directory that is writable by the group.
   * Grant the permission 770 "rwxrwx---" to the directory so the shuffle server can
   * create subdirs/files within the merge folder.
   */
  def createDirWithPermission770(dirToCreate: File): Unit = {
    var attempts = 0
    val maxAttempts = Utils.MAX_DIR_CREATION_ATTEMPTS
    var created: File = null
    while (created == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw Spark
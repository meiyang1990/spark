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

import java.io.{File, PrintStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.util.ArrayImplicits._

/**
 * Ivy依赖配置属性，封装Spark提交作业时用户指定的依赖相关参数
 * @param packagesExclusions 排除的依赖包列表
 * @param packages 需要添加的依赖包列表
 * @param repositories 远程仓库地址列表
 * @param ivyRepoPath Ivy本地仓库路径
 * @param ivySettingsPath Ivy配置文件路径
 */
private[spark] case class IvyProperties(
    packagesExclusions: String,
    packages: String,
    repositories: String,
    ivyRepoPath: String,
    ivySettingsPath: String)

/**
 * 文件路径：core/src/main/scala/org/apache/spark/util/DependencyUtils.scala
 * 依赖工具类，提供Maven/Ivy依赖解析、下载、类路径添加等功能，主要用于Spark提交作业时处理用户依赖
 */
private[spark] object DependencyUtils extends Logging {

  /**
   * 从系统属性中读取Ivy相关配置，构建IvyProperties对象
   * @return 封装完成的Ivy配置属性对象
   */
  def getIvyProperties(): IvyProperties = {
    val Seq(packagesExclusions, packages, repositories, ivyRepoPath, ivySettingsPath) = Seq(
      JAR_PACKAGES_EXCLUSIONS.key,
      JAR_PACKAGES.key,
      JAR_REPOSITORIES.key,
      JAR_IVY_REPO_PATH.key,
      JAR_IVY_SETTING_PATH.key
    ).map(sys.props.get(_).orNull)
    IvyProperties(packagesExclusions, packages, repositories, ivyRepoPath, ivySettingsPath)
  }

  /**
   * Download Ivy URI's dependency jars.
   *
   * @param uri Ivy URI need to be downloaded. The URI format should be:
   *              `ivy://group:module:version[?query]`
   *            Ivy URI query part format should be:
   *              `parameter=value&parameter=value...`
   *            Note that currently Ivy URI query part support two parameters:
   *             1. transitive: whether to download dependent jars related to your Ivy URI.
   *                transitive=false or `transitive=true`, if not set, the default value is true.
   *             2. exclude: exclusion list when download Ivy URI jar and dependency jars.
   *                The `exclude` parameter content is a ',' separated `group:module` pair string :
   *                `exclude=group:module,group:module...`
   * @return List of jars downloaded.
   */
  /**
   * 解析Ivy URI格式的依赖，下载并返回所有依赖jar的本地路径列表
   * @param uri Ivy格式依赖URI，格式为ivy://group:module:version[?query]
   * @return 下载完成的依赖jar本地路径列表
   */
  def resolveMavenDependencies(uri: URI): Seq[String] = {
    val ivyProperties = DependencyUtils.getIvyProperties()
    val authority = uri.getAuthority
    // 校验URI格式合法性，必须包含group:module:version格式的授权部分
    if (authority == null) {
      throw new IllegalArgumentException(
        s"Invalid Ivy URI authority in uri ${uri.toString}:" +
          " Expected 'org:module:version', found null.")
    }
    if (authority.split(":").length != 3) {
      throw new IllegalArgumentException(
        s"Invalid Ivy URI authority in uri ${uri.toString}:" +
          s" Expected 'org:module:version', found $authority.")
    }

    // 解析URI查询参数，获取是否传递依赖、排除列表、额外仓库地址
    val (transitive, exclusionList, repos) = MavenUtils.parseQueryParams(uri)
    // 合并系统配置仓库和URI指定仓库，生成完整仓库列表
    val fullReposList = Seq(ivyProperties.repositories, repos)
      .filter(!SparkStringUtils.isBlank(_))
      .mkString(",")
    resolveMavenDependencies(
      transitive,
      exclusionList,
      authority,
      fullReposList,
      ivyProperties.ivyRepoPath,
      Option(ivyProperties.ivySettingsPath)
    )
  }

  /**
   * 根据参数解析Maven依赖，下载依赖jar到本地并返回路径列表
   * @param packagesTransitive 是否下载传递依赖
   * @param packagesExclusions 需要排除的依赖包，逗号分隔
   * @param packages 需要解析的依赖包，格式为group:module:version，多个逗号分隔
   * @param repositories Maven仓库地址，多个逗号分隔
   * @param ivyRepoPath Ivy本地仓库路径
   * @param ivySettingsPath Ivy配置文件路径（可选）
   * @return 下载完成的依赖jar本地路径列表
   */
  def resolveMavenDependencies(
      packagesTransitive: Boolean,
      packagesExclusions: String,
      packages: String,
      repositories: String,
      ivyRepoPath: String,
      ivySettingsPath: Option[String]): Seq[String] = {
    // 拆分排除依赖列表为Seq
    val exclusions: Seq[String] =
      if (!SparkStringUtils.isBlank(packagesExclusions)) {
        packagesExclusions.split(",").toImmutableArraySeq
      } else {
        Nil
      }
    // Create the IvySettings, either load from file or build defaults
    implicit val printStream: PrintStream = SparkSubmit.printStream
    // 加载或构建Ivy配置，优先使用用户指定的配置文件
    val ivySettings = ivySettingsPath match {
      case Some(path) =>
        MavenUtils.loadIvySettings(path, Option(repositories), Option(ivyRepoPath))

      case None =>
        MavenUtils.buildIvySettings(
          Option(repositories),
          Option(ivyRepoPath))
    }

    MavenUtils.resolveMavenCoordinates(packages, ivySettings,
      transitive = packagesTransitive, exclusions = exclusions)
  }

  /**
   * 解析并下载用户指定的jar包，过滤掉用户主jar包，返回下载后本地路径逗号分隔字符串
   * @param jars 待下载的jar路径列表，逗号分隔
   * @param userJar 用户提交的主jar包路径
   * @param sparkConf Spark配置对象
   * @param hadoopConf Hadoop配置对象
   * @return 下载完成的jar本地路径，逗号分隔，无则返回null
   */
  def resolveAndDownloadJars(
      jars: String,
      userJar: String,
      sparkConf: SparkConf,
      hadoopConf: Configuration): String = {
    val targetDir = Utils.createTempDir()
    val userJarName = userJar.split(File.separatorChar).last
    Option(jars)
      .map {
        // 解析通配路径，拆分并过滤掉用户主jar
        resolveGlobPaths(_, hadoopConf)
          .split(",")
          .filterNot(_.contains(userJarName))
          .mkString(",")
      }
      .filterNot(_ == "")
      // 下载所有jar到临时目录
      .map(downloadFileList(_, targetDir, sparkConf, hadoopConf))
      .orNull
  }

  /**
   * 将逗号分隔的jar列表依次添加到类加载器的类路径中
   * @param jars 待添加的jar路径，逗号分隔
   * @param loader 目标类加载器
   */
  def addJarsToClassPath(jars: String, loader: MutableURLClassLoader): Unit = {
    if (jars != null) {
      for (jar <- jars.split(",")) {
        addJarToClasspath(jar, loader)
      }
    }
  }

  /**
   * Download a list of remote files to temp local files. If the file is local, the original file
   * will be returned.
   *
   * @param fileList A comma separated file list.
   * @param targetDir A temporary directory for which downloaded files.
   * @param sparkConf Spark configuration.
   * @param hadoopConf Hadoop configuration.
   * @return A comma separated local files list.
   */
  /**
   * 批量下载远程文件到本地临时目录，本地文件直接返回原路径
   * @param fileList 待下载文件路径，逗号分隔
   * @param targetDir 下载目标临时目录
   * @param sparkConf Spark配置对象
   * @param hadoopConf Hadoop配置对象
   * @return 下载后的本地文件路径，逗号分隔
   */
  def downloadFileList(
      fileList: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration): String = {
    require(fileList != null, "fileList cannot be null.")
    Utils.stringToSeq(fileList)
      .map(downloadFile(_, targetDir, sparkConf, hadoopConf))
      .mkString(",")
  }

  /**
   * Download a file from the remote to a local temporary directory. If the input path points to
   * a local path, returns it with no operation.
   *
   * @param path A file path from where the files will be downloaded.
   * @param targetDir A temporary directory for which downloaded files.
   * @param sparkConf Spark configuration.
   * @param hadoopConf Hadoop configuration.
   * @return Path to the local file.
   */
  /**
   * 下载单个远程文件到本地临时目录，本地文件直接返回原路径
   * @param path 待下载文件路径
   * @param targetDir 下载目标临时目录
   * @param sparkConf Spark配置对象
   * @param hadoopConf Hadoop配置对象
   * @return 本地文件路径
   */
  def downloadFile(
      path: String,
      targetDir: File,
      sparkConf: SparkConf,
      hadoopConf: Configuration): String = {
    require(path != null, "path cannot be null.")
    val uri = Utils.resolveURI(path)

    uri.getScheme match {
      // 本地文件直接返回原路径
      case "file" | "local" => path
      // 测试环境下HTTP/HTTPS/FTP协议返回模拟路径，不实际下载
      case "http" | "https" | "ftp" if Utils.isTesting =>
        // This is only used for SparkSubmitSuite unit test. Instead of downloading file remotely,
        // return a dummy local path instead.
        val file = new File(uri.getPath)
        new File(targetDir, file.getName).toURI.toString
      // 其他协议（HDFS等）通过Hadoop API下载到本地
      case _ =>
        val fname = new Path(uri).getName()
        val localFile = Utils.doFetchFile(uri.toString(), targetDir, fname, sparkConf, hadoopConf)
        localFile.toURI().toString()
    }
  }

  /**
   * 批量解析包含通配符的路径，展开为实际文件路径，支持#fragment命名语法
   * @param paths 待解析路径，逗号分隔，可包含通配符
   * @param hadoopConf Hadoop配置对象
   * @return 展开后的实际文件路径，逗号分隔
   */
  def resolveGlobPaths(paths: String, hadoopConf: Configuration): String = {
    require(paths != null, "paths cannot be null.")
    Utils.stringToSeq(paths).flatMap { path =>
      val (base, fragment) = splitOnFragment(path)
      (resolveGlobPath(base, hadoopConf), fragment) match {
        // 通配匹配到多个文件且带fragment时抛出歧义错误
        case (resolved, Some(_)) if resolved.length > 1 => throw new SparkException(
            s"${base.toString} resolves ambiguously to multiple files: ${resolved.mkString(",")}")
        // 添加fragment后缀
        case (resolved, Some(namedAs)) => resolved.map(_ + "#" + namedAs)
        // 无fragment直接返回
        case (resolved, _) => resolved
      }
    }.mkString(",")
  }

  /**
   * 将单个本地jar添加到类加载器的类路径中，远程jar跳过并打印警告
   * @param localJar jar文件路径
   * @param loader 目标类加载器
   */
  def addJarToClasspath(localJar: String, loader: MutableURLClassLoader): Unit = {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      // 本地文件存在则添加到类路径，不存在则打印警告
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          logWarning(log"Local jar ${MDC(FILE_NAME, file)} does not exist, skipping.")
        }
      // 远程jar不添加，打印警告
      case _ =>
        logWarning(log"Skip remote jar ${MDC(LogKeys.URI, uri)}.")
    }
  }

  /**
   * Merge a sequence of comma-separated file lists, some of which may be null to indicate
   * no files, into a single comma-separated string.
   */
  /**
   * 合并多个逗号分隔的文件路径列表，过滤空值，生成单个合并后的逗号分隔字符串
   * @param lists 待合并的路径列表，每个元素为逗号分隔字符串
   * @return 合并后的路径，逗号分隔，无有效路径则返回null
   */
  def mergeFileLists(lists: String*): String = {
    val merged = lists.filterNot(SparkStringUtils.isBlank)
      .flatMap(Utils.stringToSeq)
    if (merged.nonEmpty) merged.mkString(",") else null
  }

  /**
   * 将路径拆分为基础URI和fragment（#后面的部分）
   * @param path 原始路径字符串
   * @return 拆分后的(基础URI, fragment可选)
   */
  private def splitOnFragment(path: String): (URI, Option[String]) = {
    val uri = Utils.resolveURI(path)
    val withoutFragment = new URI(uri.getScheme, uri.getSchemeSpecificPart, null)
    (withoutFragment, Option(uri.getFragment))
  }

  /**
   * 解析包含通配符的URI，展开匹配到的所有文件URI
   * @param uri 待解析URI，可包含通配符
   * @param hadoopConf Hadoop配置对象
   * @return 匹配到的所有文件URI字符串数组
   */
  private def resolveGlobPath(uri: URI, hadoopConf: Configuration): Array[String] = {
    uri.getScheme match {
      // 本地/HTTP等协议不展开通配，直接返回原URI
      case "local" | "http" | "https" | "ftp" => Array(uri.toString)
      // HDFS等文件系统协议使用Hadoop glob API展开通配
      case _ =>
        val fs = FileSystem.get(uri, hadoopConf)
        Option(fs.globStatus(new Path(uri))).map { status =>
          // 只保留文件，过滤目录，转换为URI字符串
          status.filter(_.isFile).map(_.getPath.toUri.toString)
        }.getOrElse(Array(uri.toString))
    }
  }

}
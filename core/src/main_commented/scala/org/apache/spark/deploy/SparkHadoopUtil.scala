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

package org.apache.spark.deploy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, File, FileNotFoundException, IOException}
import java.net.InetAddress
import java.security.PrivilegedExceptionAction
import java.text.DateFormat
import java.util.{Date, Locale}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._
import scala.language.existentials

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem.HdfsDataOutputStreamBuilder
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{ReadOnlySparkConf, SparkConf, SparkException}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * 文件级注释：Spark与Hadoop交互的工具类，提供Hadoop配置整合、凭证管理、路径处理等通用能力
 */
/**
 * Contains util methods to interact with Hadoop from Spark.
 */
/**
 * 类级注释：Spark与Hadoop交互工具类，提供Hadoop配置整合、Kerberos认证、凭证管理、文件路径处理等核心能力
 * 仅在Spark内部部署模块使用
 */
private[spark] class SparkHadoopUtil extends Logging {
  private val sparkConf = new SparkConf(false).loadFromSystemProperties(true)
  val conf: Configuration = newConfiguration(sparkConf)
  UserGroupInformation.setConfiguration(conf)

  /**
   * 方法级注释：以Spark用户身份执行给定函数，用于HDFS/YARN调用的身份认证
   * 该方法会将UGI设置为线程局部变量，自动传递给子线程
   * @param func 要执行的函数
   */
  /**
   * Runs the given function with a Hadoop UserGroupInformation as a thread local variable
   * (distributed to child threads), used for authenticating HDFS and YARN calls.
   *
   * IMPORTANT NOTE: If this function is going to be called repeated in the same process
   * you need to look https://issues.apache.org/jira/browse/HDFS-3545 and possibly
   * do a FileSystem.closeAllForUGI in order to avoid leaking Filesystems
   */
  def runAsSparkUser(func: () => Unit): Unit = {
    createSparkUser().doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = func()
    })
  }

  /**
   * 方法级注释：创建当前用户对应的远程UGI对象，用于Hadoop认证
   * @return 创建好的UGI对象，已继承当前用户凭证
   */
  def createSparkUser(): UserGroupInformation = {
    val user = Utils.getCurrentUserName()
    logDebug("creating UGI for user: " + user)
    val ugi = UserGroupInformation.createRemoteUser(user)
    transferCredentials(UserGroupInformation.getCurrentUser(), ugi)
    ugi
  }

  /**
   * 方法级注释：将源UGI的凭证转移到目标UGI
   * @param source 源UGI
   * @param dest 目标UGI
   */
  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation): Unit = {
    dest.addCredentials(source.getCredentials())
  }

  /**
   * Appends S3-specific, spark.hadoop.*, and spark.buffer.size configurations to a Hadoop
   * configuration.
   */
  def appendS3AndSparkHadoopHiveConfigurations(
      conf: SparkConf,
      hadoopConf: Configuration): Unit = {
    SparkHadoopUtil.appendS3AndSparkHadoopHiveConfigurations(conf, hadoopConf)
  }

  /**
   * Appends spark.hadoop.* configurations from a [[SparkConf]] to a Hadoop
   * configuration without the spark.hadoop. prefix.
   */
  def appendSparkHadoopConfigs(conf: ReadOnlySparkConf, hadoopConf: Configuration): Unit = {
    SparkHadoopUtil.appendSparkHadoopConfigs(conf, hadoopConf)
  }

  /**
   * Appends spark.hadoop.* configurations from a Map to another without the spark.hadoop. prefix.
   */
  def appendSparkHadoopConfigs(
      srcMap: Map[String, String],
      destMap: HashMap[String, String]): Unit = {
    // Copy any "spark.hadoop.foo=bar" system properties into destMap as "foo=bar"
    for ((key, value) <- srcMap if key.startsWith("spark.hadoop.")) {
      destMap.put(key.substring("spark.hadoop.".length), value)
    }
  }

  def appendSparkHiveConfigs(
      srcMap: Map[String, String],
      destMap: HashMap[String, String]): Unit = {
    // Copy any "spark.hive.foo=bar" system properties into destMap as "hive.foo=bar"
    for ((key, value) <- srcMap if key.startsWith("spark.hive.")) {
      destMap.put(key.substring("spark.".length), value)
    }
  }

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initialize some Hadoop
   * subsystems.
   */
  def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    hadoopConf.addResource(SparkHadoopUtil.SPARK_HADOOP_CONF_FILE)
    hadoopConf
  }

  /**
   * Add any user credentials to the job conf which are necessary for running on a secure Hadoop
   * cluster.
   */
  def addCredentials(conf: JobConf): Unit = {
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }

  def addCurrentUserCredentials(creds: Credentials): Unit = {
    UserGroupInformation.getCurrentUser.addCredentials(creds)
  }

  /**
   * 方法级注释：使用keytab登录Kerberos获取认证凭证
   * @param principalName Kerberos主体名称
   * @param keytabFilename keytab文件路径
   */
  def loginUserFromKeytab(principalName: String, keytabFilename: String): Unit = {
    if (!new File(keytabFilename).exists()) {
      throw new SparkException(s"Keytab file: ${keytabFilename} does not exist")
    } else {
      logInfo(log"Attempting to login to Kerberos using principal: " +
        log"${MDC(LogKeys.PRINCIPAL, principalName)} and keytab: " +
        log"${MDC(LogKeys.KEYTAB, keytabFilename)}")
      UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  /**
   * Add or overwrite current user's credentials with serialized delegation tokens,
   * also confirms correct hadoop configuration is set.
   */
  private[spark] def addDelegationTokens(tokens: Array[Byte], sparkConf: SparkConf): Unit = {
    UserGroupInformation.setConfiguration(newConfiguration(sparkConf))
    val creds = deserialize(tokens)
    logInfo("Updating delegation tokens for current user.")
    logDebug(s"Adding/updating delegation tokens ${dumpTokens(creds)}")
    addCurrentUserCredentials(creds)
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes read. If
   * getFSBytesReadOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes read on r since t.
   */
  private[spark] def getFSBytesReadOnThreadCallback(): () => Long = {
    val f = () => FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics.getBytesRead).sum
    val baseline = (Thread.currentThread().getId, f())

    /**
     * This function may be called in both spawned child threads and parent task thread (in
     * PythonRDD), and Hadoop FileSystem uses thread local variables to track the statistics.
     * So we need a map to track the bytes read from the child threads and parent thread,
     * summing them together to get the bytes read of this task.
     */
    new (() => Long) {
      private val bytesReadMap = new mutable.HashMap[Long, Long]()

      override def apply(): Long = {
        bytesReadMap.synchronized {
          bytesReadMap.put(Thread.currentThread().getId, f())
          bytesReadMap.map { case (k, v) =>
            v - (if (k == baseline._1) baseline._2 else 0)
          }.sum
        }
      }
    }
  }

  /**
   * Returns a function that can be called to find Hadoop FileSystem bytes written. If
   * getFSBytesWrittenOnThreadCallback is called from thread r at time t, the returned callback will
   * return the bytes written on r since t.
   *
   * @return None if the required method can't be found.
   */
  private[spark] def getFSBytesWrittenOnThreadCallback(): () => Long = {
    val threadStats = FileSystem.getAllStatistics.asScala.map(_.getThreadStatistics)
    val f = () => threadStats.map(_.getBytesWritten).sum
    val baselineBytesWritten = f()
    () => f() - baselineBytesWritten
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
   */
  def listLeafStatuses(fs: FileSystem, basePath: Path): Seq[FileStatus] = {
    listLeafStatuses(fs, fs.getFileStatus(basePath))
  }

  /**
   * Get [[FileStatus]] objects for all leaf children (files) under the given base path. If the
   * given path points to a file, return a single-element collection containing [[FileStatus]] of
   * that file.
   */
  def listLeafStatuses(fs: FileSystem, baseStatus: FileStatus): Seq[FileStatus] = {
    def recurse(status: FileStatus): Seq[FileStatus] = {
      val (directories, leaves) = fs.listStatus(status.getPath).partition(_.isDirectory)
      (leaves ++ directories.flatMap(f => listLeafStatuses(fs, f))).toImmutableArraySeq
    }

    if (baseStatus.isDirectory) recurse(baseStatus) else Seq(baseStatus)
  }

  /**
   * 方法级注释：判断路径是否包含通配符
   * @param pattern 待检查路径
   * @return 是否包含通配符
   */
  def isGlobPath(pattern: Path): Boolean = {
    pattern.toString.exists("{}[]*?\\".toSet.contains)
  }

  /**
   * 方法级注释：对通配符路径进行匹配展开
   * @param pattern 带通配符的路径
   * @return 匹配到的路径列表
   */
  def globPath(pattern: Path): Seq[Path] = {
    val fs = pattern.getFileSystem(conf)
    globPath(fs, pattern)
  }

  /**
   * 方法级注释：使用指定文件系统对通配符路径进行匹配展开
   * @param fs 文件系统对象
   * @param pattern 带通配符的路径
   * @return 匹配到的路径列表
   */
  def globPath(fs: FileSystem, pattern: Path): Seq[Path] = {
    Option(fs.globStatus(pattern)).map { statuses =>
      statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toImmutableArraySeq
    }.getOrElse(Seq.empty[Path])
  }

  /**
   * 方法级注释：如果需要则展开通配符路径，否则返回原始路径
   * @param pattern 输入路径
   * @return 展开后的路径列表
   */
  def globPathIfNecessary(pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(pattern) else Seq(pattern)
  }

  /**
   * 方法级注释：使用指定文件系统，需要时展开通配符路径，否则返回原始路径
   * @param fs 文件系统对象
   * @param pattern 输入路径
   * @return 展开后的路径列表
   */
  def globPathIfNecessary(fs: FileSystem, pattern: Path): Seq[Path] = {
    if (isGlobPath(pattern)) globPath(fs, pattern) else Seq(pattern)
  }

  private val HADOOP_CONF_PATTERN = "(\\$\\{hadoopconf-[^}$\\s]+})".r.unanchored

  /**
   * Substitute variables by looking them up in Hadoop configs. Only variables that match the
   * ${hadoopconf- .. } pattern are substituted.
   */
  def substituteHadoopVariables(text: String, hadoopConf: Configuration): String = {
    text match {
      case HADOOP_CONF_PATTERN(matched) =>
        logDebug(text + " matched " + HADOOP_CONF_PATTERN)
        val key = matched.substring(13, matched.length() - 1) // remove ${hadoopconf- .. }
        val eval = Option[String](hadoopConf.get(key))
          .map { value =>
            logDebug("Substituted " + matched + " with " + value)
            text.replace(matched, value)
          }
        if (eval.isEmpty) {
          // The variable was not found in Hadoop configs, so return text as is.
          text
        } else {
          // Continue to substitute more variables.
          substituteHadoopVariables(eval.get, hadoopConf)
        }
      case _ =>
        logDebug(text + " didn't match " + HADOOP_CONF_PATTERN)
        text
    }
  }

  /**
   * Dump the credentials' tokens to string values.
   *
   * @param credentials credentials
   * @return an iterator over the string values. If no credentials are passed in: an empty list
   */
  private[spark] def dumpTokens(credentials: Credentials): Iterable[String] = {
    if (credentials != null) {
      credentials.getAllTokens.asScala.map(tokenToString)
    } else {
      Seq.empty
    }
  }

  /**
   * Convert a token to a string for logging.
   * If its an abstract delegation token, attempt to unmarshall it and then
   * print more details, including timestamps in human-readable form.
   *
   * @param token token to convert to a string
   * @return a printable string value.
   */
  private[spark] def tokenToString(token: Token[_ <: TokenIdentifier]): String = {
    val df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT, Locale.US)
    val buffer = new StringBuilder(128)
    buffer.append(token.toString)
    try {
      val ti = token.decodeIdentifier
      buffer.append("; ").append(ti)
      ti match {
        case dt: AbstractDelegationTokenIdentifier =>
          // include human times and the renewer, which the HDFS tokens toString omits
          buffer.append("; Renewer: ").append(dt.getRenewer)
          buffer.append("; Issued: ").append(df.format(new Date(dt.getIssueDate)))
          buffer.append("; Max Date: ").append(df.format(new Date(dt.getMaxDate)))
        case _ =>
      }
    } catch {
      case e: IOException =>
        logDebug(s"Failed to decode $token: $e", e)
    }
    buffer.toString
  }

  /**
   * 方法级注释：将凭证对象序列化为字节数组
   * @param creds 凭证对象
   * @return 序列化后的字节数组
   */
  def serialize(creds: Credentials): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream
    val dataStream = new DataOutputStream(byteStream)
    creds.writeTokenStorageToStream(dataStream)
    byteStream.toByteArray
  }

  /**
   * 方法级注释：从字节数组反序列化出凭证对象
   * @param tokenBytes 序列化后的字节数组
   * @return 反序列化得到的凭证对象
   */
  def deserialize(tokenBytes: Array[Byte]): Credentials = {
    val tokensBuf = new ByteArrayInputStream(tokenBytes)

    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(tokensBuf))
    creds
  }

  /**
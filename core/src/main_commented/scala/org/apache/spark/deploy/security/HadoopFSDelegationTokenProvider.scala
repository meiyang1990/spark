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

package org.apache.spark.deploy.security

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.util.Utils

/**
 * Hadoop HDFS 委派令牌提供者，负责在Kerberos安全模式下获取和管理HDFS的委派令牌
 * 用于Spark应用在YARN集群运行时，定期更新HDFS访问凭证
 */
private[deploy] class HadoopFSDelegationTokenProvider
    extends HadoopDelegationTokenProvider with Logging {

  // 令牌更新间隔，首次获取令牌后设置，None表示无法获取有效间隔
  private var tokenRenewalInterval: Option[Long] = null

  override val serviceName: String = "hadoopfs"

  /**
   * 获取HDFS委派令牌并计算下一次更新时间
   * @param hadoopConf Hadoop配置
   * @param sparkConf Spark配置
   * @param creds 存储获取到的委派令牌的凭证对象
   * @return 下一次更新时间戳，None表示不需要更新
   */
  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      // 获取需要访问的所有HDFS文件系统
      val fileSystems = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf, hadoopConf)
      // 需要排除令牌更新的文件系统主机列表
      val fsToExclude = sparkConf.get(YARN_KERBEROS_FILESYSTEM_RENEWAL_EXCLUDE)
        .map(new Path(_).getFileSystem(hadoopConf).getUri.getHost)
        .toSet
      // 获取委派令牌并存储到凭证中
      val fetchCreds = fetchDelegationTokens(getTokenRenewer(sparkConf, hadoopConf), fileSystems,
        creds, fsToExclude)

      // 首次调用时计算令牌更新间隔，仅执行一次
      if (tokenRenewalInterval == null) {
        tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, fileSystems)
      }

      // 计算下一次更新时间，取所有令牌中最早的过期时间
      val nextRenewalDate = tokenRenewalInterval.flatMap { interval =>
        val nextRenewalDates = fetchCreds.getAllTokens.asScala
          .filter(_.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier])
          .map { token =>
            val identifier = token
              .decodeIdentifier()
              .asInstanceOf[AbstractDelegationTokenIdentifier]
            val tokenKind = token.getKind.toString
            getIssueDate(tokenKind, identifier) + interval
          }
        if (nextRenewalDates.isEmpty) None else Some(nextRenewalDates.min)
      }

      nextRenewalDate
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to get token from service ${MDC(SERVICE_NAME, serviceName)}", e)
        None
    }
  }

  /**
   * 检查是否需要获取委派令牌
   * @return Hadoop安全模式开启时返回true，否则返回false
   */
  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    UserGroupInformation.isSecurityEnabled
  }

  /**
   * 获取委派令牌的更新者主体名称
   * YARN模式下使用ResourceManager principal作为更新者，独立模式使用当前用户
   */
  private def getTokenRenewer(sparkConf: SparkConf, hadoopConf: Configuration): String = {
    val master = sparkConf.get("spark.master", null)
    val tokenRenewer = if (master != null && master.contains("yarn")) {
      Master.getMasterPrincipal(hadoopConf)
    } else {
      UserGroupInformation.getCurrentUser().getUserName()
    }
    logDebug("Delegation token renewer is: " + tokenRenewer)

    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer."
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    tokenRenewer
  }

  /**
   * 为指定文件系统批量获取委派令牌，添加到凭证对象中
   * @param renewer 令牌更新者主体
   * @param filesystems 需要获取令牌的文件系统集合
   * @param creds 存储令牌的凭证对象
   * @param fsToExclude 需要排除更新的文件系统主机集合
   * @return 填充了令牌的凭证对象
   */
  private def fetchDelegationTokens(
      renewer: String,
      filesystems: Set[FileSystem],
      creds: Credentials,
      fsToExclude: Set[String]): Credentials = {

    filesystems.foreach { fs =>
      if (fsToExclude.contains(fs.getUri.getHost)) {
        // YARN RM会跳过空更新者的令牌更新，因此使用空更新者实现排除
        logInfo(log"getting token for: ${MDC(FILE_SYSTEM, fs)} with empty renewer to skip renewal")
        Utils.tryLogNonFatalError { fs.addDelegationTokens("", creds) }
      } else {
        logInfo(log"getting token for: ${MDC(FILE_SYSTEM, fs)} with" +
          log" renewer ${MDC(TOKEN_RENEWER, renewer)}")
        Utils.tryLogNonFatalError { fs.addDelegationTokens(renewer, creds) }
      }
    }

    creds
  }

  /**
   * 计算委派令牌的更新间隔，通过实际续约获取过期时间差
   * YARN模式下无法使用RM作为更新者的令牌手动续约，因此创建当前用户作为更新者的临时令牌计算间隔
   * @param hadoopConf Hadoop配置
   * @param filesystems 需要计算间隔的文件系统集合
   * @return 最小更新间隔，None表示无法获取有效间隔
   */
  private def getTokenRenewalInterval(
      hadoopConf: Configuration,
      filesystems: Set[FileSystem]): Option[Long] = {
    // 使用当前登录用户作为更新者创建临时令牌
    val renewer = UserGroupInformation.getCurrentUser().getUserName()

    val creds = new Credentials()
    fetchDelegationTokens(renewer, filesystems, creds, Set.empty)

    val renewIntervals = creds.getAllTokens.asScala.filter {
      _.decodeIdentifier().isInstanceOf[AbstractDelegationTokenIdentifier]
    }.flatMap { token =>
      Try {
        // 续约获取新的过期时间，计算间隔
        val newExpiration = token.renew(hadoopConf)
        val identifier = token.decodeIdentifier().asInstanceOf[AbstractDelegationTokenIdentifier]
        val tokenKind = token.getKind.toString
        val interval = newExpiration - getIssueDate(tokenKind, identifier)
        logInfo(log"Renewal interval is ${MDC(TOTAL_TIME, interval)} for" +
          log" token ${MDC(TOKEN_KIND, tokenKind)}")
        // 临时令牌使用后取消，减轻服务端压力
        token.cancel(hadoopConf)
        interval
      }.toOption
    }
    if (renewIntervals.isEmpty) None else Some(renewIntervals.min)
  }

  /**
   * 获取令牌签发日期，处理异常时间情况
   * @param kind 令牌类型
   * @param identifier 令牌标识符
   * @return 有效的签发日期，异常时使用当前时间替代
   */
  private def getIssueDate(kind: String, identifier: AbstractDelegationTokenIdentifier): Long = {
    val now = System.currentTimeMillis()
    val issueDate = identifier.getIssueDate
    if (issueDate > now) {
      logWarning(log"Token ${MDC(TOKEN_KIND, kind)} has set up issue date later than " +
        log"current time (provided: " +
        log"${MDC(ISSUE_DATE, issueDate)} / current timestamp: ${MDC(CURRENT_TIME, now)}). " +
        log"Please make sure clocks are in sync between " +
        log"machines. If the issue is not a clock mismatch, consult token implementor to check " +
        log"whether issue date is valid.")
      issueDate
    } else if (issueDate > 0L) {
      issueDate
    } else {
      logWarning(log"Token ${MDC(TOKEN_KIND, kind)} has not set up issue date properly " +
        log"(provided: ${MDC(ISSUE_DATE, issueDate)}). " +
        log"Using current timestamp (${MDC(CURRENT_TIME, now)} as issue date instead. " +
        log"Consult token implementor to fix the behavior.")
      now
    }
  }
}

/**
 * HadoopFSDelegationTokenProvider 的伴生对象，提供工具方法获取需要访问的HDFS文件系统列表
 */
private[deploy] object HadoopFSDelegationTokenProvider {
  /**
   * 获取Spark应用需要访问的所有HDFS文件系统
   * 包含默认文件系统、用户配置的额外文件系统、YARN模式下的staging目录文件系统
   * @param sparkConf Spark配置
   * @param hadoopConf Hadoop配置
   * @return 需要访问的文件系统集合
   */
  def hadoopFSsToAccess(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Set[FileSystem] = {
    // scalastyle:off FileSystemGet
    val defaultFS = FileSystem.get(hadoopConf)
    // scalastyle:on FileSystemGet

    val filesystemsToAccess = sparkConf.get(KERBEROS_FILESYSTEMS_TO_ACCESS)
      .map(new Path(_).getFileSystem(hadoopConf))
      .toSet

    val master = sparkConf.get("spark.master", null)
    val stagingFS = if (master != null && master.contains("yarn")) {
      sparkConf.get(STAGING_DIR).map(new Path(_).getFileSystem(hadoopConf))
    } else {
      None
    }

    filesystemsToAccess ++ stagingFS + defaultFS
  }
}